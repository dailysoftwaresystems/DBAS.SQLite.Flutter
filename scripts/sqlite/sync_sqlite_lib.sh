#!/bin/bash
set -e

# Usage: sync_sqlite_lib.sh [TAG]
#   TAG - release tag to sync from, e.g. "v4.7.0" or "v4.7.0-beta".
#         Omitted = latest stable (non-draft, non-prerelease) release.
TAG="${1:-}"

# Verify GitHub CLI is installed
if ! command -v gh &> /dev/null; then
  echo "❌ GitHub CLI (gh) is not installed. Install from https://cli.github.com/"
  exit 1
fi

# Verify the user is authenticated
if ! gh auth status &> /dev/null; then
  echo "❌ Not authenticated with GitHub. Run 'gh auth login' first."
  exit 1
fi

GITHUB_TOKEN="$(gh auth token)"
if [[ -z "$GITHUB_TOKEN" ]]; then
  echo "❌ Failed to retrieve GitHub token from gh CLI."
  exit 1
fi

REPO="dailysoftwaresystems/DBAS.SQLite"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NATIVE_LIBS_DIR="$SCRIPT_DIR/../../native_libs"
OUT_DIR="$NATIVE_LIBS_DIR/sqlite"

# DBAS.SQLite no longer commits dbas/dist; the built binaries ship as release
# assets. Each tarball is packed with `tar -C dbas/dist -czf <name> <subdir>`,
# so extracting them all into one directory reproduces the dbas/dist tree.
# This package links against every platform, so it wants all five.
#
# Every tarball also carries a root-level BUILDINFO provenance stamp, so they
# cannot simply be extracted on top of each other - the five would collide on
# that one path and the survivor would describe a single leg while appearing
# to describe the whole tree. Each is unpacked into its own leg directory so
# the stamps can be compared, then the payloads are merged.
CHECKSUMS="SHA256SUMS"
ASSETS=(
  "dbas-dist-linux.tar.gz"
  "dbas-dist-windows.tar.gz"
  "dbas-dist-android.tar.gz"
  "dbas-dist-web.tar.gz"
  "dbas-dist-apple.tar.gz"
)
# Paths that must exist after extraction, otherwise the assets are not the
# tree the copy steps below assume.
EXPECTED=(
  "android/a64" "android/armeabi" "android/x86_64"
  "macos/a64" "macos/x86" "macos/dbas_sqlite.xcframework"
  "ios/dbas_sqlite.xcframework"
  "windows" "linux" "web"
)

# Reuse the gh login for the asset downloads too.
export GH_TOKEN="$GITHUB_TOKEN"

if [ -z "$TAG" ]; then
  echo "Resolving latest stable release of $REPO..."
  TAG="$(gh release list --repo "$REPO" --exclude-drafts --exclude-pre-releases --limit 1 --json tagName --jq '.[0].tagName')" || {
    echo "❌ Failed to list releases of $REPO."
    exit 1
  }
  if [ -z "$TAG" ]; then
    echo "❌ $REPO has no stable release to sync from. Pass a tag argument to pick one explicitly."
    exit 1
  fi
fi
echo "Syncing from release: $TAG"

# Everything lands in a staging tree first. The existing $OUT_DIR is only
# replaced once the download, checksum check and extraction have all passed,
# so a failed sync leaves the working copy exactly as it was. The staging
# tree sits beside $OUT_DIR so the final swap is a same-volume rename.
mkdir -p "$NATIVE_LIBS_DIR"
STAGING_ROOT="$NATIVE_LIBS_DIR/.sqlite-sync-$$"
DOWNLOAD_DIR="$STAGING_ROOT/assets"
LEGS_DIR="$STAGING_ROOT/legs"
EXTRACT_DIR="$STAGING_ROOT/dist"
BACKUP_DIR="$STAGING_ROOT/backup"

cleanup() {
  rm -rf "$STAGING_ROOT"
}
trap cleanup EXIT

fail() {
  echo "❌ Sync failed: $1"
  echo "   Nothing was replaced; $OUT_DIR is unchanged."
  exit 1
}

sha256_of() {
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1 | tr '[:upper:]' '[:lower:]'
  else
    shasum -a 256 "$1" | cut -d' ' -f1 | tr '[:upper:]' '[:lower:]'
  fi
}

expected_sum_for() {
  # Reads "<hash>  <name>" lines and echoes the hash whose basename matches $1.
  local wanted=$1 hash name
  while read -r hash name; do
    name="${name#\*}"
    name="${name##*/}"
    if [ "$name" = "$wanted" ]; then
      printf '%s' "$hash" | tr '[:upper:]' '[:lower:]'
      return 0
    fi
  done < "$DOWNLOAD_DIR/$CHECKSUMS"
}

buildinfo_field() {
  # Echoes the value of key $2 in the "key=value" BUILDINFO file $1, or
  # nothing when the key is absent. grep exiting 1 on no-match must not trip
  # `set -e`, hence the `|| true`.
  local line=""
  line="$(grep -m1 "^$2=" "$1" 2>/dev/null || true)"
  printf '%s' "${line#*=}"
}

mkdir -p "$DOWNLOAD_DIR" "$LEGS_DIR" "$EXTRACT_DIR" "$BACKUP_DIR"

echo "Downloading release assets into $DOWNLOAD_DIR"
DOWNLOAD_ARGS=()
for name in "${ASSETS[@]}" "$CHECKSUMS"; do
  DOWNLOAD_ARGS+=(--pattern "$name")
done
gh release download "$TAG" --repo "$REPO" --dir "$DOWNLOAD_DIR" --clobber "${DOWNLOAD_ARGS[@]}" \
  || fail "gh release download failed for tag $TAG."

for name in "${ASSETS[@]}" "$CHECKSUMS"; do
  [ -f "$DOWNLOAD_DIR/$name" ] || fail "Release $TAG is missing the asset $name."
done

echo "Verifying $CHECKSUMS..."
for name in "${ASSETS[@]}"; do
  expected="$(expected_sum_for "$name")"
  [ -n "$expected" ] || fail "$CHECKSUMS has no entry for $name."
  actual="$(sha256_of "$DOWNLOAD_DIR/$name")"
  [ "$expected" = "$actual" ] || fail "Checksum mismatch for $name: expected $expected, got $actual."
  echo "  verified $name"
done

# Unpack each asset into its own leg directory, check its provenance stamp,
# then merge the payload into the shared tree.
#
# The legs are built by separate deploy jobs, so a partial re-run can publish
# an asset built from a different commit than its siblings. SHA256SUMS cannot
# catch that - it is generated alongside whatever assets exist - but the
# BUILDINFO stamps can: a consistent release has one sha/ref/run/attempt
# across all five.
PROV_SHA=""
PROV_REF=""
PROV_RUN=""
PROV_ATTEMPT=""
PROV_SOURCE=""
SYNCED_LEGS=()

for name in "${ASSETS[@]}"; do
  echo "Extracting $name"
  leg="${name#dbas-dist-}"
  leg="${leg%.tar.gz}"
  mkdir -p "$LEGS_DIR/$leg"
  tar -xzf "$DOWNLOAD_DIR/$name" -C "$LEGS_DIR/$leg" || fail "Failed to extract $name."

  bi="$LEGS_DIR/$leg/BUILDINFO"
  [ -f "$bi" ] || fail "$name has no BUILDINFO provenance stamp - it predates the artifact release format."

  leg_sha="$(buildinfo_field "$bi" sha)"
  leg_ref="$(buildinfo_field "$bi" ref)"
  leg_run="$(buildinfo_field "$bi" run)"
  leg_attempt="$(buildinfo_field "$bi" attempt)"
  leg_name="$(buildinfo_field "$bi" leg)"

  [ -n "$leg_sha" ] || fail "$name's BUILDINFO has no sha - the stamp is malformed."
  [ "$leg_name" = "$leg" ] \
    || fail "$name carries a BUILDINFO for leg '$leg_name' - the asset and its stamp disagree."

  if [ -z "$PROV_SOURCE" ]; then
    PROV_SHA="$leg_sha"
    PROV_REF="$leg_ref"
    PROV_RUN="$leg_run"
    PROV_ATTEMPT="$leg_attempt"
    PROV_SOURCE="$name"
  elif [ "$leg_sha" != "$PROV_SHA" ] || [ "$leg_run" != "$PROV_RUN" ] \
       || [ "$leg_ref" != "$PROV_REF" ] || [ "$leg_attempt" != "$PROV_ATTEMPT" ]; then
    fail "Release $TAG mixes builds: $PROV_SOURCE is sha=$PROV_SHA run=$PROV_RUN attempt=$PROV_ATTEMPT, but $name is sha=$leg_sha run=$leg_run attempt=$leg_attempt."
  fi
  SYNCED_LEGS+=("$leg")

  for entry in "$LEGS_DIR/$leg"/*; do
    [ -e "$entry" ] || continue
    base="$(basename "$entry")"
    if [ "$base" = "BUILDINFO" ]; then
      continue
    fi
    if [ -e "$EXTRACT_DIR/$base" ]; then
      fail "Two release assets both ship $base - they cannot be merged into one tree."
    fi
    mv "$entry" "$EXTRACT_DIR/$base" || fail "Could not stage $base out of $name."
  done
done

for path in "${EXPECTED[@]}"; do
  [ -e "$EXTRACT_DIR/$path" ] \
    || fail "Extracted assets are missing $path - release $TAG does not look like a complete dist."
done

# One stamp for the merged tree, replacing the five that would have collided.
{
  echo "# Written by scripts/sqlite/sync_sqlite_lib.{sh,ps1} - do not edit."
  echo "# Describes ONLY the directories named in legs=; anything else under"
  echo "# this tree predates the sync and is not refreshed by it."
  echo "tag=$TAG"
  echo "sha=$PROV_SHA"
  echo "ref=$PROV_REF"
  echo "run=$PROV_RUN"
  echo "attempt=$PROV_ATTEMPT"
  printf 'legs=%s\n' "$(printf '%s\n' "${SYNCED_LEGS[@]}" | sort | paste -sd, -)"
} > "$EXTRACT_DIR/BUILDINFO" || fail "Could not write the merged BUILDINFO."

# Known-good: swap it in, one top-level entry at a time.
#
# Deliberately NOT a wholesale replace of $OUT_DIR: this tree is committed in
# the repo and holds directories no release asset ships (tests/), which a
# whole-directory swap would silently delete. Only the entries the release
# actually carries are replaced; everything else is left untouched. Each
# replaced entry still swaps atomically, and the previous copies are held
# aside so a failure part-way through can put the tree back.
echo "Updating $OUT_DIR"
mkdir -p "$OUT_DIR" || fail "Could not create $OUT_DIR."

SWAP_ENTRIES=()
for entry in "$EXTRACT_DIR"/*; do
  [ -e "$entry" ] || continue
  SWAP_ENTRIES+=("$(basename "$entry")")
done
[ "${#SWAP_ENTRIES[@]}" -gt 0 ] || fail "Nothing was staged for $OUT_DIR."

for base in "${SWAP_ENTRIES[@]}"; do
  if [ -e "$OUT_DIR/$base" ]; then
    mv "$OUT_DIR/$base" "$BACKUP_DIR/$base" || fail "Could not move the existing $OUT_DIR/$base aside."
  fi
done

restore_backup() {
  local b
  for b in "${SWAP_ENTRIES[@]}"; do
    rm -rf "$OUT_DIR/$b"
    if [ -e "$BACKUP_DIR/$b" ]; then
      mv "$BACKUP_DIR/$b" "$OUT_DIR/$b" || echo "⚠️  Could not restore $OUT_DIR/$b from $BACKUP_DIR/$b."
    fi
  done
}

for base in "${SWAP_ENTRIES[@]}"; do
  if ! mv "$EXTRACT_DIR/$base" "$OUT_DIR/$base"; then
    restore_backup
    echo "❌ Sync failed: could not move $base into $OUT_DIR."
    echo "   The previous tree was restored."
    exit 1
  fi
done

echo "All binaries downloaded in: $OUT_DIR, copying binaries to respective platform directories..."

echo "Copying android binaries..."
mkdir -p "$SCRIPT_DIR/../../android/src/main/jniLibs/arm64-v8a"
mkdir -p "$SCRIPT_DIR/../../android/src/main/jniLibs/armeabi-v7a"
mkdir -p "$SCRIPT_DIR/../../android/src/main/jniLibs/x86_64"
mkdir -p "$SCRIPT_DIR/../../macos/libs/a64"
mkdir -p "$SCRIPT_DIR/../../macos/libs/x86"
mkdir -p "$SCRIPT_DIR/../../windows/libs"
mkdir -p "$SCRIPT_DIR/../../linux/libs"
mkdir -p "$SCRIPT_DIR/../../web/libs"

cp -r "$OUT_DIR/android/a64/"* "$SCRIPT_DIR/../../android/src/main/jniLibs/arm64-v8a"
cp -r "$OUT_DIR/android/armeabi/"* "$SCRIPT_DIR/../../android/src/main/jniLibs/armeabi-v7a"
cp -r "$OUT_DIR/android/x86_64/"* "$SCRIPT_DIR/../../android/src/main/jniLibs/x86_64"
cp -r "$OUT_DIR/macos/a64/"* "$SCRIPT_DIR/../../macos/libs/a64"
cp -r "$OUT_DIR/macos/x86/"* "$SCRIPT_DIR/../../macos/libs/x86"
cp -r "$OUT_DIR/windows/"* "$SCRIPT_DIR/../../windows/libs"
cp -r "$OUT_DIR/linux/"* "$SCRIPT_DIR/../../linux/libs"
cp -r "$OUT_DIR/web/"* "$SCRIPT_DIR/../../web/libs"

# The cross-origin-isolation service worker must also sit at the example's
# web ROOT (a service worker only controls its own URL path; isolation must
# apply to the document at "/"). Guarded for older dists without it.
if [ -f "$OUT_DIR/web/coi-serviceworker.js" ]; then
  mkdir -p "$SCRIPT_DIR/../../example/web"
  cp "$OUT_DIR/web/coi-serviceworker.js" "$SCRIPT_DIR/../../example/web/"
fi

echo "Copying ios binaries..."
mkdir -p "$SCRIPT_DIR/../../ios/dbas_sqlite/dbas_sqlite.xcframework"
cp -r "$OUT_DIR/ios/dbas_sqlite.xcframework/"* "$SCRIPT_DIR/../../ios/dbas_sqlite/dbas_sqlite.xcframework"

echo "Copying macos binaries..."
mkdir -p "$SCRIPT_DIR/../../macos/dbas_sqlite/dbas_sqlite.xcframework"
cp -r "$OUT_DIR/macos/dbas_sqlite.xcframework/"* "$SCRIPT_DIR/../../macos/dbas_sqlite/dbas_sqlite.xcframework"

# Defensive: fix the upstream `_x86_x64` typo (extra `x`) in xcframework slice names.
for fw in \
    "$SCRIPT_DIR/../../ios/dbas_sqlite/dbas_sqlite.xcframework" \
    "$SCRIPT_DIR/../../macos/dbas_sqlite/dbas_sqlite.xcframework"; do
    for dir in "$fw"/*_x86_x64*; do
        [ -d "$dir" ] || continue
        fixed="${dir//_x86_x64/_x86_64}"
        echo "Fixing slice name typo: $(basename "$dir") -> $(basename "$fixed")"
        mv "$dir" "$fixed"
    done
done

echo "All platform binaries copied successfully."
