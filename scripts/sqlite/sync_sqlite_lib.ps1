param(
    # Release tag to sync from, e.g. "v4.7.0" or "v4.7.0-beta".
    # Omitted = latest stable (non-draft, non-prerelease) release.
    [string]$Tag
)

$ErrorActionPreference = "Stop"

# Verify GitHub CLI is installed
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Error "❌ GitHub CLI (gh) is not installed. Install from https://cli.github.com/"
    exit 1
}

# Verify the user is authenticated
gh auth status *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Error "❌ Not authenticated with GitHub. Run 'gh auth login' first."
    exit 1
}

$GitHubToken = (gh auth token).Trim()
if ([string]::IsNullOrWhiteSpace($GitHubToken)) {
    Write-Error "❌ Failed to retrieve GitHub token from gh CLI."
    exit 1
}
$REPO = "dailysoftwaresystems/DBAS.SQLite"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$NativeLibsDir = "$SCRIPT_DIR/../../native_libs"
$OUT_DIR = "$NativeLibsDir/sqlite"

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
$CHECKSUMS = "SHA256SUMS"
$ASSETS = @(
    "dbas-dist-linux.tar.gz",
    "dbas-dist-windows.tar.gz",
    "dbas-dist-android.tar.gz",
    "dbas-dist-web.tar.gz",
    "dbas-dist-apple.tar.gz"
)
# Paths that must exist after extraction, otherwise the assets are not the
# tree the copy steps below assume.
$EXPECTED = @(
    "android/a64", "android/armeabi", "android/x86_64",
    "macos/a64", "macos/x86", "macos/dbas_sqlite.xcframework",
    "ios/dbas_sqlite.xcframework",
    "windows", "linux", "web"
)

# Reuse the gh login for the asset downloads too.
$env:GH_TOKEN = $GitHubToken

if ([string]::IsNullOrWhiteSpace($Tag)) {
    Write-Host "Resolving latest stable release of $REPO..."
    $Tag = (gh release list --repo $REPO --exclude-drafts --exclude-pre-releases --limit 1 --json tagName --jq '.[0].tagName' | Select-Object -First 1)
    if ($LASTEXITCODE -ne 0) {
        Write-Error "❌ Failed to list releases of $REPO."
        exit 1
    }
    $Tag = "$Tag".Trim()
    if ([string]::IsNullOrWhiteSpace($Tag)) {
        Write-Error "❌ $REPO has no stable release to sync from. Pass -Tag to pick one explicitly."
        exit 1
    }
}
Write-Host "Syncing from release: $Tag"

# Everything lands in a staging tree first. The existing $OUT_DIR is only
# replaced once the download, checksum check and extraction have all passed,
# so a failed sync leaves the working copy exactly as it was. The staging
# tree sits beside $OUT_DIR so the final swap is a same-volume rename.
New-Item -ItemType Directory -Force -Path $NativeLibsDir | Out-Null
$StagingRoot = Join-Path $NativeLibsDir ".sqlite-sync-$PID"
# Kept as bare leaf names too: the extraction step below runs tar from inside
# $StagingRoot and must address these relatively. See the comment there.
$DownloadDirName = "assets"
$ExtractDirName = "dist"
$LegsDirName = "legs"
$DownloadDir = Join-Path $StagingRoot $DownloadDirName
$ExtractDir = Join-Path $StagingRoot $ExtractDirName
$LegsDir = Join-Path $StagingRoot $LegsDirName
$BackupDir = Join-Path $StagingRoot "backup"

# Set when a failed swap has already put the previous tree back, so the
# handler does not claim nothing was touched when something was.
$FailureNote = ""

function Get-Sha256($path) {
    return (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Test-Checksums($sumsPath, $dir, $names) {
    $expected = @{}
    foreach ($line in (Get-Content -LiteralPath $sumsPath)) {
        $trimmed = $line.Trim()
        if ($trimmed -eq "") { continue }
        $parts = $trimmed -split '\s+', 2
        if ($parts.Count -lt 2) { continue }
        $name = $parts[1].Trim().TrimStart('*')
        $expected[(Split-Path -Leaf $name)] = $parts[0].ToLowerInvariant()
    }

    foreach ($name in $names) {
        if (-not $expected.ContainsKey($name)) {
            throw "$CHECKSUMS has no entry for $name."
        }
        $actual = Get-Sha256 (Join-Path $dir $name)
        if ($actual -ne $expected[$name]) {
            throw "Checksum mismatch for ${name}: expected $($expected[$name]), got $actual."
        }
        Write-Host "  verified $name"
    }
}

function Read-BuildInfo($path) {
    # Parses the "key=value" provenance stamp into a hashtable.
    $map = @{}
    foreach ($line in (Get-Content -LiteralPath $path)) {
        $trimmed = $line.Trim()
        if ($trimmed -eq "" -or $trimmed.StartsWith("#")) { continue }
        $i = $trimmed.IndexOf('=')
        if ($i -lt 1) { continue }
        $map[$trimmed.Substring(0, $i)] = $trimmed.Substring($i + 1)
    }
    return $map
}

try {
    New-Item -ItemType Directory -Force -Path $DownloadDir | Out-Null
    New-Item -ItemType Directory -Force -Path $ExtractDir | Out-Null
    New-Item -ItemType Directory -Force -Path $LegsDir | Out-Null
    New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null

    Write-Host "Downloading release assets into $DownloadDir"
    $downloadArgs = @("release", "download", $Tag, "--repo", $REPO, "--dir", $DownloadDir, "--clobber")
    foreach ($name in ($ASSETS + $CHECKSUMS)) { $downloadArgs += @("--pattern", $name) }
    & gh @downloadArgs
    if ($LASTEXITCODE -ne 0) {
        throw "gh release download failed for tag $Tag."
    }

    foreach ($name in ($ASSETS + $CHECKSUMS)) {
        if (-not (Test-Path -LiteralPath (Join-Path $DownloadDir $name))) {
            throw "Release $Tag is missing the asset $name."
        }
    }

    Write-Host "Verifying $CHECKSUMS..."
    Test-Checksums (Join-Path $DownloadDir $CHECKSUMS) $DownloadDir $ASSETS

    # tar runs from inside $StagingRoot and is given RELATIVE paths on purpose.
    # Whichever tar PATH resolves varies per machine, and the two behave
    # differently on Windows absolute paths:
    #   * GNU tar (C:\Program Files\Git\usr\bin\tar.exe, ahead of system32 on
    #     many dev boxes) reads "C:\..." as a remote HOST spec - everything
    #     before the first ":" is a hostname - and dies with
    #     "tar (child): Cannot connect to C: resolve failed". It also cannot
    #     chdir to a Windows absolute -C target ("Cannot open"). Forward
    #     slashes do not help: "C:/..." still has the colon first.
    #   * bsdtar (C:\WINDOWS\system32\tar.exe) handles them fine, but rejects
    #     --force-local outright ("Option --force-local is not supported"), so
    #     that GNU-only flag would just move the breakage to the other tar.
    # Relative paths contain no colon, so both implementations agree on them.
    # Extraction stays -C-relative, so the resulting tree is byte-identical to
    # what the .sh twin produces.
    $legNames = @()
    foreach ($name in $ASSETS) {
        $leg = $name -replace '^dbas-dist-', '' -replace '\.tar\.gz$', ''
        $legNames += $leg
        New-Item -ItemType Directory -Force -Path (Join-Path $LegsDir $leg) | Out-Null
    }

    Push-Location -LiteralPath $StagingRoot
    try {
        for ($i = 0; $i -lt $ASSETS.Count; $i++) {
            Write-Host "Extracting $($ASSETS[$i])"
            & tar -xzf "$DownloadDirName/$($ASSETS[$i])" -C "$LegsDirName/$($legNames[$i])"
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to extract $($ASSETS[$i])."
            }
        }
    } finally {
        Pop-Location
    }

    # Check each leg's provenance stamp, then merge the payloads.
    #
    # The legs are built by separate deploy jobs, so a partial re-run can
    # publish an asset built from a different commit than its siblings.
    # SHA256SUMS cannot catch that - it is generated alongside whatever assets
    # exist - but the BUILDINFO stamps can: a consistent release has one
    # sha/ref/run/attempt across all five.
    $prov = $null
    $provSource = ""
    for ($i = 0; $i -lt $ASSETS.Count; $i++) {
        $name = $ASSETS[$i]
        $leg = $legNames[$i]
        $legRoot = Join-Path $LegsDir $leg
        $biPath = Join-Path $legRoot "BUILDINFO"
        if (-not (Test-Path -LiteralPath $biPath)) {
            throw "$name has no BUILDINFO provenance stamp - it predates the artifact release format."
        }
        $bi = Read-BuildInfo $biPath

        if ([string]::IsNullOrWhiteSpace($bi["sha"])) {
            throw "$name's BUILDINFO has no sha - the stamp is malformed."
        }
        if ($bi["leg"] -ne $leg) {
            throw "$name carries a BUILDINFO for leg '$($bi["leg"])' - the asset and its stamp disagree."
        }

        if ($null -eq $prov) {
            $prov = $bi
            $provSource = $name
        } elseif ($bi["sha"] -ne $prov["sha"] -or $bi["run"] -ne $prov["run"] -or
                  $bi["ref"] -ne $prov["ref"] -or $bi["attempt"] -ne $prov["attempt"]) {
            throw ("Release $Tag mixes builds: $provSource is sha=$($prov["sha"]) run=$($prov["run"]) " +
                   "attempt=$($prov["attempt"]), but $name is sha=$($bi["sha"]) run=$($bi["run"]) attempt=$($bi["attempt"]).")
        }

        foreach ($entry in (Get-ChildItem -LiteralPath $legRoot -Force)) {
            if ($entry.Name -eq "BUILDINFO") { continue }
            $dest = Join-Path $ExtractDir $entry.Name
            if (Test-Path -LiteralPath $dest) {
                throw "Two release assets both ship $($entry.Name) - they cannot be merged into one tree."
            }
            Move-Item -LiteralPath $entry.FullName -Destination $dest
        }
    }

    foreach ($path in $EXPECTED) {
        if (-not (Test-Path -LiteralPath (Join-Path $ExtractDir $path))) {
            throw "Extracted assets are missing $path - release $Tag does not look like a complete dist."
        }
    }

    # One stamp for the merged tree, replacing the five that would have
    # collided. Written through .NET with explicit UTF-8-no-BOM and LF
    # endings so the file is byte-identical to what the .sh twin produces -
    # Set-Content's defaults differ between Windows PowerShell and pwsh.
    $legList = (($legNames | Sort-Object) -join ',')
    $biLines = @(
        "# Written by scripts/sqlite/sync_sqlite_lib.{sh,ps1} - do not edit.",
        "# Describes ONLY the directories named in legs=; anything else under",
        "# this tree predates the sync and is not refreshed by it.",
        "tag=$Tag",
        "sha=$($prov["sha"])",
        "ref=$($prov["ref"])",
        "run=$($prov["run"])",
        "attempt=$($prov["attempt"])",
        "legs=$legList"
    )
    [System.IO.File]::WriteAllText(
        (Join-Path $ExtractDir "BUILDINFO"),
        (($biLines -join "`n") + "`n"),
        (New-Object System.Text.UTF8Encoding($false)))

    # Known-good: swap it in, one top-level entry at a time.
    #
    # Deliberately NOT a wholesale replace of $OUT_DIR: this tree is committed
    # in the repo and holds directories no release asset ships (tests/), which
    # a whole-directory swap would silently delete. Only the entries the
    # release actually carries are replaced; everything else is left untouched.
    # Each replaced entry still swaps atomically, and the previous copies are
    # held aside so a failure part-way through can put the tree back.
    Write-Host "Updating $OUT_DIR"
    New-Item -ItemType Directory -Force -Path $OUT_DIR | Out-Null

    $swapEntries = @(Get-ChildItem -LiteralPath $ExtractDir -Force | ForEach-Object { $_.Name })
    if ($swapEntries.Count -eq 0) {
        throw "Nothing was staged for $OUT_DIR."
    }

    foreach ($base in $swapEntries) {
        $current = Join-Path $OUT_DIR $base
        if (Test-Path -LiteralPath $current) {
            Move-Item -LiteralPath $current -Destination (Join-Path $BackupDir $base) -Force
        }
    }
    try {
        foreach ($base in $swapEntries) {
            Move-Item -LiteralPath (Join-Path $ExtractDir $base) -Destination (Join-Path $OUT_DIR $base) -Force
        }
    } catch {
        foreach ($base in $swapEntries) {
            $current = Join-Path $OUT_DIR $base
            $backup = Join-Path $BackupDir $base
            if (Test-Path -LiteralPath $current) {
                Remove-Item -LiteralPath $current -Recurse -Force -ErrorAction SilentlyContinue
            }
            if (Test-Path -LiteralPath $backup) {
                Move-Item -LiteralPath $backup -Destination $current -Force
            }
        }
        $FailureNote = "   The previous tree was restored."
        throw
    }
} catch {
    Write-Host "❌ Sync failed: $($_.Exception.Message)"
    if ($FailureNote) {
        Write-Host $FailureNote
    } else {
        Write-Host "   Nothing was replaced; $OUT_DIR is unchanged."
    }
    exit 1
} finally {
    if (Test-Path -LiteralPath $StagingRoot) {
        Remove-Item -LiteralPath $StagingRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "All binaries downloaded in: $OUT_DIR, copying binaries to respective platform directories..."

Write-Host "Copying android binaries..."
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../android/src/main/jniLibs/arm64-v8a" | Out-Null
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../android/src/main/jniLibs/armeabi-v7a" | Out-Null
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../android/src/main/jniLibs/x86_64" | Out-Null
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../macos/libs/a64" | Out-Null
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../macos/libs/x86" | Out-Null
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../windows/libs" | Out-Null
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../linux/libs" | Out-Null
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../web/libs" | Out-Null

Copy-Item "$OUT_DIR/android/a64/*" -Destination "$SCRIPT_DIR/../../android/src/main/jniLibs/arm64-v8a" -Recurse -Force
Copy-Item "$OUT_DIR/android/armeabi/*" -Destination "$SCRIPT_DIR/../../android/src/main/jniLibs/armeabi-v7a" -Recurse -Force
Copy-Item "$OUT_DIR/android/x86_64/*" -Destination "$SCRIPT_DIR/../../android/src/main/jniLibs/x86_64" -Recurse -Force
Copy-Item "$OUT_DIR/macos/a64/*" -Destination "$SCRIPT_DIR/../../macos/libs/a64" -Recurse -Force
Copy-Item "$OUT_DIR/macos/x86/*" -Destination "$SCRIPT_DIR/../../macos/libs/x86" -Recurse -Force
Copy-Item "$OUT_DIR/windows/*" -Destination "$SCRIPT_DIR/../../windows/libs" -Recurse -Force
Copy-Item "$OUT_DIR/linux/*" -Destination "$SCRIPT_DIR/../../linux/libs" -Recurse -Force
Copy-Item "$OUT_DIR/web/*" -Destination "$SCRIPT_DIR/../../web/libs" -Recurse -Force

# The cross-origin-isolation service worker must also sit at the example's
# web ROOT, not just in web/libs: a service worker only controls its own URL
# path, so to isolate the document (required for SharedArrayBuffer / the
# multi-worker DB pool) it has to be served from "/". Guarded so the sync
# still works against an older dist that predates coi-serviceworker.js.
if (Test-Path "$OUT_DIR/web/coi-serviceworker.js") {
    New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../example/web" | Out-Null
    Copy-Item "$OUT_DIR/web/coi-serviceworker.js" -Destination "$SCRIPT_DIR/../../example/web/" -Force
}

Write-Host "Copying ios binaries..."
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../ios/dbas_sqlite" | Out-Null
Copy-Item "$OUT_DIR/ios/dbas_sqlite.xcframework" -Destination "$SCRIPT_DIR/../../ios/dbas_sqlite" -Recurse -Force

Write-Host "Copying macos binaries..."
New-Item -ItemType Directory -Force -Path "$SCRIPT_DIR/../../macos/dbas_sqlite" | Out-Null
Copy-Item "$OUT_DIR/macos/dbas_sqlite.xcframework" -Destination "$SCRIPT_DIR/../../macos/dbas_sqlite" -Recurse -Force

# Defensive: fix the upstream `_x86_x64` typo (extra `x`) in xcframework slice names.
$Xcframeworks = @(
    "$SCRIPT_DIR/../../ios/dbas_sqlite/dbas_sqlite.xcframework",
    "$SCRIPT_DIR/../../macos/dbas_sqlite/dbas_sqlite.xcframework"
)
foreach ($fw in $Xcframeworks) {
    Get-ChildItem -Path $fw -Directory -Filter "*_x86_x64*" -ErrorAction SilentlyContinue | ForEach-Object {
        $newName = $_.Name -replace '_x86_x64', '_x86_64'
        Write-Host "Fixing slice name typo: $($_.Name) -> $newName"
        Rename-Item -Path $_.FullName -NewName $newName
    }
}

Write-Host "All platform binaries copied successfully."
