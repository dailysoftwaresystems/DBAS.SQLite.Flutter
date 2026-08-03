import 'dart:developer' as developer;
import 'dart:ffi';
import 'dart:io';
import 'package:ffi/ffi.dart';
import 'package:flutter/foundation.dart' show protected;
import 'package:path/path.dart' as path;
import 'dbas_sqlite_native_interface.dart';
import 'package:dbas_sqlite/src/dbas_sqlite.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_db.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_row_cache.dart';
import 'package:dbas_sqlite/src/helpers/dbas_sqlite_platform_util.dart';

/// Base class for the native FFI app implementation.
///
/// Owns shared FFI marshalling — pointer alloc/free, UTF-8 conversion,
/// blob copying. The single concrete subclass [DbasSqliteNativeApp]
/// (in `dbas_sqlite_native_app_ffi.dart`) provides the raw `nativeXxx`
/// calls via the protected abstract methods below.
///
/// The v2.4.0 ABI threads a [SQLiteStmtHandle] (uint64 → Dart `int`)
/// through every per-stmt call. There is no shared row cache here in
/// v2.4.0 — each `DbasSqliteReader` owns its own [RowData] cache,
/// populated by [readRowAndCache].
abstract class DbasSqliteNativeAppBase extends DbasSqliteNativeInterface {
  DbasSqliteNativeAppBase(super.dbName);

  /// SQLite version cached at [initialize] time. Sync return.
  String _sqliteVersion = '';

  /// `true` when the current process is the Flutter test runner.
  /// Read via the conditional `dart:io` helper so the abstract
  /// interface does not need to import it.
  @protected
  bool get isTest => DbasSqlitePlatformUtil.isTest();

  /// Resolves the on-disk path to the platform's native dynamic
  /// library. Only the FFI subclass calls this; web never loads
  /// this base class. Throws [UnsupportedError] on unrecognised
  /// platforms (matches the `UnsupportedError` previously thrown by
  /// `DbasSqliteNativeInterface._getPlatform` for unsupported
  /// non-web targets).
  Future<String> getLibraryPath() async {
    if (Platform.isIOS || (Platform.isMacOS && !isTest)) {
      return '';
    } else if (Platform.isMacOS) {
      String arch = Platform.version.contains('arm64') ||
              Platform.version.toLowerCase().contains('arm64')
          ? 'a64'
          : 'x86';
      final relativePath = path.join('macos', 'libs', arch, 'dbas_sqlite.dylib');
      final baseDir =
          isTest ? _resolveTestBaseDir(relativePath) : Directory.current.path;
      return path.join(baseDir, relativePath);
    }

    late String libPath;
    if (Platform.isAndroid) {
      return 'dbas_sqlite.so';
    } else if (Platform.isWindows) {
      if (isTest) {
        String arch = Platform.version.contains('_x64') ? 'x64' : 'x86';
        final relativePath = path.join('windows', 'libs', arch, 'dbas_sqlite.dll');
        final baseDir = _resolveTestBaseDir(relativePath);
        libPath = path.join(baseDir, relativePath);
      } else {
        libPath = 'dbas_sqlite.dll';
      }
    } else if (Platform.isLinux) {
      if (isTest) {
        final relativePath = path.join('linux', 'libs', 'dbas_sqlite.so');
        final baseDir = _resolveTestBaseDir(relativePath);
        libPath = path.join(baseDir, relativePath);
      } else {
        libPath = 'dbas_sqlite.so';
      }
    } else {
      throw UnsupportedError('Platform ${Platform.operatingSystem} not supported.');
    }

    return libPath.replaceAll('\\', '/');
  }

  /// Walks one level up from `Directory.current` if the relative
  /// `platformSubPath` (e.g. `windows/libs/x64/dbas_sqlite.dll`) is
  /// not present at the expected location — handles the case where
  /// `flutter test` is run from the `example/` subdirectory but the
  /// pre-built native libraries live at the package root.
  String _resolveTestBaseDir(String platformSubPath) {
    final current = Directory.current.path;
    final candidate = path.join(current, platformSubPath);
    if (File(candidate).existsSync()) {
      return current;
    }
    return path.dirname(current);
  }

  // ── Protected abstract methods (raw native calls) ──────────────────────

  // Library-scoped
  Pointer<Utf8> nativeGetSqliteVersion();
  int nativeGetAbiVersion();

  // Connection lifecycle
  Pointer<DbasSqliteDbStruct> nativeOpenDb(Pointer<Utf8> path);
  int nativeIsOpened(Pointer<DbasSqliteDbStruct> dbPtr);
  int nativeExecuteSql(Pointer<DbasSqliteDbStruct> dbPtr, Pointer<Utf8> sql);
  int nativeCloseDb(Pointer<DbasSqliteDbStruct> dbPtr, int checkpoint);
  Pointer<Utf8> nativeGetLastDbError(Pointer<DbasSqliteDbStruct> dbPtr);
  int nativeGetExtendedErrorCode(Pointer<DbasSqliteDbStruct> dbPtr);
  int nativeGetAffectedRows(Pointer<DbasSqliteDbStruct> dbPtr);
  int nativeGetLastInsertedId(Pointer<DbasSqliteDbStruct> dbPtr);
  int nativeGetTotalChanges(Pointer<DbasSqliteDbStruct> dbPtr);
  Pointer<Utf8> nativeGetDbFileName(Pointer<DbasSqliteDbStruct> dbPtr);
  int nativeSetBusyTimeout(Pointer<DbasSqliteDbStruct> dbPtr, int ms);
  int nativeEnableWal(Pointer<DbasSqliteDbStruct> dbPtr);

  // Statement lifecycle
  int nativePrepareQuery(Pointer<DbasSqliteDbStruct> dbPtr, Pointer<Utf8> sql);
  int nativeFinalizeStmt(Pointer<DbasSqliteDbStruct> dbPtr, int handle);
  int nativeReadRow(Pointer<DbasSqliteDbStruct> dbPtr, int handle);
  Pointer<Utf8> nativeGetLastStmtError(Pointer<DbasSqliteDbStruct> dbPtr, int handle);
  int nativeGetStmtAffectedRows(Pointer<DbasSqliteDbStruct> dbPtr, int handle);
  int nativeGetStmtLastInsertedId(Pointer<DbasSqliteDbStruct> dbPtr, int handle);

  // Bindings (positional) — every call takes (db, handle, index, …)
  int nativeBindNull(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int index);
  int nativeBindInt(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int index, int value);
  int nativeBindInt64(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int index, int value);
  int nativeBindFloat(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int index, double value);
  int nativeBindDouble(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int index, double value);
  int nativeBindText(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int index, Pointer<Utf8> value);
  int nativeBindBlob(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int index, Pointer<Uint8> value, int length);

  // Bindings (named)
  int nativeBindNameNull(Pointer<DbasSqliteDbStruct> dbPtr, int handle, Pointer<Utf8> name);
  int nativeBindNameInt(Pointer<DbasSqliteDbStruct> dbPtr, int handle, Pointer<Utf8> name, int value);
  int nativeBindNameInt64(Pointer<DbasSqliteDbStruct> dbPtr, int handle, Pointer<Utf8> name, int value);
  int nativeBindNameFloat(Pointer<DbasSqliteDbStruct> dbPtr, int handle, Pointer<Utf8> name, double value);
  int nativeBindNameDouble(Pointer<DbasSqliteDbStruct> dbPtr, int handle, Pointer<Utf8> name, double value);
  int nativeBindNameText(Pointer<DbasSqliteDbStruct> dbPtr, int handle, Pointer<Utf8> name, Pointer<Utf8> value);
  int nativeBindNameBlob(Pointer<DbasSqliteDbStruct> dbPtr, int handle, Pointer<Utf8> name, Pointer<Uint8> value, int length);

  // Column accessors
  int nativeIsNull(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int colIndex);
  Pointer<Utf8> nativeGetColumnText(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int colIndex);
  int nativeGetColumnInt(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int colIndex);
  int nativeGetColumnInt64(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int colIndex);
  double nativeGetColumnFloat(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int colIndex);
  double nativeGetColumnDouble(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int colIndex);
  Pointer<Uint8> nativeGetColumnBlob(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int columnIndex);
  int nativeGetColumnBytes(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int columnIndex);
  Pointer<Utf8> nativeGetColumnName(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int columnIndex);
  int nativeGetColumnType(Pointer<DbasSqliteDbStruct> dbPtr, int handle, int colIndex);
  int nativeGetColumnCount(Pointer<DbasSqliteDbStruct> dbPtr, int handle);

  // Connection pool (C-managed)
  Pointer<DbasSqlitePoolStruct> nativeCreatePool(Pointer<Utf8> path, int readerCount);
  Pointer<DbasSqliteDbStruct> nativePoolGetWriter(Pointer<DbasSqlitePoolStruct> poolPtr);
  Pointer<DbasSqliteDbStruct> nativePoolAcquireReader(Pointer<DbasSqlitePoolStruct> poolPtr);
  Pointer<DbasSqliteDbStruct> nativePoolAcquireReaderBlocking(Pointer<DbasSqlitePoolStruct> poolPtr, int timeoutMs);
  int nativePoolLastAcquireStatus();
  void nativePoolReleaseReader(Pointer<DbasSqlitePoolStruct> poolPtr, Pointer<DbasSqliteDbStruct> readerPtr);
  void nativeClosePool(Pointer<DbasSqlitePoolStruct> poolPtr);

  // ── Pointer resolution (overridable for extra validation) ──────────────

  Pointer<DbasSqliteDbStruct> resolveDbPtr(int address) =>
      Pointer<DbasSqliteDbStruct>.fromAddress(address);

  Pointer<DbasSqlitePoolStruct> resolvePoolPtr(int address) =>
      Pointer<DbasSqlitePoolStruct>.fromAddress(address);

  // ── Library-scoped ─────────────────────────────────────────────────────

  /// Captures the SQLite version into [_sqliteVersion]. Subclasses
  /// must call this once after FFI bindings are loaded.
  @protected
  void cacheSqliteVersion() {
    final ptr = nativeGetSqliteVersion();
    _sqliteVersion = (ptr == nullptr || ptr.address == 0) ? '' : ptr.toDartString();
  }

  @override
  String getSqliteVersion() => _sqliteVersion;

  @override
  int getAbiVersion() => nativeGetAbiVersion();

  // ── Shared lifecycle implementation ────────────────────────────────────

  @override
  Future<int> openDb(String path) async {
    Pointer<Utf8> pathPtr = nullptr;
    try {
      pathPtr = path.toNativeUtf8();
      final ptr = nativeOpenDb(pathPtr);
      return ptr.address;
    } finally {
      if (pathPtr != nullptr) calloc.free(pathPtr);
    }
  }

  @override
  Future<bool> databaseExists(String fileName) async {
    final dbFile = File(fileName);
    return await dbFile.exists();
  }

  /// Scratch path an attach writes to before swapping it onto
  /// [fileName]. A **sibling** of the real path, and that is
  /// load-bearing: `rename` is atomic only within one filesystem, and a
  /// temp on another volume (a system temp directory, say) would silently
  /// degrade into a copy — reopening exactly the window the temp exists
  /// to close. Deriving it by appending to the resolved database path is
  /// what makes "same directory" true by construction rather than by
  /// assumption.
  ///
  /// Deliberately NOT one of the suffixes [dropDb] deletes. [dropDb] runs
  /// *between* the write and the rename, so a drop that also swept this
  /// path would delete the freshly-written replacement one line before it
  /// is swapped in.
  String _attachTempPath(String fileName) => '$fileName.attach.tmp';

  /// Replaces [fileName]'s database with [content].
  ///
  /// **Writes a sibling temp, then swaps.** The eager variant of the same
  /// atomicity contract [attachStreamDb] documents at length: the real
  /// path is either the database that was there before or the database
  /// the caller supplied, never a fragment of the second one. Its window
  /// was much smaller than the streaming variant's — `writeAsBytes` on an
  /// in-memory buffer, not a download — but not zero, and not
  /// zero-consequence: `writeAsBytes` opens for writing (truncating)
  /// before copying, so a write that dies partway used to leave a
  /// truncated file where the previous database had been.
  ///
  /// `flush: true` is load-bearing here, not tidiness. Publishing a file
  /// by renaming it means the rename must not be able to complete over
  /// bytes the OS is still holding — otherwise a crash immediately after
  /// the swap leaves a correctly-named, incompletely-written database,
  /// which is the same class of harm one layer down.
  @override
  Future attachDb(String fileName, List<int> content) async {
    final tmpFile = File(_attachTempPath(fileName));
    try {
      await tmpFile.writeAsBytes(content, flush: true);
      await dropDb(fileName);
      await tmpFile.rename(fileName);
    } finally {
      await _deleteAttachTemp(tmpFile);
    }
  }

  /// Removes an attach's scratch file without ever replacing the failure
  /// that brought teardown here.
  ///
  /// **The guard is load-bearing, and the argument that it was not is
  /// wrong past one specific line.** "A cleanup failure is acceptable
  /// because the DATABASE is intact — what is lost is only the
  /// description of why the attach failed" holds while the temp is being
  /// written, and stops holding the moment [dropDb] has run: [dropDb]
  /// attempts all four deletions and reports the ones that failed, so the
  /// destination can be HALF-DELETED — a `.db` gone with a `-wal` left
  /// behind is a corrupt-looking next open, not an intact database. And
  /// the two failures correlate: on Windows the thing that makes a
  /// delete of the live path throw (a lock over that directory, or a
  /// handle the OS has not released yet) is exactly what makes this
  /// delete throw too. Unguarded, the error that survives names
  /// `.attach.tmp` and says nothing at all about the `-wal` that could
  /// not be removed from the real path.
  ///
  /// Reported rather than swallowed: a stranded scratch file is debris
  /// the next attach truncates, so it costs nothing but it is also
  /// nobody's recovery artifact and no consumer knows to look for one.
  /// [DbasSqlite.reportDiagnosticInternal] is what a consumer can
  /// actually read — `developer.log` is dropped whenever no VM service
  /// client is subscribed.
  Future<void> _deleteAttachTemp(File tmpFile) async {
    try {
      if (await tmpFile.exists()) await tmpFile.delete();
    } catch (e, st) {
      developer.log(
        'attach: deleting the scratch file "${tmpFile.path}" failed',
        name: 'dbas_sqlite.DbasSqliteNativeAppBase',
        error: e,
        stackTrace: st,
      );
      DbasSqlite.reportDiagnosticInternal(
        'attach: deleting the scratch file "${tmpFile.path}" failed ($e). '
        'It is debris the next attach truncates, so nothing depends on it '
        'being gone. This is reported and NOT raised because raising it '
        'would replace whatever failure brought the attach here — and '
        'when that failure came from dropDb the destination may be '
        'half-deleted, which is the part worth reading.',
        name: 'dbas_sqlite.DbasSqliteNativeAppBase',
      );
    }
  }

  /// Replaces [fileName]'s database with the bytes arriving on [stream].
  ///
  /// **Writes a sibling temp, flushes it, closes it, and only then drops
  /// the destination and renames the temp onto it.** The order is the
  /// whole point. Deleting first and writing to the real path afterwards
  /// meant that from [dropDb] until the last chunk landed there was no
  /// database at [fileName] — only an absence, and then a fragment — and
  /// the length of that window is the CALLER's to decide, not this
  /// library's: [stream] is caller-supplied, and where it is network-fed
  /// the window is the whole download. A source that failed partway
  /// therefore destroyed the database it was replacing and left a
  /// truncated file in its place, with nothing left to retry against.
  ///
  /// Now a failed attach is a no-op: the real path is untouched until a
  /// complete replacement exists beside it, and the `finally` removes the
  /// scratch file on every path. `openWrite()` truncates, so a temp
  /// stranded by a killed process is reused rather than appended to.
  ///
  /// **Its sibling [streamCopyDb] is not the same code despite looking
  /// like it.** That method also deletes its destination first, and may:
  /// its SOURCE is a local file already settled by a checkpoint before
  /// any deletion happens, so there is no window in which the outcome
  /// depends on something that can still fail. The asymmetry is in the
  /// source, not in the deletion order.
  ///
  /// **Residual, stated rather than papered over.** Between [dropDb]
  /// returning and `rename` completing the old database is gone and the
  /// new one is not yet in place. Nothing that can take time happens
  /// there — the payload is already written and fsynced — so the window
  /// is a single metadata operation rather than a transfer, but it is not
  /// nothing. Closing it entirely needs a rename that is also a delete of
  /// the four SQLite side files, which no filesystem offers as one
  /// operation.
  ///
  /// A rejected alternative: keeping the destination in place and writing
  /// through it, restoring the previous bytes on failure. That needs the
  /// whole previous database held somewhere for the duration — which is
  /// the temp file again, only now on the failure path where it is least
  /// affordable — and it still cannot survive the process dying mid-restore.
  @override
  Future attachStreamDb(String fileName, Stream<List<int>> stream) async {
    final tmpFile = File(_attachTempPath(fileName));
    try {
      final sink = tmpFile.openWrite();
      try {
        await for (final chunk in stream) {
          sink.add(chunk);
        }
        await sink.flush();
      } finally {
        await sink.close();
      }
      await dropDb(fileName);
      await tmpFile.rename(fileName);
    } finally {
      // Unconditional, and after a successful rename it finds nothing:
      // the temp no longer exists under that name. The scratch file is
      // never a recovery artifact — no consumer knows to look for it —
      // so leaving one behind on the failure path would only be debris
      // for the next attach to truncate. See [_deleteAttachTemp] for why
      // its own failure is reported instead of raised.
      await _deleteAttachTemp(tmpFile);
    }
  }

  @override
  Future<List<int>> getContent(String fileName) async {
    final dbFile = File(fileName);
    return await dbFile.readAsBytes();
  }

  @override
  Future<void> streamCopyDb(String sourceFileName, String destFileName) async {
    for (final ext in ['', '-wal', '-shm']) {
      final f = File('$destFileName$ext');
      if (await f.exists()) await f.delete();
    }

    final sourceFile = File(sourceFileName);
    final destFile = File(destFileName);
    final sink = destFile.openWrite();
    try {
      await sourceFile.openRead().pipe(sink);
    } finally {
      await sink.close();
    }
  }

  @override
  Future<void> dropDb(String fileName) async {
    // Attempt every deletion even if one fails — partial cleanup is
    // worse than reporting all failures together. A leftover WAL/SHM
    // after the .db is gone produces a corrupt-looking next openDb.
    final errors = <String>[];
    for (final ext in ['', '-wal', '-shm', '-journal']) {
      final f = File('$fileName$ext');
      try {
        if (await f.exists()) await f.delete();
      } catch (e) {
        errors.add('$fileName$ext: $e');
      }
    }
    if (errors.isNotEmpty) {
      throw FileSystemException(
        'dropDb: ${errors.length} of 4 paths failed to delete:\n'
        '${errors.join("\n")}',
        fileName,
      );
    }
  }

  @override
  bool isOpened(int dbPtr) => nativeIsOpened(resolveDbPtr(dbPtr)) == 1;

  @override
  Future<int> executeSql(int dbPtr, String sql) async {
    Pointer<Utf8> sqlPtr = nullptr;
    try {
      sqlPtr = sql.toNativeUtf8();
      return nativeExecuteSql(resolveDbPtr(dbPtr), sqlPtr);
    } finally {
      if (sqlPtr != nullptr) calloc.free(sqlPtr);
    }
  }

  @override
  Future<int> closeDb(int dbPtr, {bool checkpoint = false}) async =>
      nativeCloseDb(resolveDbPtr(dbPtr), checkpoint ? 1 : 0);

  @override
  String? getLastDbError(int dbPtr) {
    final ptr = nativeGetLastDbError(resolveDbPtr(dbPtr));
    return (ptr == nullptr || ptr.address == 0) ? null : ptr.toDartString();
  }

  @override
  int? getUniqueErrorCode(int dbPtr) {
    // Returns null when the connection has no active error. The C
    // wrapper returns `0` for "no error" and `-1` for a NULL / unopened
    // connection — neither is a legitimate SQLite rc, so both collapse
    // to null. Some bind-time failures (e.g. SQLITE_RANGE from
    // `sqlite3_bind_*`) propagate the rc to the caller without
    // updating `sqlite3_extended_errcode`, so callers fall back to the
    // primary rc they observed at the failing call site.
    final ext = nativeGetExtendedErrorCode(resolveDbPtr(dbPtr));
    return ext <= 0 ? null : ext;
  }

  @override
  int? getErrorCode(int dbPtr) {
    // SQLite encodes the primary code in the low byte of the extended
    // code, so deriving one from the other avoids a second FFI lookup
    // for `sqlite3_errcode`. Returns null if the lib reports no error
    // (extended == 0) or a NULL/unopened connection (extended == -1) —
    // taking `& 0xFF` of -1 would yield a nonsense 255 that masks the
    // sentinel.
    final ext = nativeGetExtendedErrorCode(resolveDbPtr(dbPtr));
    return ext <= 0 ? null : (ext & 0xFF);
  }

  @override
  int getAffectedRows(int dbPtr) => nativeGetAffectedRows(resolveDbPtr(dbPtr));

  @override
  int getLastInsertedId(int dbPtr) =>
      nativeGetLastInsertedId(resolveDbPtr(dbPtr));

  @override
  int getTotalChanges(int dbPtr) => nativeGetTotalChanges(resolveDbPtr(dbPtr));

  @override
  String? getDbFileName(int dbPtr) {
    final ptr = nativeGetDbFileName(resolveDbPtr(dbPtr));
    return (ptr == nullptr || ptr.address == 0) ? null : ptr.toDartString();
  }

  @override
  Future<int> setBusyTimeout(int dbPtr, int ms) async =>
      nativeSetBusyTimeout(resolveDbPtr(dbPtr), ms);

  @override
  Future<int> enableWal(int dbPtr) async => nativeEnableWal(resolveDbPtr(dbPtr));

  // ── Statement lifecycle ────────────────────────────────────────────────

  @override
  Future<({int handle, int columnCount, List<String> columnNames})>
      prepareQuery(int dbPtr, String sql) async {
    Pointer<Utf8> sqlPtr = nullptr;
    try {
      sqlPtr = sql.toNativeUtf8();
      final ptr = resolveDbPtr(dbPtr);
      final handle = nativePrepareQuery(ptr, sqlPtr);
      if (handle == sqliteInvalidStmtHandle) {
        return (
          handle: handle,
          columnCount: 0,
          columnNames: const <String>[],
        );
      }
      // Capture column metadata once — stable for the lifetime of the
      // prepared statement, available before the first step.
      final count = nativeGetColumnCount(ptr, handle);
      final names = <String>[];
      for (int i = 0; i < count; i++) {
        final namePtr = nativeGetColumnName(ptr, handle, i);
        names.add((namePtr == nullptr || namePtr.address == 0)
            ? ''
            : namePtr.toDartString());
      }
      return (handle: handle, columnCount: count, columnNames: names);
    } finally {
      if (sqlPtr != nullptr) calloc.free(sqlPtr);
    }
  }

  @override
  Future<int> finalizeStmt(int dbPtr, int handle) async =>
      nativeFinalizeStmt(resolveDbPtr(dbPtr), handle);

  @override
  Future<int> readRowAndCache(int dbPtr, int handle, RowData cache) async {
    final ptr = resolveDbPtr(dbPtr);
    final status = nativeReadRow(ptr, handle);

    if (status == sqliteRow) {
      // columnCount / columnNames are populated at prepare time
      // (see prepareQuery). Honour the cached value but heal the
      // cache if for any reason it disagrees with the live column
      // count — defensive against future code paths that bypass
      // prepareQuery's metadata capture.
      final count = nativeGetColumnCount(ptr, handle);
      if (cache.columnCount != count) cache.columnCount = count;
      if (cache.columnNames.length != count) {
        final names = <String>[];
        for (int i = 0; i < count; i++) {
          names.add(nativeGetColumnName(ptr, handle, i).toDartString());
        }
        cache.columnNames = names;
      }
      final columns = <ColumnData>[];
      for (int i = 0; i < count; i++) {
        final type = nativeGetColumnType(ptr, handle, i);
        final isNull = nativeIsNull(ptr, handle, i) == 1;
        dynamic value;
        if (!isNull) {
          switch (type) {
            case 1: // SQLITE_INTEGER
              value = nativeGetColumnInt64(ptr, handle, i);
            case 2: // SQLITE_FLOAT
              value = nativeGetColumnDouble(ptr, handle, i);
            case 3: // SQLITE_TEXT
              value = nativeGetColumnText(ptr, handle, i).toDartString();
            case 4: // SQLITE_BLOB
              final blobPtr = nativeGetColumnBlob(ptr, handle, i);
              final length = nativeGetColumnBytes(ptr, handle, i);
              value = blobPtr.asTypedList(length).toList();
            default:
              // Unknown SQLite type code — surface loudly so a
              // future SQLite extension or a custom type doesn't
              // silently lose its value. Treat as text (the
              // C lib's GetColumnText will coerce per SQLite's
              // type-affinity rules).
              developer.log(
                'Unknown SQLite column type $type at index $i — '
                'falling back to text coercion',
                name: 'dbas_sqlite.DbasSqliteNativeAppBase',
              );
              final textPtr = nativeGetColumnText(ptr, handle, i);
              value = (textPtr == nullptr || textPtr.address == 0)
                  ? null
                  : textPtr.toDartString();
          }
        }
        columns.add(ColumnData(type: type, isNull: isNull, value: value));
      }
      cache.columns = columns;
    } else {
      cache.columns = null;
    }
    return status;
  }

  @override
  String? getLastStmtError(int dbPtr, int handle) {
    final ptr = nativeGetLastStmtError(resolveDbPtr(dbPtr), handle);
    return (ptr == nullptr || ptr.address == 0) ? null : ptr.toDartString();
  }

  @override
  int getStmtAffectedRows(int dbPtr, int handle) =>
      nativeGetStmtAffectedRows(resolveDbPtr(dbPtr), handle);

  @override
  int getStmtLastInsertedId(int dbPtr, int handle) =>
      nativeGetStmtLastInsertedId(resolveDbPtr(dbPtr), handle);

  // ── Bindings (positional) ──────────────────────────────────────────────
  // The base implementations are synchronous from Dart's perspective —
  // they call into native FFI directly. They are typed as `Future<int>`
  // in the interface so the worker variant can override them with an
  // isolate dispatch; the base supports both shapes via `async` returns.

  @override
  Future<int> bindNull(int dbPtr, int handle, int index) async =>
      nativeBindNull(resolveDbPtr(dbPtr), handle, index);

  @override
  Future<int> bindInt(int dbPtr, int handle, int index, int value) async =>
      nativeBindInt(resolveDbPtr(dbPtr), handle, index, value);

  @override
  Future<int> bindInt64(int dbPtr, int handle, int index, int value) async =>
      nativeBindInt64(resolveDbPtr(dbPtr), handle, index, value);

  @override
  Future<int> bindFloat(int dbPtr, int handle, int index, double value) async =>
      nativeBindFloat(resolveDbPtr(dbPtr), handle, index, value);

  @override
  Future<int> bindDouble(int dbPtr, int handle, int index, double value) async =>
      nativeBindDouble(resolveDbPtr(dbPtr), handle, index, value);

  @override
  Future<int> bindText(int dbPtr, int handle, int index, String value) async {
    Pointer<Utf8> valuePtr = nullptr;
    try {
      valuePtr = value.toNativeUtf8();
      return nativeBindText(resolveDbPtr(dbPtr), handle, index, valuePtr);
    } finally {
      if (valuePtr != nullptr) calloc.free(valuePtr);
    }
  }

  @override
  Future<int> bindBlob(int dbPtr, int handle, int index, List<int> value) async {
    Pointer<Uint8> ptr = nullptr;
    try {
      ptr = calloc<Uint8>(value.length);
      for (var i = 0; i < value.length; i++) {
        ptr[i] = value[i];
      }
      return nativeBindBlob(resolveDbPtr(dbPtr), handle, index, ptr, value.length);
    } finally {
      if (ptr != nullptr) calloc.free(ptr);
    }
  }

  // ── Bindings (named) ───────────────────────────────────────────────────

  @override
  Future<int> bindNameNull(int dbPtr, int handle, String name) async {
    Pointer<Utf8> namePtr = nullptr;
    try {
      namePtr = name.toNativeUtf8();
      return nativeBindNameNull(resolveDbPtr(dbPtr), handle, namePtr);
    } finally {
      if (namePtr != nullptr) calloc.free(namePtr);
    }
  }

  @override
  Future<int> bindNameInt(int dbPtr, int handle, String name, int value) async {
    Pointer<Utf8> namePtr = nullptr;
    try {
      namePtr = name.toNativeUtf8();
      return nativeBindNameInt(resolveDbPtr(dbPtr), handle, namePtr, value);
    } finally {
      if (namePtr != nullptr) calloc.free(namePtr);
    }
  }

  @override
  Future<int> bindNameInt64(int dbPtr, int handle, String name, int value) async {
    Pointer<Utf8> namePtr = nullptr;
    try {
      namePtr = name.toNativeUtf8();
      return nativeBindNameInt64(resolveDbPtr(dbPtr), handle, namePtr, value);
    } finally {
      if (namePtr != nullptr) calloc.free(namePtr);
    }
  }

  @override
  Future<int> bindNameFloat(int dbPtr, int handle, String name, double value) async {
    Pointer<Utf8> namePtr = nullptr;
    try {
      namePtr = name.toNativeUtf8();
      return nativeBindNameFloat(resolveDbPtr(dbPtr), handle, namePtr, value);
    } finally {
      if (namePtr != nullptr) calloc.free(namePtr);
    }
  }

  @override
  Future<int> bindNameDouble(int dbPtr, int handle, String name, double value) async {
    Pointer<Utf8> namePtr = nullptr;
    try {
      namePtr = name.toNativeUtf8();
      return nativeBindNameDouble(resolveDbPtr(dbPtr), handle, namePtr, value);
    } finally {
      if (namePtr != nullptr) calloc.free(namePtr);
    }
  }

  @override
  Future<int> bindNameText(int dbPtr, int handle, String name, String value) async {
    Pointer<Utf8> namePtr = nullptr;
    Pointer<Utf8> valuePtr = nullptr;
    try {
      namePtr = name.toNativeUtf8();
      valuePtr = value.toNativeUtf8();
      return nativeBindNameText(resolveDbPtr(dbPtr), handle, namePtr, valuePtr);
    } finally {
      if (namePtr != nullptr) calloc.free(namePtr);
      if (valuePtr != nullptr) calloc.free(valuePtr);
    }
  }

  @override
  Future<int> bindNameBlob(int dbPtr, int handle, String name, List<int> value) async {
    Pointer<Utf8> namePtr = nullptr;
    Pointer<Uint8> ptr = nullptr;
    try {
      namePtr = name.toNativeUtf8();
      ptr = calloc<Uint8>(value.length);
      for (var i = 0; i < value.length; i++) {
        ptr[i] = value[i];
      }
      return nativeBindNameBlob(resolveDbPtr(dbPtr), handle, namePtr, ptr, value.length);
    } finally {
      if (ptr != nullptr) calloc.free(ptr);
      if (namePtr != nullptr) calloc.free(namePtr);
    }
  }

  // ── Column accessors — kept for ABI completeness ──────────────────────
  // Both the native pool path and the native single-connection path
  // go through `readRowAndCache` + per-reader `RowData`, so these are
  // not called from any current consumer. Leaving them in keeps the
  // FFI binding table aligned with the C header so a future
  // direct-step reader can use them without re-binding.

  @override
  bool isNull(int dbPtr, int handle, int colIndex) =>
      nativeIsNull(resolveDbPtr(dbPtr), handle, colIndex) == 1;

  @override
  String getColumnText(int dbPtr, int handle, int colIndex) {
    final ptr = nativeGetColumnText(resolveDbPtr(dbPtr), handle, colIndex);
    return (ptr == nullptr || ptr.address == 0) ? '' : ptr.toDartString();
  }

  @override
  int getColumnInt(int dbPtr, int handle, int colIndex) =>
      nativeGetColumnInt(resolveDbPtr(dbPtr), handle, colIndex);

  @override
  int getColumnInt64(int dbPtr, int handle, int colIndex) =>
      nativeGetColumnInt64(resolveDbPtr(dbPtr), handle, colIndex);

  @override
  double getColumnFloat(int dbPtr, int handle, int colIndex) =>
      nativeGetColumnFloat(resolveDbPtr(dbPtr), handle, colIndex);

  @override
  double getColumnDouble(int dbPtr, int handle, int colIndex) =>
      nativeGetColumnDouble(resolveDbPtr(dbPtr), handle, colIndex);

  @override
  List<int> getColumnBlob(int dbPtr, int handle, int columnIndex) {
    final length = nativeGetColumnBytes(resolveDbPtr(dbPtr), handle, columnIndex);
    if (length <= 0) return const [];
    final ptr = nativeGetColumnBlob(resolveDbPtr(dbPtr), handle, columnIndex);
    if (ptr == nullptr || ptr.address == 0) return const [];
    return ptr.asTypedList(length).toList();
  }

  @override
  int getColumnBytes(int dbPtr, int handle, int columnIndex) =>
      nativeGetColumnBytes(resolveDbPtr(dbPtr), handle, columnIndex);

  @override
  String getColumnName(int dbPtr, int handle, int columnIndex) {
    final ptr = nativeGetColumnName(resolveDbPtr(dbPtr), handle, columnIndex);
    return (ptr == nullptr || ptr.address == 0) ? '' : ptr.toDartString();
  }

  @override
  int getColumnType(int dbPtr, int handle, int colIndex) =>
      nativeGetColumnType(resolveDbPtr(dbPtr), handle, colIndex);

  @override
  int getColumnCount(int dbPtr, int handle) =>
      nativeGetColumnCount(resolveDbPtr(dbPtr), handle);

  // ── Connection Pool (C-managed) ────────────────────────────────────────

  @override
  Future<int> createPool(String path, int readerCount) async {
    Pointer<Utf8> pathPtr = nullptr;
    try {
      pathPtr = path.toNativeUtf8();
      final poolPtr = nativeCreatePool(pathPtr, readerCount);
      if (poolPtr == nullptr || poolPtr.address == 0) {
        throw Exception('Failed to create pool: $path');
      }
      return poolPtr.address;
    } finally {
      if (pathPtr != nullptr) calloc.free(pathPtr);
    }
  }

  @override
  int poolGetWriter(int poolPtr) =>
      nativePoolGetWriter(resolvePoolPtr(poolPtr)).address;

  @override
  int poolAcquireReader(int poolPtr) {
    final reader = nativePoolAcquireReader(resolvePoolPtr(poolPtr));
    if (reader == nullptr || reader.address == 0) return 0;
    return reader.address;
  }

  @override
  Future<({int readerPtr, PoolAcquireStatus status})> poolAcquireReaderBlocking(
      int poolPtr, int timeoutMs) async {
    final reader =
        nativePoolAcquireReaderBlocking(resolvePoolPtr(poolPtr), timeoutMs);
    // Same-thread read: this direct-FFI path runs the acquire and the
    // status read back-to-back with no await between them, so the
    // per-thread native status is still this acquire's.
    final status = PoolAcquireStatus.fromCode(nativePoolLastAcquireStatus());
    final ptr = (reader == nullptr || reader.address == 0) ? 0 : reader.address;
    return (readerPtr: ptr, status: status);
  }

  @override
  void poolReleaseReader(int poolPtr, int readerPtr) {
    nativePoolReleaseReader(resolvePoolPtr(poolPtr), resolveDbPtr(readerPtr));
  }

  @override
  Future<void> closePool(int poolPtr) async {
    nativeClosePool(resolvePoolPtr(poolPtr));
  }
}

