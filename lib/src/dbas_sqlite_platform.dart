import 'package:dbas_sqlite/src/native/dbas_sqlite_native_interface.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_db.dart'
    if (dart.library.js_interop) 'package:dbas_sqlite/src/stub/dbas_sqlite_db_stub.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_row_cache.dart';
import 'package:flutter/foundation.dart' show visibleForTesting;

/// Platform dispatcher — routes per-call work to the
/// per-`dbName` [DbasSqliteNativeInterface] delegate. Stateless: every
/// call unwraps the [DbasSqliteDb] handle to (name, ptr) and forwards.
final class DbasSqlitePlatform {
  static DbasSqlitePlatform? _instance;
  static final Map<String, DbasSqliteNativeInterface> _delegate = {};

  DbasSqlitePlatform._dbasSqlitePlatform();

  static Future<DbasSqlitePlatform> getInstance({String dbName = 'dbas.db'}) async {
    _getInterface(dbName: dbName);
    _instance ??= DbasSqlitePlatform._dbasSqlitePlatform();
    await _instance!.initialize(dbName);
    return _instance!;
  }

  static DbasSqliteNativeInterface _getInterface({String dbName = 'dbas.db'}) {
    if (!_delegate.containsKey(dbName)) {
      _delegate[dbName] = DbasSqliteNativeInterface.getInstance(dbName: dbName);
    }
    return _delegate[dbName]!;
  }

  Future<void> initialize(String name) async => await _delegate[name]!.initialize();

  /// Test-only seam: routes every later platform call for [dbName]
  /// through [delegate] instead of the real one.
  ///
  /// **This map is the ONLY injection point there is.** A double for this
  /// class itself is impossible — it is a `final class`, so nothing
  /// outside its own library can extend or implement it — while
  /// [DbasSqliteNativeInterface] is a plain abstract class and is what
  /// every method above actually resolves through. Injecting here is
  /// therefore not a shortcut around a nicer seam; it is the seam.
  ///
  /// What it exists to make reachable is the set of native outcomes the
  /// real FFI layer cannot be asked to produce on demand: a specific
  /// [PoolAcquireStatus], a `finalizeStmt` that fails, a `poolReleaseReader`
  /// that can be OBSERVED rather than inferred. Wrap the real delegate
  /// (`DbasSqliteNativeInterface.getInstance(dbName: dbName)`) and override
  /// only the calls under test, so everything else keeps running against
  /// real SQLite.
  ///
  /// Production behaviour is unchanged by the existence of this method:
  /// nothing in this class reads a flag it sets, and with no test calling
  /// it the map is populated exactly as before, lazily, by [_getInterface].
  /// Pair it with [debugResetDelegates] in a `finally` / `addTearDown` —
  /// the map is static, so a leaked wrapper would intercept every later
  /// test's calls for that database.
  @visibleForTesting
  static void debugInjectDelegate(
      String dbName, DbasSqliteNativeInterface delegate) {
    // Recorded so [debugResetDelegates] can undo exactly this and nothing
    // else. The PREVIOUS occupant is kept, not just the key: a reset that
    // removed the key instead would leave the injected database with no
    // entry at all, and every accessor above indexes the map with `!`.
    _injectedDelegates[dbName] =
        (injected: delegate, previous: _delegate[dbName]);
    _delegate[dbName] = delegate;
  }

  /// Every delegate [debugInjectDelegate] has installed and not yet
  /// undone, with the entry it displaced.
  static final Map<
      String,
      ({
        DbasSqliteNativeInterface injected,
        DbasSqliteNativeInterface? previous
      })> _injectedDelegates = {};

  /// Undoes every [debugInjectDelegate] made since the last reset,
  /// restoring the entry each one displaced.
  ///
  /// **Scoped to what was injected, and that scoping is load-bearing.**
  /// This used to `clear()` the whole map, which is a process-wide effect
  /// dressed up as a test teardown: the map is static and is never
  /// cleared on close, so it accumulates an entry for every database the
  /// run has ever touched — measured, the first call in this package's
  /// suite dropped **fifteen** of them. Every accessor above resolves
  /// through `_delegate[name]!` and only [_getInterface] ever repopulates
  /// a key — and that is reached solely from [getInstance] / [createPool]
  /// — so a cleared entry belonging to an OPEN database is never
  /// repaired: its next call, `closeDb()` included, dies on a null-check
  /// of a missing key and the database is left permanently unusable and
  /// unclosable. That the suite stayed green only says none of those
  /// fifteen happened to be open at that instant.
  ///
  /// Restoring rather than removing keeps the injected database usable
  /// too: `DbasSqliteNativeInterface.getInstance` would hand back the
  /// same per-`dbName` instance, so restoring is identity-preserving and
  /// loses no native state, but it does not depend on a later
  /// [_getInterface] that may never come.
  ///
  /// An entry someone else has since replaced is left alone — the same
  /// identity guard, for the same reason, as the instance-map removals in
  /// [DbasSqlite]: "undo my injection" is "restore this key" only while
  /// the key still holds what this injected.
  @visibleForTesting
  static void debugResetDelegates() {
    for (final entry in _injectedDelegates.entries) {
      if (!identical(_delegate[entry.key], entry.value.injected)) continue;
      final previous = entry.value.previous;
      if (previous == null) {
        _delegate.remove(entry.key);
      } else {
        _delegate[entry.key] = previous;
      }
    }
    _injectedDelegates.clear();
  }

  /// Direct access to the underlying delegate. Reserved for code
  /// paths that need access to platform-specific helpers not exposed
  /// on the abstract interface; the per-stmt prepare/bind/step/finalize
  /// flow uses the abstract interface methods directly.
  DbasSqliteNativeInterface delegate(String dbName) => _delegate[dbName]!;

  // ── Library-scoped ───────────────────────────────────────────────────

  String getSqliteVersion(String dbName) =>
      _delegate[dbName]!.getSqliteVersion();
  int getAbiVersion(String dbName) => _delegate[dbName]!.getAbiVersion();

  // ── Connection lifecycle ─────────────────────────────────────────────

  Future<DbasSqliteDb> openDb(String fileName) async {
    final dbName = _getDbName(fileName);
    final dbPtr = await _getInterface(dbName: dbName).openDb(fileName);
    final isOpened = _delegate[dbName]!.isOpened(dbPtr);
    final lastError = isOpened ? null : _delegate[dbName]!.getLastDbError(dbPtr);
    if (dbPtr == 0 || !isOpened) {
      final msg = (lastError != null && lastError.isNotEmpty) ? lastError : 'Unknown error';
      throw Exception('Failed to open db in: $fileName. Reason: $msg');
    }
    return DbasSqliteDb(dbName, dbPtr);
  }

  bool isOpened(DbasSqliteDb db) => _delegate[db.name]!.isOpened(db.ptr);

  Future<int> closeDb(DbasSqliteDb db, {bool checkpoint = false}) async =>
      await _delegate[db.name]!.closeDb(db.ptr, checkpoint: checkpoint);

  /// SQLite **primary** result code for [db]'s last failed operation
  /// (e.g. `19` for `SQLITE_CONSTRAINT`, `5` for `SQLITE_BUSY`).
  ///
  /// Native: derived as the low byte of [getUniqueErrorCode]
  /// (`sqlite3_extended_errcode`'s low byte is the primary code by
  /// SQLite convention). Web: returns the `rc` field of the most
  /// recent worker `postErr` envelope routed through this shim, or
  /// `null` when no worker-side error has been observed since the
  /// last successful call.
  ///
  /// MUST be called right after observing a non-OK rc from a C call
  /// against the same connection; otherwise the result may reflect a
  /// different operation.
  int? getErrorCode(DbasSqliteDb db) =>
      _delegate[db.name]!.getErrorCode(db.ptr);

  /// SQLite **extended** result code for [db]'s last failed operation
  /// — the unique discriminator within a primary code (e.g. `2067` for
  /// `SQLITE_CONSTRAINT_UNIQUE`, `787` for `SQLITE_CONSTRAINT_FOREIGNKEY`).
  ///
  /// Native: returned by the native lib's `GetExtendedErrorCode` FFI
  /// entry point (`sqlite3_extended_errcode`). Web: returned via the
  /// `extendedRc` field of the worker's `postErr` envelope. `null`
  /// when the SQLite layer didn't queue an extended rc (e.g. some
  /// bind-time failures) or when no error has been observed.
  ///
  /// Same temporal constraint as [getErrorCode]: call right after the
  /// failing rc is observed.
  int? getUniqueErrorCode(DbasSqliteDb db) =>
      _delegate[db.name]!.getUniqueErrorCode(db.ptr);

  // ── File operations ──────────────────────────────────────────────────

  Future<bool> databaseExists(String fileName) async {
    final dbName = _getDbName(fileName);
    return await _delegate[dbName]!.databaseExists(fileName);
  }

  Future attachDb(String fileName, List<int> content) async {
    final dbName = _getDbName(fileName);
    await _delegate[dbName]!.attachDb(fileName, content);
  }

  Future attachStreamDb(String fileName, Stream<List<int>> stream) async {
    final dbName = _getDbName(fileName);
    await _delegate[dbName]!.attachStreamDb(fileName, stream);
  }

  Future<List<int>> getContent(String fileName) async {
    final dbName = _getDbName(fileName);
    return await _delegate[dbName]!.getContent(fileName);
  }

  Future<void> streamCopyDb(String src, String dest) async {
    final dbName = _getDbName(src);
    await _delegate[dbName]!.streamCopyDb(src, dest);
  }

  Future dropDb(String fileName) async {
    final dbName = _getDbName(fileName);
    await _delegate[dbName]!.dropDb(fileName);
    _delegate.remove(dbName);
    DbasSqliteNativeInterface.removeInstance(dbName: dbName);
  }

  // ── One-shot SQL (DDL/DML, transaction primitives) ───────────────────

  Future<int> executeSql(DbasSqliteDb db, String sql) async =>
      await _delegate[db.name]!.executeSql(db.ptr, sql);

  // ── Statement (handle-aware) ─────────────────────────────────────────

  Future<({int handle, int columnCount, List<String> columnNames})>
      prepareQuery(DbasSqliteDb db, String sql) async =>
          await _delegate[db.name]!.prepareQuery(db.ptr, sql);

  Future<int> finalizeStmt(DbasSqliteDb db, int handle) async =>
      await _delegate[db.name]!.finalizeStmt(db.ptr, handle);

  Future<int> readRowAndCache(DbasSqliteDb db, int handle, RowData cache) async =>
      await _delegate[db.name]!.readRowAndCache(db.ptr, handle, cache);

  String? getLastStmtError(DbasSqliteDb db, int handle) =>
      _delegate[db.name]!.getLastStmtError(db.ptr, handle);

  int getStmtAffectedRows(DbasSqliteDb db, int handle) =>
      _delegate[db.name]!.getStmtAffectedRows(db.ptr, handle);

  int getStmtLastInsertedId(DbasSqliteDb db, int handle) =>
      _delegate[db.name]!.getStmtLastInsertedId(db.ptr, handle);

  // ── Bindings (positional, async) ─────────────────────────────────────

  Future<int> bindNull(DbasSqliteDb db, int handle, int index) =>
      _delegate[db.name]!.bindNull(db.ptr, handle, index);
  Future<int> bindInt(DbasSqliteDb db, int handle, int index, int value) =>
      _delegate[db.name]!.bindInt(db.ptr, handle, index, value);
  Future<int> bindInt64(DbasSqliteDb db, int handle, int index, int value) =>
      _delegate[db.name]!.bindInt64(db.ptr, handle, index, value);
  Future<int> bindFloat(DbasSqliteDb db, int handle, int index, double value) =>
      _delegate[db.name]!.bindFloat(db.ptr, handle, index, value);
  Future<int> bindDouble(DbasSqliteDb db, int handle, int index, double value) =>
      _delegate[db.name]!.bindDouble(db.ptr, handle, index, value);
  Future<int> bindText(DbasSqliteDb db, int handle, int index, String value) =>
      _delegate[db.name]!.bindText(db.ptr, handle, index, value);
  Future<int> bindBlob(DbasSqliteDb db, int handle, int index, List<int> value) =>
      _delegate[db.name]!.bindBlob(db.ptr, handle, index, value);

  // ── Bindings (named, async) ──────────────────────────────────────────

  Future<int> bindNameNull(DbasSqliteDb db, int handle, String name) =>
      _delegate[db.name]!.bindNameNull(db.ptr, handle, name);
  Future<int> bindNameInt(DbasSqliteDb db, int handle, String name, int value) =>
      _delegate[db.name]!.bindNameInt(db.ptr, handle, name, value);
  Future<int> bindNameInt64(DbasSqliteDb db, int handle, String name, int value) =>
      _delegate[db.name]!.bindNameInt64(db.ptr, handle, name, value);
  Future<int> bindNameFloat(DbasSqliteDb db, int handle, String name, double value) =>
      _delegate[db.name]!.bindNameFloat(db.ptr, handle, name, value);
  Future<int> bindNameDouble(DbasSqliteDb db, int handle, String name, double value) =>
      _delegate[db.name]!.bindNameDouble(db.ptr, handle, name, value);
  Future<int> bindNameText(DbasSqliteDb db, int handle, String name, String value) =>
      _delegate[db.name]!.bindNameText(db.ptr, handle, name, value);
  Future<int> bindNameBlob(DbasSqliteDb db, int handle, String name, List<int> value) =>
      _delegate[db.name]!.bindNameBlob(db.ptr, handle, name, value);

  // ── Connection state accessors ───────────────────────────────────────

  String? getLastDbError(DbasSqliteDb db) =>
      _delegate[db.name]!.getLastDbError(db.ptr);

  int getAffectedRows(DbasSqliteDb db) =>
      _delegate[db.name]!.getAffectedRows(db.ptr);

  int getLastInsertedId(DbasSqliteDb db) =>
      _delegate[db.name]!.getLastInsertedId(db.ptr);

  int getTotalChanges(DbasSqliteDb db) =>
      _delegate[db.name]!.getTotalChanges(db.ptr);

  String? getDbFileName(DbasSqliteDb db) =>
      _delegate[db.name]!.getDbFileName(db.ptr);

  Future<int> setBusyTimeout(DbasSqliteDb db, int ms) async =>
      await _delegate[db.name]!.setBusyTimeout(db.ptr, ms);

  Future<int> enableWal(DbasSqliteDb db) async =>
      await _delegate[db.name]!.enableWal(db.ptr);

  // ── Connection Pool ──────────────────────────────────────────────────

  Future<int> createPool(String dbName, String fileName, int readerCount) async {
    final delegate = _getInterface(dbName: dbName);
    await delegate.initialize();
    return await delegate.createPool(fileName, readerCount);
  }

  int poolGetWriter(String dbName, int poolPtr) =>
      _delegate[dbName]!.poolGetWriter(poolPtr);

  int poolAcquireReader(String dbName, int poolPtr) =>
      _delegate[dbName]!.poolAcquireReader(poolPtr);

  Future<({int readerPtr, PoolAcquireStatus status})> poolAcquireReaderBlocking(
          String dbName, int poolPtr, int timeoutMs) async =>
      await _delegate[dbName]!.poolAcquireReaderBlocking(poolPtr, timeoutMs);

  void poolReleaseReader(String dbName, int poolPtr, int readerPtr) =>
      _delegate[dbName]!.poolReleaseReader(poolPtr, readerPtr);

  Future<void> closePool(String dbName, int poolPtr) async {
    await _delegate[dbName]!.closePool(poolPtr);
  }

  // ── Helpers ──────────────────────────────────────────────────────────

  String _getDbName(String fileName) {
    String dbName = fileName;
    if (fileName.contains('/')) {
      dbName = fileName.split('/').last;
    } else if (fileName.contains('\\')) {
      dbName = fileName.split('\\').last;
    }
    return dbName;
  }
}
