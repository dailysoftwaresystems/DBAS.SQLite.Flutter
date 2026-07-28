import 'dart:async';
import 'dart:collection';
import 'dart:developer' as developer;

import 'package:dbas_sqlite/src/dbas_sqlite_db.dart'
    if (dart.library.js_interop) 'package:dbas_sqlite/src/stub/dbas_sqlite_db_stub.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_platform.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_statement.dart';
import 'package:dbas_sqlite/src/exceptions/dbas_sqlite_exception.dart';
import 'package:dbas_sqlite/src/helpers/paths/dbas_sqlite_paths.dart' as paths;
import 'package:dbas_sqlite/src/helpers/dbas_sqlite_platform_util.dart';
import 'package:dbas_sqlite/src/native/dbas_sqlite_native_interface.dart';
import 'package:flutter/foundation.dart';

/// Opaque handle for one reentrant use of the writer connection,
/// handed out by [DbasSqlite.beginReentrantWriterOpInternal] and given
/// back to [DbasSqlite.endReentrantWriterOpInternal].
///
/// Carries the operation's process-unique `id` **and** the `generation`
/// (epoch) of the transaction it was registered against. Both halves
/// are **load-bearing**: an un-registration only takes effect when both
/// match a still-registered operation, so a token left over from a
/// transaction that has already ended can never cancel a *later*
/// transaction's live registration — see
/// [DbasSqlite.endReentrantWriterOpInternal] for the silent failure
/// that guards against.
typedef ReentrantWriterOpToken = ({int id, int generation});

/// One registered reentrant use of the writer connection. Lives in
/// `DbasSqlite._reentrantWriterOps` between
/// [DbasSqlite.beginReentrantWriterOpInternal] and
/// [DbasSqlite.endReentrantWriterOpInternal].
class _ReentrantWriterOp {
  _ReentrantWriterOp(this.generation);

  /// Epoch of the transaction this operation was registered against.
  final int generation;

  /// The still-pending dispatch chain behind this operation, or `null`
  /// when there is nothing to wait for — either the operation never
  /// registered one (reads don't), or its dispatch already settled.
  ///
  /// Never completes with an error:
  /// [DbasSqlite.trackReentrantWriterOpDispatchInternal] neutralises it
  /// so [DbasSqlite.rollback]'s drain can await it without inheriting
  /// the operation's failure.
  Future<void>? dispatch;
}

/// A cross-platform SQLite database wrapper for Flutter.
///
/// Provides a unified API to interact with SQLite databases on
/// Android, iOS, macOS, Linux, Windows and Web platforms.
///
/// Uses a singleton pattern per database name — calling [getInstance]
/// with the same `dbName` always returns the same instance.
///
/// ```dart
/// final db = await DbasSqlite.getInstance(dbName: 'myapp.db');
/// await db.openDb();
///
/// // Write
/// final stmt = await db.prepareQuery(
///   'INSERT INTO users (name, email) VALUES (?, ?)',
/// );
/// await stmt.executeSql(params: ['John', 'john@example.com']);
/// final id = stmt.getLastInsertedId();
/// await stmt.close();
///
/// // Read
/// final readStmt = await db.prepareQuery(
///   'SELECT * FROM users WHERE id > ?',
/// );
/// final reader = await readStmt.executeReader(params: [0]);
/// while (await reader.readRow()) {
///   print(reader.getColumnText(0));
/// }
/// await reader.close();
/// await readStmt.close();
///
/// await db.closeDb();
/// ```
class DbasSqlite {
  /// Default timeout for `PoolAcquireReaderBlocking` when an
  /// `executeReader` must wait for a free pool slot. The C-side
  /// condvar is signalled on every reader release so this is a
  /// pure deadline, not a polling loop.
  static const int kPoolAcquireTimeoutMs = 30000;

  /// Default per-slot timeout when [setBusyTimeout] reconfigures
  /// pool readers. Shorter than [kPoolAcquireTimeoutMs] because
  /// reconfiguration is best-effort and the user should know quickly
  /// when readers are too contended.
  static const int kSetBusyTimeoutAcquireMs = 5000;

  /// Deadline for a caller parked on the writer-lock FIFO queue
  /// (`executeSql` outside a transaction, `vacuum`, and
  /// `beginTransaction` — including every `strict: true` call). The
  /// writer-side twin of [kPoolAcquireTimeoutMs]: waiting forever turns
  /// a self-deadlock into a wedged flow with no error and no
  /// diagnostics, so the wait is bounded and surfaces
  /// [DbasSqliteErrorCode.writerLockWaitTimeout] instead.
  static const int kWriterLockWaitTimeoutMs = 30000;

  /// Test-only override for [kPoolAcquireTimeoutMs]. Setting this in
  /// production code is a smell; the field exists so timeout-path
  /// tests complete in milliseconds.
  @visibleForTesting
  static int? debugPoolAcquireTimeoutMs;

  /// Test-only override for [kSetBusyTimeoutAcquireMs]. Same rationale
  /// as [debugPoolAcquireTimeoutMs] — without this, the busy-reader
  /// negative test would pause for 5 s on every run.
  @visibleForTesting
  static int? debugSetBusyTimeoutAcquireMs;

  /// Test-only override for [kWriterLockWaitTimeoutMs]. Same rationale
  /// as [debugPoolAcquireTimeoutMs] — the self-deadlock test for
  /// `beginTransaction(strict: true)` would otherwise pause for 30 s on
  /// every run. Reset it to `null` in a `finally`; it is static, so a
  /// leaked override would shorten every later test's writer wait.
  @visibleForTesting
  static int? debugWriterLockWaitTimeoutMs;

  static final Map<String, DbasSqlite> _instance = {};

  final DbasSqlitePlatform _platform;
  final String dbName;
  DbasSqliteDb? _db;
  bool _isInTransaction = false;
  // Set when an `executeSql` runs while a transaction is active, so
  // subsequent `executeReader` / `executeScalar` calls in the same tx
  // route through the writer connection (read-your-writes). Cleared on
  // begin/commit/rollback. The flag is only read by the statement layer.
  bool _transactionHasWrites = false;

  /// `true` if the most recent [beginTransaction] call actually issued
  /// `BEGIN TRANSACTION`; `false` if it took the idempotent join path.
  /// Backing field for [startedCurrentTransaction].
  bool _startedCurrentTransaction = false;

  /// Every `executeSql` call — and the prepare phase of every
  /// `executeReader` call — currently using the writer connection
  /// REENTRANTLY, i.e. dispatched while already inside this active
  /// transaction (per [DbasSqliteStatement]'s `lockHeld` / `useWriter`
  /// routing) rather than through [_acquireWriterLock]. Keyed by the
  /// operation id inside [ReentrantWriterOpToken].
  ///
  /// Read by [commit]'s pre-flight check (a non-empty map blocks the
  /// `COMMIT`) and drained by [rollback] — see their docs for the two
  /// different hazards those two treatments close.
  final Map<int, _ReentrantWriterOp> _reentrantWriterOps = {};

  /// Source of process-unique ids for [_reentrantWriterOps]. Ids are
  /// never reused, so a token can only ever match the one operation it
  /// was handed out for.
  int _reentrantWriterOpSeq = 0;

  /// Epoch stamped onto every [ReentrantWriterOpToken]. Rotated by
  /// [_rotateReentrantWriterOpEpoch] whenever a transaction starts or
  /// ends — see [endReentrantWriterOpInternal].
  int _transactionGeneration = 0;

  int? _poolPtr;
  /// Reader count requested at openDb time; used by
  /// [setBusyTimeout] to bound its reader-reconfiguration loop. `0`
  /// when the pool wasn't created (single-connection fallback).
  int _readerPoolSize = 0;
  final Set<DbasSqliteStatement> _activeStatements = {};
  final Queue<Completer<void>> _writerWaitQueue = Queue<Completer<void>>();
  bool _writerLockHeld = false;
  // Dart-level FIFO semaphore that gates entry to
  // `poolAcquireReaderBlocking`. Capacity is set to [_readerPoolSize]
  // on openDb, so at most that many concurrent C-level blocking
  // acquires can be in flight. Without this gate, a `Future.wait` of
  // many `executeReader` calls fans out one blocking acquire per
  // call across the worker isolates; once every worker is parked
  // inside `pool_acquire_reader_blocking`, no worker remains to
  // process `prepareQuery` / `finalizeStmt` for in-flight reads, so
  // no reader is ever released and the pool deadlocks until the
  // 30 s C-side timeout fires. The gate caps concurrent blocking
  // acquires at [_readerPoolSize]; with the worker pool sized at
  // `readerPoolSize + 2`, at least two workers are always available
  // for the non-blocking read steps. Excess callers wait here in
  // Dart microtasks instead of occupying a worker.
  int _readerSlotsAvailable = 0;
  final Queue<Completer<void>> _readerSlotWaitQueue = Queue<Completer<void>>();

  /// Latched at the start of [closeDb] and cleared by [openDb]. While
  /// set, [_acquireReaderSlot] rejects synchronously with
  /// [DbasSqliteErrorCode.readerSlotWaitCancelled] and
  /// [_acquireWriterLock] with
  /// [DbasSqliteErrorCode.writerLockWaitCancelled], so no caller can
  /// race into [_platform]'s `poolAcquireReaderBlocking` / `executeSql`
  /// against the worker-isolate `closePool` / `closeDb` dispatch
  /// happening on a sibling isolate. The pre-sweep queue drain in
  /// [closeDb] complements this for already-parked waiters.
  bool _closing = false;

  /// Non-null while an [openDb] call on this instance is in flight.
  /// Concurrent [openDb] callers await this single future instead of
  /// each racing their own pool creation for the same database file —
  /// see [openDb] for why the plain `isOpened()` guard is insufficient.
  Future<void>? _opening;

  /// When `true`, binding a named parameter that does not exist in
  /// the prepared statement throws an exception instead of silently
  /// skipping it. Defaults to `false` (C#/SQLite-compatible behaviour).
  bool throwOnMissingNamedParams = false;

  DbasSqlite._dbasSqlite(this._platform, this.dbName,
      {this.throwOnMissingNamedParams = false});

  /// Returns a singleton instance of [DbasSqlite] for the given
  /// [dbName]. Defaults to `'dbas.db'`.
  static Future<DbasSqlite> getInstance({
    String dbName = 'dbas.db',
    bool throwOnMissingNamedParams = false,
    int workerPoolSize = 4,
  }) async {
    if (_instance.containsKey(dbName)) {
      assert(
        workerPoolSize == DbasSqliteNativeInterface.workerPoolSize,
        'DbasSqlite.getInstance: workerPoolSize=$workerPoolSize was passed for '
        'an already-initialized instance of "$dbName" (current pool size is '
        '${DbasSqliteNativeInterface.workerPoolSize}).',
      );
      _instance[dbName]!.throwOnMissingNamedParams = throwOnMissingNamedParams;
      return _instance[dbName]!;
    }

    DbasSqliteNativeInterface.workerPoolSize = workerPoolSize;
    _instance[dbName] = DbasSqlite._dbasSqlite(
      await DbasSqlitePlatform.getInstance(dbName: dbName),
      dbName,
      throwOnMissingNamedParams: throwOnMissingNamedParams,
    );
    return _instance[dbName]!;
  }

  // ── Lifecycle ────────────────────────────────────────────────────────

  /// Returns the full filesystem path for the database.
  Future<String> getAppDatabasePath({String? dbName}) async {
    dbName ??= this.dbName;
    final dbPath = await paths.resolveDatabaseDirectory(
      isTest: DbasSqlitePlatformUtil.isTest(),
    );
    return '$dbPath/$dbName';
  }

  /// Checks whether the database file exists on disk (or in OPFS on web).
  Future<bool> databaseExists() async {
    final fileName = await getAppDatabasePath(dbName: dbName);
    return await _platform.databaseExists(fileName);
  }

  /// Attaches a database from bytes and optionally opens it.
  ///
  /// **Eager input**: the caller passes the full database content as
  /// a single in-memory buffer. For multi-hundred-MB imports prefer
  /// [attachStreamDb], which writes chunks incrementally without
  /// holding the whole file in memory.
  Future<DbasSqlite> attachDb(List<int> bytes, {bool openDb = true}) async {
    if (_instance.containsKey(dbName)) {
      if (_instance[dbName]!.isOpened()) {
        await _instance[dbName]!.closeDb();
      }
      _instance.remove(dbName);
    }

    final fileName = await getAppDatabasePath(dbName: dbName);
    await _platform.attachDb(fileName, bytes);
    final instance = await getInstance(dbName: dbName);
    if (openDb) await instance.openDb();
    return instance;
  }

  /// Attaches a database from a byte stream and optionally opens it.
  ///
  /// **Streaming input**: chunks are written incrementally as they
  /// arrive from [stream]. Native pipes them through
  /// `File.openWrite()`; web sends each chunk via the worker's
  /// chunked-attach protocol with per-chunk ACK backpressure, so the
  /// worker holds at most one chunk at a time. Use this for imports
  /// large enough that the in-memory [attachDb] would be wasteful.
  Future<DbasSqlite> attachStreamDb(Stream<List<int>> stream,
      {bool openDb = true}) async {
    if (_instance.containsKey(dbName)) {
      if (_instance[dbName]!.isOpened()) {
        await _instance[dbName]!.closeDb();
      }
      _instance.remove(dbName);
    }

    final fileName = await getAppDatabasePath(dbName: dbName);
    await _platform.attachStreamDb(fileName, stream);
    final instance = await getInstance(dbName: dbName);
    if (openDb) await instance.openDb();
    return instance;
  }

  /// Copies the current database to a new database with the given
  /// [destDbName]. Streamed chunk-by-chunk.
  Future<void> streamCopyDb(String destDbName) async {
    final src = await getAppDatabasePath(dbName: dbName);
    final dest = await getAppDatabasePath(dbName: destDbName);
    await _platform.streamCopyDb(src, dest);
  }

  /// Returns the raw bytes of the database file.
  ///
  /// **Eager**: the full database content is materialised in Dart
  /// memory before this future completes. For large databases (more
  /// than a few hundred MB) prefer [streamCopyDb] to copy the file
  /// into another OPFS / filesystem location without round-tripping
  /// the bytes through the Dart heap, or feed [attachStreamDb] from
  /// a real source stream when re-importing.
  Future<List<int>> getContent() async {
    final fileName = await getAppDatabasePath(dbName: dbName);
    return await _platform.getContent(fileName);
  }

  /// Deletes the database file (including WAL and SHM journal files).
  Future<void> dropDb() async {
    if (!await databaseExists()) return;
    if (isOpened()) await closeDb();

    final fileName = await getAppDatabasePath(dbName: dbName);
    await _platform.dropDb(fileName);
  }

  /// Opens the database using a connection pool with WAL mode.
  ///
  /// Creates one writer connection and [readerPoolSize] read-only
  /// readers. Falls back to a single connection if pool creation
  /// fails.
  ///
  /// **Idempotent.** Calling `openDb()` on an already-open instance is
  /// a no-op and returns immediately. Calling it with a different
  /// [readerPoolSize] than the original open throws a
  /// [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.openDbReopenWithDifferentPoolSize] — pool
  /// resizing isn't supported; close the database first if you need to
  /// change the pool size.
  Future<void> openDb({int readerPoolSize = 4}) async {
    if (isOpened()) {
      _assertReopenPoolSize(readerPoolSize);
      return;
    }

    // Single-flight: coalesce concurrent opens of this instance onto one
    // in-flight open. The `isOpened()` guard above is NOT sufficient on
    // its own — `_db` is only assigned AFTER the `createPool` await in
    // [_performOpen], so two `openDb()` calls that arrive before the
    // first finishes both observe `_db == null`, both fall through, and
    // each issues its own `createPool` for the same database file. On
    // web the pool layer is process-wide and rejects the second with
    // `POOL_ALREADY_ACTIVE`; on native it would spin up a duplicate C
    // pool against the same file. Awaiting the in-flight open is what
    // upholds the documented idempotency contract under concurrency —
    // it is not a retry that papers over the race.
    final inFlight = _opening;
    if (inFlight != null) {
      await inFlight;
      // The open we waited on may have left the instance open, or it may
      // have failed / been closed again in the meantime. If it left us
      // open, honour this call's pool-size contract and return; otherwise
      // start a fresh open below.
      if (isOpened()) {
        _assertReopenPoolSize(readerPoolSize);
        return;
      }
      return openDb(readerPoolSize: readerPoolSize);
    }

    final open = _performOpen(readerPoolSize);
    _opening = open;
    try {
      await open;
    } finally {
      // Clear the marker only if it still points at this open — a
      // close→open during teardown could have replaced it.
      if (identical(_opening, open)) _opening = null;
    }
  }

  /// Throws [DbasSqliteErrorCode.openDbReopenWithDifferentPoolSize] when
  /// an already-open instance is asked to (re)open with a different
  /// [readerPoolSize] — pool resizing isn't supported.
  void _assertReopenPoolSize(int readerPoolSize) {
    if (readerPoolSize != _readerPoolSize) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.openDbReopenWithDifferentPoolSize,
        'openDb("$dbName") was called with readerPoolSize=$readerPoolSize '
        'but the database is already opened with readerPoolSize=$_readerPoolSize. '
        'Close the database before re-opening with a different pool size.',
      );
    }
  }

  /// Performs the actual open: resolves the file path, creates the
  /// connection pool (or falls back to a single connection), and
  /// publishes the [_db] handle. Always run through [openDb]'s
  /// single-flight guard so at most one open runs per instance at a time.
  Future<void> _performOpen(int readerPoolSize) async {
    final fileName = await getAppDatabasePath(dbName: dbName);

    // Clear the close-latched flag so re-opening this same instance
    // after a [closeDb] doesn't reject every acquire. Reached when a
    // caller retains the instance reference across close→open, and on
    // the [attachDb] / [attachStreamDb] `openDb: true` paths.
    _closing = false;

    if (readerPoolSize > 0) {
      final poolPtr = await _platform.createPool(dbName, fileName, readerPoolSize);
      if (poolPtr != 0) {
        _poolPtr = poolPtr;
        _readerPoolSize = readerPoolSize;
        _readerSlotsAvailable = readerPoolSize;
        final writerPtr = _platform.poolGetWriter(dbName, poolPtr);
        _db = DbasSqliteDb(dbName, writerPtr);
        return;
      }
    }

    _readerPoolSize = 0;
    _readerSlotsAvailable = 0;
    _db = await _platform.openDb(fileName);
  }

  /// Returns `true` if the database connection is currently open.
  bool isOpened() => _db != null && _platform.isOpened(_db!);

  /// Closes the database connection and removes the instance from
  /// the cache. Active readers and statements are closed first.
  /// Active transactions are rolled back.
  ///
  /// If the in-flight `rollback()` itself fails (e.g. the connection
  /// is already in a corrupt state at the SQLite layer), the failure
  /// is logged via `dart:developer` and teardown continues — otherwise
  /// a single rollback failure would skip statement cleanup, queue
  /// cancellation, and pool close, leaving the cache and OS resources
  /// dangling. Code that needs to react to a failed rollback must call
  /// `rollback()` explicitly before `closeDb()`.
  Future<void> closeDb() async {
    // Latch the closing flag and drain queues BEFORE anything else.
    //
    // Order is load-bearing: the held statement's reader `onClose`
    // (run during the sweep below) calls [_releaseReaderSlot], which
    // would normally grant a parked reader-slot waiter. If we
    // granted one here, that waiter would race into [_platform]'s
    // `poolAcquireReaderBlocking` → `prepareQuery` on a worker
    // isolate while this method dispatches `closePool` on a sibling
    // worker isolate — concurrent against the same C `SQLitePool`,
    // with a segfault when `ClosePool` destroys the pool
    // lock/condvar underneath the parked acquire. Draining the queue
    // here rejects every parked waiter with
    // [DbasSqliteErrorCode.readerSlotWaitCancelled] and empties the
    // queue, so the sweep's `onClose` finds an empty queue and
    // harmlessly increments [_readerSlotsAvailable].
    //
    // The [_closing] flag complements that for NEW callers: any
    // [_acquireReaderSlot] / [_acquireWriterLock] arriving while
    // teardown is in flight rejects synchronously with the same code
    // instead of entering the (now-empty) queue.
    _closing = true;
    _cancelWriterWaitQueue();
    _cancelReaderSlotWaitQueue();

    try {
      await rollback();
    } catch (e, st) {
      developer.log(
        'closeDb: rollback of in-flight transaction failed; '
        'continuing teardown',
        name: 'dbas_sqlite.DbasSqlite',
        error: e,
        stackTrace: st,
      );
    }

    // Close every still-open statement (which closes its active reader
    // if any). List.of() snapshots the set since close() mutates it.
    int stmtCloseFailures = 0;
    for (final stmt in List.of(_activeStatements)) {
      try {
        await stmt.close();
      } catch (e, st) {
        stmtCloseFailures++;
        developer.log(
          'closeDb: statement close failed for "$dbName"',
          name: 'dbas_sqlite.DbasSqlite',
          error: e,
          stackTrace: st,
        );
      }
    }
    _activeStatements.clear();

    if (_instance.containsKey(dbName)) {
      _instance.remove(dbName);
    }

    if (_poolPtr != null) {
      // ClosePool force-drains any handles we missed (defensive).
      await _platform.closePool(dbName, _poolPtr!);
      _poolPtr = null;
      _db = null;
    } else if (_db != null) {
      // Single-connection fallback. Tracked statements above should
      // have finalised every handle; if CloseDb returns SQLITE_BUSY
      // it means at least one handle is still live — either tracked
      // statements that failed to finalise (stmtCloseFailures > 0) or
      // a handle leaked outside our tracking. Surface the right one.
      final rc = await _platform.closeDb(_db!, checkpoint: false);
      if (rc == sqliteBusy) {
        final err = _platform.getLastDbError(_db!) ?? 'live handles';
        // Capture both rcs BEFORE nulling _db — the helpers need the
        // live connection to call sqlite3_errcode / sqlite3_extended_errcode.
        // Fall back to the observed `rc` when the helpers return null
        // (the C lib didn't queue an active error on the connection).
        final primaryRc = _platform.getErrorCode(_db!) ?? rc;
        final uniqueRc = _platform.getUniqueErrorCode(_db!);
        _db = null;
        if (stmtCloseFailures > 0) {
          throw DbasSqliteException.sqlite(
            DbasSqliteErrorCode.closeDbBusyWithStmtFinalizeFailures,
            'Cannot close database "$dbName": $err. '
            '$stmtCloseFailures tracked statement(s) failed to finalize '
            '(see prior log entries for the underlying errors).',
            sqliteCode: primaryRc,
            sqliteUniqueCode: uniqueRc,
          );
        }
        throw DbasSqliteException.sqlite(
          DbasSqliteErrorCode.closeDbBusyLeakedHandle,
          'Cannot close database "$dbName": $err. '
          'A statement handle was leaked outside the tracked set; '
          'this is a bug — please report.',
          sqliteCode: primaryRc,
          sqliteUniqueCode: uniqueRc,
        );
      }
      _db = null;
    }
  }

  // ── New: prepare / utilities ─────────────────────────────────────────

  /// Prepares a SQL statement. Returns a [DbasSqliteStatement] that
  /// owns parameter binding and execution.
  ///
  /// The statement holds the SQL until executed; the underlying
  /// native handle is allocated lazily at execute time on the
  /// connection appropriate for the execution mode (writer for
  /// `executeSql`, pool reader for `executeReader` outside
  /// transactions, writer inside transactions).
  ///
  /// Multiple statements may be prepared on the same `DbasSqlite`
  /// without blocking each other. Caller MUST call
  /// [DbasSqliteStatement.close] when done; closing the database
  /// auto-closes any still-open statements as a safety net.
  Future<DbasSqliteStatement> prepareQuery(String sql) async {
    if (!isOpened()) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.prepareQueryDatabaseNotOpened,
        'Database is not opened. Please open the database before preparing.',
      );
    }
    final stmt = DbasSqliteStatement.internal(this, _platform, sql);
    _activeStatements.add(stmt);
    return stmt;
  }

  /// Returns the runtime SQLite version (e.g. `"3.52.0"`).
  ///
  /// On native: cached during platform initialization (one FFI call
  /// during `getInstance`); subsequent calls return synchronously.
  ///
  /// On web: populated from `SELECT sqlite_version()` against the JS
  /// pool the first time `createPool` runs. Returns `''` until the
  /// pool exists (i.e. before `openDb`).
  String getSqliteVersion() => _platform.getSqliteVersion(dbName);

  /// Cumulative row-change counter for this connection
  /// (`sqlite3_total_changes64`). Returns `-1` if the database is not
  /// opened.
  ///
  /// **Web platform:** always returns `0` — the JS pool does not
  /// expose `sqlite3_total_changes`. Do not use this as a
  /// cache-invalidation or audit signal in code that runs on web.
  int getTotalChanges() {
    if (_db == null) return -1;
    return _platform.getTotalChanges(_db!);
  }

  /// File name used to open the connection. `null` if the database is
  /// not opened. The C string is copied into a Dart [String]
  /// immediately, so the value outlives the C buffer — safe to keep
  /// across `closeDb()`.
  String? getDbFileName() {
    if (_db == null) return null;
    return _platform.getDbFileName(_db!);
  }

  /// Override the SQLite busy-timeout (ms) on the writer and every
  /// pool reader.
  ///
  /// **Native:** holds each reader slot in turn for
  /// [kSetBusyTimeoutAcquireMs] before reconfiguring; throws a
  /// [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.setBusyTimeoutReaderBusy] if any one slot is
  /// still busy after the timeout. Other failure codes from this method:
  /// [DbasSqliteErrorCode.setBusyTimeoutDatabaseNotOpened],
  /// [DbasSqliteErrorCode.setBusyTimeoutWriterFailed],
  /// [DbasSqliteErrorCode.setBusyTimeoutReaderFailed].
  /// Recommended: call at openDb time before any reads, or inside a
  /// `db.transaction(...)` block where readers are quiescent.
  ///
  /// **Web:** silent no-op. The JS pool has its own busy-handling
  /// model and does not expose a per-connection `busy_timeout`
  /// accessor. Apps relying on a specific busy-timeout value on web
  /// must not depend on this call to apply it.
  Future<void> setBusyTimeout(int ms) async {
    if (_db == null) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.setBusyTimeoutDatabaseNotOpened,
        'Database is not opened.',
      );
    }
    if (kIsWeb) {
      // The JS pool does not expose a per-connection busy_timeout
      // accessor; the writer worker manages its own busy handling.
      // Returning silently here is consistent with the JS pool's
      // model — there is nothing to do.
      return;
    }
    final rc = await _platform.setBusyTimeout(_db!, ms);
    if (rc != sqliteOk) {
      final err = _platform.getLastDbError(_db!) ?? 'rc=$rc';
      final primary = _platform.getErrorCode(_db!) ?? rc;
      throw DbasSqliteException.sqlite(
        DbasSqliteErrorCode.setBusyTimeoutWriterFailed,
        'setBusyTimeout failed on writer: $err',
        sqliteCode: primary,
        sqliteUniqueCode: _platform.getUniqueErrorCode(_db!),
      );
    }

    if (_poolPtr == null || _readerPoolSize == 0) return;

    // Hold all reader slots exclusively, then reconfigure each one
    // exactly once. Acquiring all up front prevents the same slot
    // from being reconfigured multiple times in a release-then-
    // re-acquire cycle. Release happens in the `finally` so a mid-
    // loop failure doesn't leak slots.
    final acquireMs =
        debugSetBusyTimeoutAcquireMs ?? kSetBusyTimeoutAcquireMs;
    final delegate = _platform.delegate(dbName);
    final acquired = <int>[];
    try {
      for (int i = 0; i < _readerPoolSize; i++) {
        final acquire =
            await delegate.poolAcquireReaderBlocking(_poolPtr!, acquireMs);
        if (acquire.readerPtr == 0) {
          if (acquire.status == PoolAcquireStatus.closing) {
            throw DbasSqliteException.dart(
              DbasSqliteErrorCode.readerSlotWaitCancelled,
              'setBusyTimeout was cancelled: the database is closing.',
            );
          }
          throw DbasSqliteException.dart(
            DbasSqliteErrorCode.setBusyTimeoutReaderBusy,
            'setBusyTimeout: pool reader $i was busy for '
            '${acquireMs}ms — close in-flight readers first.',
          );
        }
        acquired.add(acquire.readerPtr);
      }
      for (final readerPtr in acquired) {
        final readerDb = DbasSqliteDb(dbName, readerPtr);
        final rrc = await _platform.setBusyTimeout(readerDb, ms);
        if (rrc != sqliteOk) {
          final primary = _platform.getErrorCode(readerDb) ?? rrc;
          throw DbasSqliteException.sqlite(
            DbasSqliteErrorCode.setBusyTimeoutReaderFailed,
            'setBusyTimeout failed on a pool reader: rc=$rrc',
            sqliteCode: primary,
            sqliteUniqueCode: _platform.getUniqueErrorCode(readerDb),
          );
        }
      }
    } finally {
      for (final readerPtr in acquired) {
        delegate.poolReleaseReader(_poolPtr!, readerPtr);
      }
    }
  }

  /// Switches the writer to WAL journal mode and verifies the readback.
  ///
  /// **Native:** dispatches to the C lib's `EnableWal` (idempotent on
  /// a pool that's already in WAL).
  ///
  /// **Web:** runs `PRAGMA journal_mode` and verifies the result is
  /// `wal`. The JS pool always opens with WAL via the writer worker;
  /// this serves as a defensive check that pool initialization
  /// actually succeeded.
  ///
  /// Throws an exception when WAL cannot be activated (read-only
  /// media, unsupported VFS, or — on web — pool init silently
  /// failing to set WAL).
  Future<void> enableWal() async {
    if (_db == null) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.enableWalDatabaseNotOpened,
        'Database is not opened.',
      );
    }
    final rc = await _platform.enableWal(_db!);
    if (rc != sqliteOk) {
      final err = _platform.getLastDbError(_db!) ?? 'rc=$rc';
      final primary = _platform.getErrorCode(_db!) ?? rc;
      throw DbasSqliteException.sqlite(
        DbasSqliteErrorCode.enableWalFailed,
        'enableWal failed: $err',
        sqliteCode: primary,
        sqliteUniqueCode: _platform.getUniqueErrorCode(_db!),
      );
    }
  }

  // ── Transactions ─────────────────────────────────────────────────────

  /// Returns `true` if a transaction is currently active.
  bool get isInTransaction => _isInTransaction;

  /// `true` if the most recent [beginTransaction] call on this instance
  /// issued a real `BEGIN TRANSACTION` (that call started the
  /// transaction); `false` if it took the idempotent no-op join path
  /// because a transaction was already active.
  ///
  /// Only meaningful immediately after `await`ing [beginTransaction] —
  /// read it before any other `await`, so no other caller's
  /// [beginTransaction] / [commit] / [rollback] can run first and change
  /// it underneath you. Dart's cooperative scheduling guarantees that
  /// much: nothing else runs between your `await beginTransaction()`
  /// resuming and your next synchronous statement. While a
  /// `strict: true` call is still parked waiting for the writer lock,
  /// this getter still reflects the *previous* completed call.
  ///
  /// A [beginTransaction] call made with `strict: true` always leaves
  /// this `true` — strict mode never joins.
  ///
  /// Use it to decide whether YOUR code owns ending the transaction:
  /// `if (db.startedCurrentTransaction) await db.commit();` skips the
  /// call entirely when you merely joined a caller-controlled
  /// transaction, avoiding the "a joiner's commit() ends the WHOLE
  /// transaction" hazard described in [commit]'s docs (there is no
  /// reference counting).
  ///
  /// Reset to `false` whenever the transaction ends ([commit] /
  /// [rollback], by any caller) so a stale `true` can't outlive the
  /// transaction it described.
  bool get startedCurrentTransaction => _startedCurrentTransaction;

  /// Begins a new database transaction.
  ///
  /// **Idempotent** by default (`strict: false`): if a transaction is
  /// already active this call does nothing and returns immediately — it
  /// does NOT take a second hold on the writer lock, and
  /// [startedCurrentTransaction] is set to `false` so the caller can
  /// tell it joined rather than started one. [DbasSqlite] tracks at most
  /// one active transaction with no reference counting, so a single
  /// [commit] / [rollback] call — made by ANY caller sharing this
  /// instance, not necessarily the one whose [beginTransaction] issued
  /// `BEGIN TRANSACTION` — ends the transaction for everyone. That is
  /// unchanged from all prior releases.
  ///
  /// Pass `strict: true` to opt out of the idempotent join. A strict
  /// call NEVER joins an already-active transaction: it parks on the
  /// writer-lock FIFO queue (the same queue a plain `executeSql()`
  /// outside a transaction waits on) until that transaction's [commit] /
  /// [rollback] releases the lock, and only then issues its own `BEGIN
  /// TRANSACTION` — so it always leaves [startedCurrentTransaction] as
  /// `true`. Uncontended, `strict: true` and `strict: false` behave
  /// identically.
  ///
  /// **⚠️ `strict: true` from the flow that already owns the
  /// transaction is a self-deadlock.** Strict mode does not (and cannot)
  /// know that the caller waiting for the writer lock is the same flow
  /// that holds it: it parks, and the only thing that would wake it is a
  /// [commit] / [rollback] that flow can no longer reach because it is
  /// parked. The wait is therefore **bounded** — after
  /// [kWriterLockWaitTimeoutMs] the call gives up, removes itself from
  /// the queue, and throws
  /// [DbasSqliteErrorCode.writerLockWaitTimeout], leaving the
  /// transaction it could not join completely untouched. Treat that
  /// error as a bug in the calling code, not a transient: retrying it
  /// will time out again. Use `strict: true` only where the flow is
  /// genuinely independent of any transaction it might contend with, and
  /// use [startedCurrentTransaction] (not a second `beginTransaction`)
  /// to find out whether you are inside someone else's.
  ///
  /// Throws [DbasSqliteErrorCode.beginTransactionDatabaseNotOpened] if
  /// the database isn't opened,
  /// [DbasSqliteErrorCode.writerLockWaitTimeout] if the writer-lock wait
  /// exceeds [kWriterLockWaitTimeoutMs],
  /// [DbasSqliteErrorCode.writerLockWaitCancelled] if the database is
  /// closed while this call is waiting for that lock, or
  /// [DbasSqliteErrorCode.beginTransactionDatabaseClosedWaitingLock] if
  /// it was closed after the lock was granted. A failed `BEGIN
  /// TRANSACTION` throws
  /// [DbasSqliteErrorCode.beginTransactionFailed].
  Future<void> beginTransaction({bool strict = false}) async {
    if (!isOpened()) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.beginTransactionDatabaseNotOpened,
        'Database is not opened. Please open the database before starting a transaction.',
      );
    }
    if (!strict && _isInTransaction) {
      _startedCurrentTransaction = false;
      return;
    }

    await _acquireWriterLock();
    try {
      if (!isOpened()) {
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.beginTransactionDatabaseClosedWaitingLock,
          'Database was closed while waiting for writer lock.',
        );
      }
      final rc = await _platform.executeSql(_db!, 'BEGIN TRANSACTION');
      if (rc != sqliteOk) {
        final err = _platform.getLastDbError(_db!) ?? 'rc=$rc';
        final primary = _platform.getErrorCode(_db!) ?? rc;
        throw DbasSqliteException.sqlite(
          DbasSqliteErrorCode.beginTransactionFailed,
          'BEGIN TRANSACTION failed: $err',
          sqliteCode: primary,
          sqliteUniqueCode: _platform.getUniqueErrorCode(_db!),
        );
      }
      _transactionHasWrites = false;
      _rotateReentrantWriterOpEpoch();
      _isInTransaction = true;
      _startedCurrentTransaction = true;
    } catch (_) {
      if (!_isInTransaction) _releaseWriterLock();
      rethrow;
    }
  }

  /// Throws if it is unsafe to issue `COMMIT` right now because some
  /// other in-flight use of the writer connection — a reentrant
  /// `executeSql` / `executeReader` dispatch, or a reader opened inside
  /// this transaction and routed to the writer — has not finished.
  ///
  /// Called by [commit] **before** it touches `_isInTransaction` or the
  /// writer lock, so a caller that hits this can fix the issue (await
  /// the write, close the reader) and call `commit()` again without
  /// anything having been disturbed.
  void _assertNoInFlightWriterUsers() {
    if (_reentrantWriterOps.isNotEmpty) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.commitBlockedByInFlightOperation,
        'Cannot commit: ${_reentrantWriterOps.length} operation(s) started '
        'inside this transaction (an executeSql, or the prepare phase of '
        'an executeReader) have not finished yet. Await every write and '
        'read before calling commit() — committing now would end the '
        'transaction and hand the writer lock to the next waiter while '
        'those dispatches are still running on the writer connection.',
      );
    }
    for (final stmt in _activeStatements) {
      if (stmt.hasOpenWriterReaderInternal) {
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.commitBlockedByActiveReader,
          'Cannot commit: a reader opened inside this transaction (routed '
          'to the writer connection for read-your-writes) is still open. '
          'Close it via reader.close() — or exhaust it via readRow() — '
          'before calling commit(). Its cursor lives on the writer '
          'connection: committing now would leave it stepping against a '
          'connection whose transaction has ended and whose writer lock '
          'has already been handed to the next waiter, so the rows it '
          'still returns belong to no transaction and the next writer can '
          'change them mid-iteration.',
        );
      }
    }
  }

  /// Commits the current transaction. No-op if no transaction is active.
  ///
  /// Before issuing `COMMIT`, a pre-flight check verifies the writer
  /// connection is quiescent: no `executeSql()` (or the prepare phase of
  /// an `executeReader()`) started inside this transaction is still in
  /// flight, and no reader opened inside this transaction — routed to
  /// the writer connection for read-your-writes, see
  /// [DbasSqliteStatement.executeReader] — is still open. Skipping the
  /// check would let `COMMIT`, and the writer-lock release that follows
  /// it, race that still-running work: [beginTransaction] is
  /// **idempotent**, so a caller that joined an already-active
  /// transaction never acquired the writer lock itself; if THAT caller's
  /// `commit()` ran while the real owner still had a write or a reader
  /// in flight, the owner's dispatch would keep using the writer
  /// connection after this method already committed and handed the lock
  /// to whoever is next in the FIFO queue. Violating the check throws
  /// immediately, **before** `_isInTransaction` or the writer lock is
  /// touched, with:
  ///   - [DbasSqliteErrorCode.commitBlockedByInFlightOperation]
  ///   - [DbasSqliteErrorCode.commitBlockedByActiveReader] — readers on
  ///     a pool connection (opened outside any transaction, or inside
  ///     one before its first write) are never affected; a WAL pool read
  ///     doesn't touch the writer connection at all.
  ///
  /// Neither is a transient: both mean "you called `commit()` at the
  /// wrong time". Await the write / close the reader, then commit again.
  ///
  /// If `COMMIT` fails, the implicit recovery is to [rollback]. When
  /// ONLY the rollback recovery succeeds the ORIGINAL
  /// [DbasSqliteErrorCode.commitFailed] is rethrown unchanged. When BOTH
  /// `COMMIT` and the subsequent [rollback] fail, a
  /// [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.commitRollbackAlsoFailed] is thrown instead —
  /// the original `COMMIT` failure is preserved on
  /// [DbasSqliteException.cause] with its stack trace on
  /// [DbasSqliteException.causeStackTrace]; the rollback failure is
  /// logged via `dart:developer` (its stack would otherwise be lost in
  /// the wrapper). When the original error is itself a
  /// [DbasSqliteException] its [DbasSqliteException.sqliteCode] and
  /// [DbasSqliteException.sqliteUniqueCode] are lifted onto the outer
  /// exception. This mirrors [transaction]'s handling of the identical
  /// shape; `commit()` gets its own code (not
  /// [DbasSqliteErrorCode.transactionRollbackAlsoFailed]) so callers can
  /// tell a bare `commit()` apart from one made through [transaction].
  ///
  /// Throws [DbasSqliteErrorCode.commitDatabaseNotOpened] if the
  /// database was closed while a transaction was still marked active —
  /// should not normally happen; mirrors the defensive guard already in
  /// [beginTransaction] / [vacuum].
  Future<void> commit() async {
    if (!_isInTransaction) return;
    if (!isOpened()) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.commitDatabaseNotOpened,
        'Database is not opened. The connection was closed while a '
        'transaction was still marked active — this indicates a bug; '
        'please report it.',
      );
    }
    // Both guards sit OUTSIDE the try on purpose: a pre-flight failure
    // must not trigger the auto-rollback recovery below. It means "you
    // called commit() at the wrong time", not "the database operation
    // failed", so the transaction is left completely untouched and the
    // caller can retry once the blocker is gone.
    _assertNoInFlightWriterUsers();
    try {
      final rc = await _platform.executeSql(_db!, 'COMMIT');
      if (rc != sqliteOk) {
        final err = _platform.getLastDbError(_db!) ?? 'rc=$rc';
        final primary = _platform.getErrorCode(_db!) ?? rc;
        throw DbasSqliteException.sqlite(
          DbasSqliteErrorCode.commitFailed,
          'COMMIT failed: $err',
          sqliteCode: primary,
          sqliteUniqueCode: _platform.getUniqueErrorCode(_db!),
        );
      }
      _isInTransaction = false;
      _transactionHasWrites = false;
      _startedCurrentTransaction = false;
      _rotateReentrantWriterOpEpoch();
      _releaseWriterLock();
    } catch (originalError, originalStack) {
      try {
        await rollback();
      } catch (rollbackError, rollbackStack) {
        // The rollback failure would otherwise be swallowed, leaving the
        // caller unable to tell "recovered" from "state unknown". Log it
        // with its own stack — only the wrapper's cause/stack survive
        // below — and surface the distinct code.
        developer.log(
          'commit: rollback failed after COMMIT failure; '
          'wrapping into commitRollbackAlsoFailed',
          name: 'dbas_sqlite.DbasSqlite',
          error: rollbackError,
          stackTrace: rollbackStack,
        );
        // Lift the most specific rc/extended-rc pair we can find. Prefer
        // originalError's codes since it's the proximate cause; fall
        // back to the rollback failure's codes.
        int? liftedPrimary;
        int? liftedUnique;
        if (originalError is DbasSqliteException) {
          liftedPrimary = originalError.sqliteCode;
          liftedUnique = originalError.sqliteUniqueCode;
        } else if (rollbackError is DbasSqliteException) {
          liftedPrimary = rollbackError.sqliteCode;
          liftedUnique = rollbackError.sqliteUniqueCode;
        }
        final msg = 'COMMIT failed: $originalError. '
            'Additionally, rollback also failed: $rollbackError. '
            'The database may be in an inconsistent state.';
        final ex = liftedPrimary != null
            ? DbasSqliteException.sqlite(
                DbasSqliteErrorCode.commitRollbackAlsoFailed,
                msg,
                sqliteCode: liftedPrimary,
                sqliteUniqueCode: liftedUnique,
                cause: originalError,
                causeStackTrace: originalStack,
              )
            : DbasSqliteException.dart(
                DbasSqliteErrorCode.commitRollbackAlsoFailed,
                msg,
                cause: originalError,
                causeStackTrace: originalStack,
              );
        Error.throwWithStackTrace(ex, originalStack);
      }
      rethrow;
    }
  }

  /// Rolls back the current transaction. No-op if no transaction is active.
  ///
  /// If the underlying ROLLBACK fails (corrupt connection, lock loss),
  /// the Dart-side transaction flag is still cleared and the writer
  /// lock is released — but the failure is rethrown wrapped in a
  /// [DbasSqliteException] with code [DbasSqliteErrorCode.rollbackFailed]
  /// so the caller knows the C connection's autocommit state may be
  /// inconsistent. When the underlying cause is itself a
  /// [DbasSqliteException] its [DbasSqliteException.sqliteCode] AND
  /// [DbasSqliteException.sqliteUniqueCode] are lifted onto the outer
  /// exception; the original error is attached as
  /// [DbasSqliteException.cause] (with its stack trace on
  /// [DbasSqliteException.causeStackTrace]) for programmatic
  /// inspection.
  ///
  /// **In-flight writes are drained, not rejected.** Unlike [commit],
  /// which refuses to run while the writer connection is busy,
  /// `rollback()` first **waits** for every write dispatched inside this
  /// transaction to finish, then issues `ROLLBACK`. Waiting rather than
  /// throwing is deliberate: `rollback()` is the best-effort cleanup
  /// path used by [closeDb] and by error recovery throughout this class,
  /// so a new way for it to fail would be a regression, not a safety
  /// improvement.
  ///
  /// The drain is **load-bearing, not defensive**. `executeSql` replays
  /// its bind buffer one bind at a time and every bind is its own
  /// dispatch round-trip, so a write dispatched un-awaited inside an
  /// open transaction is still walking a chain of pending dispatches
  /// when `rollback()` runs. Without the drain the `ROLLBACK` slips
  /// between two of that write's dispatches and its step then executes
  /// on a connection that is back in autocommit mode: the row commits
  /// **on its own** and survives the rollback, with no error raised on
  /// either side. Measured against this library the race reproduced on
  /// every attempt, from two binds upwards — it is inherent to the
  /// dispatch model, not a wide-statement edge case.
  ///
  /// Readers are deliberately NOT drained or rejected — a `SELECT`
  /// cannot persist anything past a `ROLLBACK`, so a live cursor only
  /// ever observes data the rollback is about to undo (or, once it
  /// lands, data belonging to no transaction). SQLite also tolerates a
  /// `ROLLBACK` with live statements on the connection, unlike `COMMIT`.
  Future<void> rollback() async {
    if (!_isInTransaction) return;
    await _drainReentrantWriterOps();
    // The drain suspends, so re-check: a concurrent commit()/rollback()
    // on this shared instance may have ended the transaction while we
    // waited, and issuing ROLLBACK outside a transaction would fail.
    if (!_isInTransaction) return;
    Object? rollbackCause;
    StackTrace? rollbackCauseStack;
    int? rollbackPrimaryRc;
    int? rollbackUniqueRc;
    String rollbackDetail = '';
    try {
      final rc = await _platform.executeSql(_db!, 'ROLLBACK');
      if (rc != sqliteOk) {
        final err = _platform.getLastDbError(_db!) ?? 'rc=$rc';
        rollbackPrimaryRc = _platform.getErrorCode(_db!) ?? rc;
        rollbackUniqueRc = _platform.getUniqueErrorCode(_db!);
        rollbackDetail = 'ROLLBACK rc=$rc: $err';
        rollbackCauseStack = StackTrace.current;
      }
    } catch (e, st) {
      rollbackCause = e;
      rollbackCauseStack = st;
      rollbackDetail = e.toString();
      if (e is DbasSqliteException) {
        rollbackPrimaryRc = e.sqliteCode;
        rollbackUniqueRc = e.sqliteUniqueCode;
      }
    } finally {
      _isInTransaction = false;
      _transactionHasWrites = false;
      _startedCurrentTransaction = false;
      _rotateReentrantWriterOpEpoch();
      _releaseWriterLock();
    }
    if (rollbackCauseStack != null) {
      final msg =
          'ROLLBACK failed; database may still be in a transaction: $rollbackDetail';
      final ex = rollbackPrimaryRc != null
          ? DbasSqliteException.sqlite(
              DbasSqliteErrorCode.rollbackFailed,
              msg,
              sqliteCode: rollbackPrimaryRc,
              sqliteUniqueCode: rollbackUniqueRc,
              cause: rollbackCause,
              causeStackTrace: rollbackCauseStack,
            )
          : DbasSqliteException.dart(
              DbasSqliteErrorCode.rollbackFailed,
              msg,
              cause: rollbackCause,
              causeStackTrace: rollbackCauseStack,
            );
      Error.throwWithStackTrace(ex, rollbackCauseStack);
    }
  }

  /// Executes [action] within a database transaction with automatic
  /// commit and rollback. If [action] throws, the transaction is
  /// rolled back and the exception is rethrown.
  ///
  /// When BOTH [action] (or `commit`) and the subsequent `rollback`
  /// fail, a [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.transactionRollbackAlsoFailed] is thrown.
  /// The original error is preserved on
  /// [DbasSqliteException.cause] with its stack trace on
  /// [DbasSqliteException.causeStackTrace]; the rollback failure is
  /// logged via `dart:developer` (its stack would otherwise be lost in
  /// the wrapper). When the original error is itself a
  /// [DbasSqliteException], its [DbasSqliteException.sqliteCode] and
  /// [DbasSqliteException.sqliteUniqueCode] are lifted onto the
  /// outer exception. Callers branching on subCategory should also
  /// inspect `cause` — the outer exception's category is
  /// [DbasSqliteErrorCategory.transactionFailed] regardless of what
  /// the proximate failure was, so a UNIQUE-violation-then-rollback-
  /// failure surfaces as `transactionRollbackAlsoFailed` outwardly
  /// while `cause` holds the original `executeSqlStepFailed` with
  /// `duplicatedData`.
  Future<void> transaction(Future<void> Function(DbasSqlite db) action) async {
    if (_isInTransaction) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.transactionAlreadyActive,
        'A transaction is already active. Cannot nest transactions.',
      );
    }
    await beginTransaction();
    try {
      await action(this);
      await commit();
    } catch (originalError, originalStack) {
      try {
        await rollback();
      } catch (rollbackError, rollbackStack) {
        developer.log(
          'transaction: rollback failed after action/commit failure; '
          'wrapping into transactionRollbackAlsoFailed',
          name: 'dbas_sqlite.DbasSqlite',
          error: rollbackError,
          stackTrace: rollbackStack,
        );
        // Lift the most specific rc/extended-rc pair we can find.
        // Prefer originalError's codes since it's the proximate cause;
        // fall back to the rollback failure's codes.
        int? liftedPrimary;
        int? liftedUnique;
        if (originalError is DbasSqliteException) {
          liftedPrimary = originalError.sqliteCode;
          liftedUnique = originalError.sqliteUniqueCode;
        } else if (rollbackError is DbasSqliteException) {
          liftedPrimary = rollbackError.sqliteCode;
          liftedUnique = rollbackError.sqliteUniqueCode;
        }
        final msg = 'Transaction failed: $originalError. '
            'Additionally, rollback also failed: $rollbackError. '
            'The database may be in an inconsistent state.';
        final ex = liftedPrimary != null
            ? DbasSqliteException.sqlite(
                DbasSqliteErrorCode.transactionRollbackAlsoFailed,
                msg,
                sqliteCode: liftedPrimary,
                sqliteUniqueCode: liftedUnique,
                cause: originalError,
                causeStackTrace: originalStack,
              )
            : DbasSqliteException.dart(
                DbasSqliteErrorCode.transactionRollbackAlsoFailed,
                msg,
                cause: originalError,
                causeStackTrace: originalStack,
              );
        Error.throwWithStackTrace(ex, originalStack);
      }
      rethrow;
    }
  }

  /// Rebuilds the database file via VACUUM. Cannot run inside a
  /// transaction.
  Future<void> vacuum() async {
    if (!isOpened()) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.vacuumDatabaseNotOpened,
        'Database is not opened.',
      );
    }
    if (_isInTransaction) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.vacuumInsideTransaction,
        'Cannot run VACUUM inside a transaction.',
      );
    }
    await _acquireWriterLock();
    try {
      if (!isOpened()) {
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.vacuumDatabaseClosedWaitingLock,
          'Database was closed while waiting for writer lock.',
        );
      }
      final rc = await _platform.executeSql(_db!, 'VACUUM');
      if (rc != sqliteOk) {
        final err = _platform.getLastDbError(_db!) ?? 'rc=$rc';
        final primary = _platform.getErrorCode(_db!) ?? rc;
        throw DbasSqliteException.sqlite(
          DbasSqliteErrorCode.vacuumFailed,
          'VACUUM failed: $err',
          sqliteCode: primary,
          sqliteUniqueCode: _platform.getUniqueErrorCode(_db!),
        );
      }
    } finally {
      _releaseWriterLock();
    }
  }

  // ── Async writer lock (FIFO) ─────────────────────────────────────────

  /// Waits for the writer lock, FIFO. The wait is bounded by
  /// [kWriterLockWaitTimeoutMs] (overridable in tests via
  /// [debugWriterLockWaitTimeoutMs]) and throws
  /// [DbasSqliteErrorCode.writerLockWaitTimeout] when the deadline
  /// passes, mirroring [_acquireReaderSlot]'s
  /// [DbasSqliteErrorCode.readerSlotWaitTimeout].
  ///
  /// An unbounded wait here is not "safe by default": the caller most
  /// likely to be starved is the flow that already owns the lock —
  /// `beginTransaction(strict: true)` called from inside its own
  /// transaction — and nothing can ever wake it. The bound turns a
  /// silently wedged flow into a diagnosable error.
  ///
  /// Removing the waiter from the queue BEFORE completing it with the
  /// timeout error is load-bearing: [_releaseWriterLock] grants the lock
  /// to whatever it finds at the head of the queue, so a timed-out
  /// waiter left in place would be handed a lock nobody will ever
  /// release.
  Future<void> _acquireWriterLock() async {
    if (_closing) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.writerLockWaitCancelled,
        'Database is closing; writer lock acquire rejected.',
      );
    }
    if (!_writerLockHeld) {
      _writerLockHeld = true;
      return;
    }
    final waiter = Completer<void>();
    _writerWaitQueue.add(waiter);
    final timeoutMs = debugWriterLockWaitTimeoutMs ?? kWriterLockWaitTimeoutMs;
    Timer? timer;
    if (timeoutMs > 0) {
      timer = Timer(Duration(milliseconds: timeoutMs), () {
        if (waiter.isCompleted) return;
        _writerWaitQueue.remove(waiter);
        waiter.completeError(DbasSqliteException.dart(
          DbasSqliteErrorCode.writerLockWaitTimeout,
          'Writer-lock wait timed out after ${timeoutMs}ms — the lock is '
          'held by another transaction or write that never released it. '
          'If this was beginTransaction(strict: true), check whether the '
          'calling flow already owns the transaction: strict mode never '
          'joins, so it would be waiting on itself.',
        ));
      });
    }
    try {
      await waiter.future;
    } finally {
      timer?.cancel();
    }
  }

  void _releaseWriterLock() {
    if (_writerWaitQueue.isNotEmpty) {
      _writerWaitQueue.removeFirst().complete();
    } else {
      _writerLockHeld = false;
    }
  }

  /// Number of callers currently parked in [_acquireWriterLock] waiting
  /// for the writer lock. Test-only seam so a test can pump the event
  /// loop until the expected number of waiters have registered — e.g. to
  /// prove a `beginTransaction(strict: true)` call really parked instead
  /// of silently joining — instead of guessing with a fixed
  /// `Future.delayed`. Mirrors [debugReaderSlotWaitQueueLength].
  @visibleForTesting
  int get debugWriterLockWaitQueueLength => _writerWaitQueue.length;

  void _cancelWriterWaitQueue() {
    while (_writerWaitQueue.isNotEmpty) {
      _writerWaitQueue.removeFirst().completeError(
        DbasSqliteException.dart(
          DbasSqliteErrorCode.writerLockWaitCancelled,
          'Database was closed while waiting for writer lock.',
        ),
      );
    }
    _writerLockHeld = false;
  }

  // ── Async reader-slot semaphore (FIFO) ───────────────────────────────

  /// Waits up to [timeoutMs] for a reader-slot to become available.
  /// Slots are released by [_releaseReaderSlot]. The release order is
  /// load-bearing: the C reader is returned to the pool BEFORE the
  /// Dart slot is released, so when the next caller's await resumes
  /// the C-side acquire is guaranteed to find a free reader.
  Future<void> _acquireReaderSlot(int timeoutMs) async {
    if (_closing) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.readerSlotWaitCancelled,
        'Database is closing; reader-slot acquire rejected.',
      );
    }
    if (_readerSlotsAvailable > 0) {
      _readerSlotsAvailable--;
      return;
    }
    final waiter = Completer<void>();
    _readerSlotWaitQueue.add(waiter);
    Timer? timer;
    if (timeoutMs > 0) {
      timer = Timer(Duration(milliseconds: timeoutMs), () {
        if (waiter.isCompleted) return;
        _readerSlotWaitQueue.remove(waiter);
        waiter.completeError(DbasSqliteException.dart(
          DbasSqliteErrorCode.readerSlotWaitTimeout,
          'Dart-side reader-slot wait timed out after ${timeoutMs}ms — '
          'all pool readers are busy. Close in-flight readers or raise '
          'DbasSqlite.kPoolAcquireTimeoutMs.',
        ));
      });
    }
    try {
      await waiter.future;
    } finally {
      timer?.cancel();
    }
  }

  void _releaseReaderSlot() {
    if (_readerSlotWaitQueue.isNotEmpty) {
      _readerSlotWaitQueue.removeFirst().complete();
    } else {
      _readerSlotsAvailable++;
    }
  }

  /// Number of callers currently parked in [_acquireReaderSlot] waiting
  /// for a pool reader-slot. Test-only seam so a test can pump the event
  /// loop until the expected number of waiters have registered, instead
  /// of guessing with a fixed `Future.delayed`.
  @visibleForTesting
  int get debugReaderSlotWaitQueueLength => _readerSlotWaitQueue.length;

  void _cancelReaderSlotWaitQueue() {
    while (_readerSlotWaitQueue.isNotEmpty) {
      _readerSlotWaitQueue.removeFirst().completeError(
        DbasSqliteException.dart(
          DbasSqliteErrorCode.readerSlotWaitCancelled,
          'Database was closed while waiting for reader slot.',
        ),
      );
    }
    _readerSlotsAvailable = 0;
  }

  // ── Internal hooks for DbasSqliteStatement ───────────────────────────
  // These have visible names but are only intended for the
  // statement implementation.

  Future<void> acquireWriterLockInternal() => _acquireWriterLock();
  void releaseWriterLockInternal() => _releaseWriterLock();
  DbasSqliteDb? get dbInternal => _db;
  int? get poolPtrInternal => _poolPtr;
  bool get transactionHasWritesInternal => _transactionHasWrites;
  void markTransactionWriteInternal() {
    if (_isInTransaction) _transactionHasWrites = true;
  }
  int get poolAcquireTimeoutMsInternal =>
      debugPoolAcquireTimeoutMs ?? kPoolAcquireTimeoutMs;
  void unregisterStatementInternal(DbasSqliteStatement stmt) =>
      _activeStatements.remove(stmt);

  /// Registers an `executeSql` / `executeReader` dispatch that is using
  /// the writer connection **reentrantly** — it observed
  /// [isInTransaction] as `true` and deliberately did not acquire the
  /// writer lock, because [beginTransaction] already holds it for the
  /// transaction's whole lifetime.
  ///
  /// While at least one operation is registered, [commit] refuses to run
  /// and throws
  /// [DbasSqliteErrorCode.commitBlockedByInFlightOperation] — see its
  /// docs for the hazard. Pair every call with exactly one
  /// [endReentrantWriterOpInternal] passing the returned token; a write
  /// additionally hands its dispatch future to
  /// [trackReentrantWriterOpDispatchInternal], which is what lets
  /// [rollback] drain it.
  ReentrantWriterOpToken beginReentrantWriterOpInternal() {
    final id = ++_reentrantWriterOpSeq;
    _reentrantWriterOps[id] = _ReentrantWriterOp(_transactionGeneration);
    return (id: id, generation: _transactionGeneration);
  }

  /// Un-registers the operation [token] identifies. Unknown or
  /// out-of-epoch tokens are ignored.
  ///
  /// **The epoch match is load-bearing, not defensive padding.** A late
  /// un-registration is genuinely reachable: [rollback] deliberately
  /// does not reject an open reader (see its docs), so it can end a
  /// transaction while an `executeReader` dispatched inside it is still
  /// in flight, and that dispatch's un-registration then lands against
  /// whatever transaction is current by then. Treating it as a plain
  /// "decrement, clamped at zero" would cancel a *live* registration
  /// belonging to the NEXT transaction and let a [commit] straight
  /// through the pre-flight that should have blocked it — silently
  /// re-opening exactly the race this mechanism exists to close.
  /// Matching on the token's id **and** generation is what makes a late
  /// un-registration a true no-op.
  void endReentrantWriterOpInternal(ReentrantWriterOpToken token) {
    final op = _reentrantWriterOps[token.id];
    if (op == null || op.generation != token.generation) return;
    _reentrantWriterOps.remove(token.id);
  }

  /// Attaches the still-pending [dispatch] chain of the operation
  /// [token] identifies, so [rollback] can **drain** it — wait for it to
  /// finish before issuing `ROLLBACK` — instead of racing it.
  ///
  /// Only writes register a dispatch. A read cannot persist anything
  /// past a `ROLLBACK`, so there is nothing for a drain to protect, and
  /// gating rollback on readers is explicitly out of scope (see
  /// [rollback]). An operation with no registered dispatch still blocks
  /// [commit] — it just isn't something [rollback] waits for.
  ///
  /// [dispatch]'s failure is neutralised here: the drain must never
  /// inherit the operation's error, and the operation's real caller is
  /// already awaiting it.
  void trackReentrantWriterOpDispatchInternal(
      ReentrantWriterOpToken token, Future<void> dispatch) {
    final op = _reentrantWriterOps[token.id];
    if (op == null || op.generation != token.generation) return;
    op.dispatch = dispatch
        .then<void>((_) {}, onError: (Object _, StackTrace _) {})
        .whenComplete(() => op.dispatch = null);
  }

  /// Waits until no registered reentrant operation has a live dispatch
  /// chain left. Never throws — every tracked future is neutralised by
  /// [trackReentrantWriterOpDispatchInternal].
  ///
  /// Loops rather than awaiting one snapshot: a caller can dispatch
  /// another un-awaited write while we are parked here, and draining
  /// "everything that was in flight when we started" would leave exactly
  /// the write that arrived last un-drained. Each tracked future clears
  /// itself from its operation once it settles, so the loop makes strict
  /// progress and ends as soon as the writer connection is quiescent.
  Future<void> _drainReentrantWriterOps() async {
    while (true) {
      final pending = <Future<void>>[
        for (final op in _reentrantWriterOps.values)
          if (op.dispatch != null) op.dispatch!,
      ];
      if (pending.isEmpty) return;
      await Future.wait(pending);
    }
  }

  /// Forgets every still-registered reentrant operation and moves the
  /// generation forward, so a token held by an operation that outlived
  /// its transaction can no longer match a registration made by the next
  /// one. Called wherever the set of operations that may legitimately
  /// block a [commit] resets: [beginTransaction], [commit], [rollback].
  void _rotateReentrantWriterOpEpoch() {
    _reentrantWriterOps.clear();
    _transactionGeneration++;
  }

  /// Acquires a pool-reader connection, gated by the Dart-level
  /// reader-slot semaphore so at most [_readerPoolSize] concurrent
  /// blocking acquires can be in flight against the C pool. Returns
  /// the reader pointer on success, or `0` if the C-side acquire
  /// timed out.
  ///
  /// Throws a [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.readerSlotWaitTimeout] when the Dart-side
  /// wait exceeds [timeoutMs] (i.e. no slot freed in time), code
  /// [DbasSqliteErrorCode.readerSlotWaitCancelled] if the database is
  /// closed while waiting, or code
  /// [DbasSqliteErrorCode.acquireReaderConnectionNoPool] when called
  /// against a single-connection (non-pool) database.
  Future<int> acquireReaderConnectionInternal(int timeoutMs) async {
    if (_poolPtr == null) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.acquireReaderConnectionNoPool,
        'No reader pool — single-connection mode does not use this path.',
      );
    }
    final stopwatch = Stopwatch()..start();
    await _acquireReaderSlot(timeoutMs);
    try {
      // With the semaphore granting at most [_readerPoolSize] slots,
      // the C pool always has a reader available here. The remaining
      // budget is a safety net in case some other caller (e.g.
      // setBusyTimeout, which intentionally bypasses the semaphore)
      // is holding readers concurrently.
      //
      // `timeoutMs <= 0` is the documented "non-blocking" form on the
      // C side; passing it through unmodified preserves that
      // semantic. Otherwise clamp the elapsed-adjusted budget into
      // [1, timeoutMs] so the C call still gets a tick to make
      // progress even when the Dart wait consumed the full window.
      final int remaining = timeoutMs <= 0
          ? timeoutMs
          : (timeoutMs - stopwatch.elapsedMilliseconds).clamp(1, timeoutMs);
      final acquire = await _platform.poolAcquireReaderBlocking(
          dbName, _poolPtr!, remaining);
      if (acquire.readerPtr != 0) {
        return acquire.readerPtr;
      }
      // No reader. Distinguish the C-reported reasons: a closing pool
      // (or a destroyed one) is terminal — surface it as a cancellation
      // so the caller doesn't misread it as a retryable timeout. The
      // catch below releases the Dart slot on the throw paths.
      switch (acquire.status) {
        case PoolAcquireStatus.closing:
          throw DbasSqliteException.dart(
            DbasSqliteErrorCode.readerSlotWaitCancelled,
            'Pool reader acquire was cancelled: the database is closing.',
          );
        case PoolAcquireStatus.invalid:
          throw DbasSqliteException.dart(
            DbasSqliteErrorCode.acquireReaderConnectionNoPool,
            'Pool reader acquire failed: the pool is no longer valid.',
          );
        case PoolAcquireStatus.ok:
        case PoolAcquireStatus.noSlot:
        case PoolAcquireStatus.timeout:
          // Transient "all readers busy / deadline elapsed" — keep the
          // existing 0-return contract; the caller raises
          // executeReaderPoolAcquireTimeout.
          _releaseReaderSlot();
          return 0;
      }
    } catch (_) {
      _releaseReaderSlot();
      rethrow;
    }
  }

  /// Returns a reader pointer to the C pool and releases the
  /// Dart-level slot. Order matters: C release first so the next
  /// semaphore-granted caller finds the reader already available.
  void releaseReaderConnectionInternal(int readerPtr) {
    if (_poolPtr == null) return;
    _platform.poolReleaseReader(dbName, _poolPtr!, readerPtr);
    _releaseReaderSlot();
  }
}
