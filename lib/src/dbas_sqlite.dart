import 'dart:async';
import 'dart:collection';
import 'dart:developer' as developer;

import 'package:dbas_sqlite/src/dbas_sqlite_checkpoint_result.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_db.dart'
    if (dart.library.js_interop) 'package:dbas_sqlite/src/stub/dbas_sqlite_db_stub.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_platform.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_row_cache.dart';
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
  ///
  /// **Checkpoints the WAL first.** Only the main `.db` file is copied,
  /// and the destination's `-wal` / `-shm` are deleted — that deletion
  /// is load-bearing for correctness, because a stale foreign `-wal`
  /// beside a copied `.db` opens with NO error and silently serves the
  /// OTHER database's rows, passing `integrity_check`. The consequence
  /// is that every frame still sitting in the source WAL would be
  /// dropped from the copy, so a PASSIVE checkpoint runs on the writer
  /// first and the copy is self-contained with no close/reopen dance at
  /// the call site.
  ///
  /// The checkpoint is **reported, not enforced**. When a reader holds a
  /// WAL snapshot, the frames above it cannot be folded by any
  /// checkpoint mode, and waiting would stall for the whole
  /// `busy_timeout` and still fold nothing. Rather than fail the copy or
  /// block on it, the shortfall is logged via `dart:developer`. Callers
  /// that need a provably complete copy should call [checkpoint]
  /// themselves and check [DbasSqliteCheckpointResult.isComplete] —
  /// close in-flight readers, then copy.
  Future<void> streamCopyDb(String destDbName) async {
    // Order is load-bearing: fold the WAL into the main `.db` BEFORE the
    // raw file read below, which never looks at the `-wal`.
    await _checkpointBeforeRawFileAccess('streamCopyDb("$destDbName")',
        takeWriterLock: true);
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
        final writer = DbasSqliteDb(dbName, writerPtr);
        _db = writer;
        // The pooled open is the only path that OPENS the database in
        // `journal_mode=wal`, so the WAL writer policy is established
        // here, before the writer is handed to any caller. [enableWal]
        // is the OTHER door into WAL mode and establishes the same
        // policy itself — see [_pinWalWriterSettings].
        //
        // Failing the policy is fatal to the OPEN specifically: the
        // half-built pool is torn down so no caller can ever be handed a
        // writer whose WAL policy is unknown. That teardown belongs to
        // this caller, NOT to the shared pragma helper — [enableWal]
        // runs on a live, published connection and must not destroy it.
        try {
          await _pinWalWriterSettings(writer, caller: 'openDb');
        } catch (e) {
          await _tearDownFailedPoolOpen(e);
          rethrow;
        }
        return;
      }
    }

    _readerPoolSize = 0;
    _readerSlotsAvailable = 0;
    _db = await _platform.openDb(fileName);
  }

  /// Establishes the **WAL writer policy** on [writer]: the fsync
  /// durability a fold runs under, then the fold itself.
  ///
  /// Called from BOTH doors into WAL mode, so a database carries the
  /// same guarantees whichever one it came through:
  ///   - [_performOpen]'s pooled branch, which OPENS the database in
  ///     `journal_mode=wal`;
  ///   - [enableWal], which moves an **already-open** connection into
  ///     WAL. That covers `openDb(readerPoolSize: 0)`, which opens in
  ///     `journal_mode=delete` (measured) and therefore never runs the
  ///     open-time branch at all — before this was shared, such a
  ///     connection reached WAL carrying the stock
  ///     `wal_autocheckpoint=1000` and an unpinned `synchronous`, which
  ///     is precisely the silent-data-loss configuration the two
  ///     pragmas exist to prevent.
  ///
  /// Order is **load-bearing**: `synchronous` governs the fsync a
  /// checkpoint performs, so it is pinned BEFORE the second pragma turns
  /// every commit into a checkpoint — no fold may run under an unpinned
  /// durability.
  ///
  /// **Idempotent.** Each pragma sets a connection-level value to a
  /// fixed constant, so re-applying them returns `SQLITE_OK` and changes
  /// nothing. That is what keeps a second [enableWal] — and an
  /// [enableWal] on a pooled database that already ran them at open —
  /// a harmless no-op rather than a double-application with a side
  /// effect.
  ///
  /// [caller] names the public method in any thrown message. What a
  /// failure does BEYOND throwing is the caller's decision: see
  /// [_performOpen], which tears the half-built pool down, versus
  /// [enableWal], which leaves the live connection standing.
  Future<void> _pinWalWriterSettings(
    DbasSqliteDb writer, {
    required String caller,
  }) async {
    await _pinWriterSynchronousFull(writer, caller);
    await _enableWriterAutoCheckpoint(writer, caller);
  }

  /// Pins the [writer]'s fsync policy: `PRAGMA synchronous=FULL`.
  ///
  /// **This changes no behaviour.** FULL is already the value — the
  /// prebuilt C library reports `DEFAULT_SYNCHRONOUS=2` and
  /// `DEFAULT_WAL_SYNCHRONOUS=2` in `PRAGMA compile_options`, and a live
  /// readback on the writer measures `synchronous=2`. The pragma is
  /// issued **deliberately explicitly rather than inherited**: until it
  /// was, the setting was load-bearing on an undocumented compile-time
  /// default of a **prebuilt binary**. Nothing in the Dart said so, and
  /// a future rebuild of that C library with different flags would
  /// change this database's durability with no code change and no test
  /// failure. Issuing it costs nothing — it is already the value — and
  /// puts the intent next to [_enableWriterAutoCheckpoint]'s fold
  /// policy, where a reader of this file can actually see it.
  ///
  /// `synchronous` is a **connection-level** setting applied once when
  /// the connection enters WAL, **never per commit**. It governs the
  /// fsync SQLite performs both when a transaction commits and when a
  /// checkpoint folds the WAL back into the main `.db` — which is why it
  /// is pinned BEFORE [_enableWriterAutoCheckpoint] turns every commit
  /// into a checkpoint, so no fold can run under an unpinned durability.
  ///
  /// A failure throws
  /// [DbasSqliteErrorCode.walSynchronousFullFailed] — the same policy
  /// as [_enableWriterAutoCheckpoint], through the shared
  /// [_applyWalWriterPragma]. What happens beyond the throw belongs to
  /// the caller; see [_pinWalWriterSettings].
  ///
  /// **Web:** no-op. `web/libs/dbas_sqlite_worker.js` already issues
  /// `PRAGMA synchronous=FULL` on the writer role during worker init and
  /// fails pool creation with `INIT_FAILED` if it cannot, so the
  /// guarantee is established there — same policy, enforced one layer
  /// down. Mirrors [setBusyTimeout]'s web no-op rationale.
  Future<void> _pinWriterSynchronousFull(DbasSqliteDb writer, String caller) {
    return _applyWalWriterPragma(
      writer,
      caller,
      'PRAGMA synchronous=FULL',
      DbasSqliteErrorCode.walSynchronousFullFailed,
      'The writer would silently fall back to whatever durability the '
      'prebuilt C library happens to be compiled with — an invisible, '
      'unpinned default.',
    );
  }

  /// Configures the [writer] connection so **every** commit folds
  /// the WAL back into the main `.db` file.
  ///
  /// Until this pragma runs, the writer inherits SQLite's stock
  /// `wal_autocheckpoint=1000` and committed frames sit in the `-wal`
  /// indefinitely: measured, 200 committed inserts leave the main `.db`
  /// at 4096 bytes with an 832 KB `-wal` and the table not in the main
  /// file **at all**. Anything that then reads the main file alone — a
  /// file copy, [streamCopyDb], a backup — silently sees a truncated or
  /// entirely empty database, with no error of any kind.
  ///
  /// `=1` checkpoints after every commit, which covers bare
  /// `INSERT`/`UPDATE`/`DELETE` too: each is an implicit transaction
  /// that commits. It is a single statement, so it is unaffected by the
  /// one-statement limit of the `executeSql` prepare path — the limit
  /// [executeScript] exists to lift.
  ///
  /// A failure throws
  /// [DbasSqliteErrorCode.walAutoCheckpointFailed]: handing out a
  /// writer that silently hoards WAL frames is the exact bug this
  /// pragma exists to prevent. It fails under the same policy as
  /// [_pinWriterSynchronousFull], through the shared
  /// [_applyWalWriterPragma]; what happens beyond the throw belongs to
  /// the caller, see [_pinWalWriterSettings].
  ///
  /// **Web:** no-op. `web/libs/dbas_sqlite_worker.js` already issues
  /// `PRAGMA wal_autocheckpoint=1` on the writer role during worker
  /// init and fails pool creation with `INIT_FAILED` if it cannot, so
  /// the guarantee is established there — same policy, enforced one
  /// layer down. Mirrors [setBusyTimeout]'s web no-op rationale.
  Future<void> _enableWriterAutoCheckpoint(
      DbasSqliteDb writer, String caller) {
    return _applyWalWriterPragma(
      writer,
      caller,
      'PRAGMA wal_autocheckpoint=1',
      DbasSqliteErrorCode.walAutoCheckpointFailed,
      'Without it, committed data would stay in the -wal and any read of '
      'the main .db file alone would silently miss it.',
    );
  }

  /// Issues one WAL writer-policy [pragma] on [writer] and turns any
  /// failure into a thrown [code], so no caller is ever left holding a
  /// writer whose WAL policy is unknown **without being told**.
  ///
  /// Shared by [_pinWriterSynchronousFull] and
  /// [_enableWriterAutoCheckpoint] so both pragmas fail under **one**
  /// policy instead of two that can drift apart. Each caller still names
  /// its own [code] and its own [consequence] — the sentence spelling
  /// out what the writer would silently do if the pragma were skipped,
  /// which is the part of the thrown message that carries the diagnosis.
  /// [caller] is the public method that asked for the policy and
  /// prefixes the message.
  ///
  /// This method **throws and nothing more**. Recovery is deliberately
  /// the caller's: an open that fails here has a half-built pool to tear
  /// down ([_performOpen]), whereas [enableWal] is called on a live,
  /// published connection that may hold statements, readers and a
  /// transaction — tearing that down would be destruction, not safety.
  /// Because the SQLite diagnostics below are read into the exception
  /// BEFORE it is thrown, any teardown a caller runs in its `catch` is
  /// already downstream of the capture, so the old "capture before the
  /// teardown nulls `_db`" ordering rule cannot be got wrong.
  ///
  /// [writer] is passed in rather than re-read from `_db` so the whole
  /// policy is applied to the connection the caller resolved, even if a
  /// concurrent `closeDb` nulls the field mid-flight.
  ///
  /// **Web:** no-op — the worker establishes both pragmas itself; see
  /// the two callers for the per-pragma rationale.
  Future<void> _applyWalWriterPragma(
    DbasSqliteDb writer,
    String caller,
    String pragma,
    DbasSqliteErrorCode code,
    String consequence,
  ) async {
    if (kIsWeb) return;

    final rc = await _platform.executeSql(writer, pragma);
    if (rc == sqliteOk) return;

    final err = _platform.getLastDbError(writer) ?? 'rc=$rc';
    final primary = _platform.getErrorCode(writer) ?? rc;
    final unique = _platform.getUniqueErrorCode(writer);
    throw DbasSqliteException.sqlite(
      code,
      '$caller("$dbName"): $pragma failed on the writer: $err. '
      '$consequence So $caller is rejected rather than completed.',
      sqliteCode: primary,
      sqliteUniqueCode: unique,
    );
  }

  /// Tears the half-built pool down after the WAL writer policy failed
  /// during an open, leaving the instance cleanly **not-open**.
  ///
  /// Called only from [_performOpen]'s `catch`. [cause] is the
  /// [_applyWalWriterPragma] failure that triggered the teardown; it
  /// already carries the SQLite diagnostics, which is why this method is
  /// free to null [_db] — and it appears here only in the teardown log.
  ///
  /// A `closePool` failure here is logged and swallowed: [cause] is the
  /// error the caller is about to rethrow, and replacing it with a
  /// teardown failure would bury the real diagnosis.
  Future<void> _tearDownFailedPoolOpen(Object cause) async {
    final poolPtr = _poolPtr;
    if (poolPtr != null) {
      try {
        await _platform.closePool(dbName, poolPtr);
      } catch (e, st) {
        developer.log(
          'openDb("$dbName"): closePool during open-failure teardown '
          'after the WAL writer policy failed ($cause)',
          name: 'dbas_sqlite.DbasSqlite',
          error: e,
          stackTrace: st,
        );
      }
    }
    _poolPtr = null;
    _db = null;
    _readerPoolSize = 0;
    _readerSlotsAvailable = 0;
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

    // Fold the WAL into the main `.db` file BEFORE the connection goes
    // away.
    //
    // **This ordering is load-bearing**, and a future edit that
    // reorders any step silently reintroduces the bug it fixes —
    // committed data that never reaches the main `.db`:
    //   1. `rollback()` above — a checkpoint issued while a transaction
    //      is still open folds NOTHING (SQLite refuses to checkpoint a
    //      connection holding one), so checkpointing before the
    //      rollback would strand every already-committed frame.
    //   2. the statement sweep above — an open reader pins a WAL
    //      snapshot and blocks the fold of every frame above it.
    //   3. this checkpoint.
    //   4. `closePool` / `closeDb` below.
    //
    // Doing it here rather than leaning on the teardown below is the
    // whole point: `closePool` does NOT fold (measured — pool writer
    // plus one other live connection, 200 committed inserts, `closeDb()`
    // → main 4096 B, `-wal` 832 KB, table absent), and the C-side
    // `closeDb(checkpoint: true)` flag runs TRUNCATE, which is
    // unusable here (see its call site below). What normally makes the
    // default case look healthy is only SQLite's LAST-connection
    // auto-checkpoint, which disappears the moment anything else still
    // has the database open.
    await _checkpointBeforeRawFileAccess('closeDb("$dbName")',
        takeWriterLock: false);

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
      //
      // `checkpoint: false` is deliberate and must stay false: the
      // C-side flag runs `wal_checkpoint(TRUNCATE)`, which waits out the
      // whole `busy_timeout` whenever a reader pins the WAL — measured
      // 5034 ms versus ~0 ms for PASSIVE — and then folds exactly the
      // same frames. The PASSIVE fold above has already done the real
      // work on both teardown paths; all TRUNCATE would add is resetting
      // the `-wal` file length, which no caller here needs.
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
  /// **[sql] must be ONE statement.** Everything after the first `;` is
  /// **silently discarded** — the returned statement prepares, steps and
  /// finalizes only the first, and the C layer passes `sqlite3_prepare_v2`
  /// a `nullptr` tail pointer, so the rest never reaches SQLite at all.
  /// There is no rc, no exception and no log: measured, a
  /// `CREATE TABLE …; CREATE UNIQUE INDEX …;` string returns success
  /// with the table created and the index **missing**. For a script of
  /// several statements use [executeScript], which routes to
  /// `sqlite3_exec` and runs all of them.
  ///
  /// The statement holds the SQL until executed; the underlying
  /// native handle is allocated lazily at execute time on the
  /// connection appropriate for the execution mode (writer for
  /// `executeSql`, pool reader for `executeReader` outside
  /// transactions, writer inside transactions).
  ///
  /// Several statement OBJECTS may be prepared on the same `DbasSqlite`
  /// without blocking each other — that concurrency is unrelated to the
  /// one-statement-per-`sql` limit above. Caller MUST call
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

  /// Switches the writer to WAL journal mode, verifies the readback, and
  /// establishes the **same WAL writer policy a pooled open does**.
  ///
  /// This is the second door into WAL mode. `openDb(readerPoolSize: 0)`
  /// opens in `journal_mode=delete` (measured), so it deliberately skips
  /// the open-time writer pragmas — there is no WAL for them to govern.
  /// Calling this method afterwards creates one. Without
  /// [_pinWalWriterSettings] below, that database would run WAL on
  /// SQLite's stock `wal_autocheckpoint=1000` with an unpinned
  /// `synchronous`: committed frames pile up in the `-wal` and any read
  /// of the main `.db` file alone silently misses them — the exact
  /// silent-data-loss bug the open path exists to prevent, reached
  /// through the public API by a different door. Whichever door a
  /// database enters WAL through, it leaves with the same guarantees.
  ///
  /// **Idempotent.** Both the journal-mode switch and the two pragmas
  /// are no-ops when already in effect, so a second call — or a call on
  /// a pooled database that already ran the pragmas at open — changes
  /// nothing and succeeds.
  ///
  /// **Native:** dispatches to the C lib's `EnableWal` (idempotent on
  /// a pool that's already in WAL).
  ///
  /// **Web:** runs `PRAGMA journal_mode` and verifies the result is
  /// `wal`. The JS pool always opens with WAL via the writer worker;
  /// this serves as a defensive check that pool initialization
  /// actually succeeded. The policy pragmas are a no-op there — the
  /// worker issues both itself at init.
  ///
  /// Throws:
  ///   - [DbasSqliteErrorCode.enableWalDatabaseNotOpened] — the database
  ///     is not open.
  ///   - [DbasSqliteErrorCode.enableWalInsideTransaction] — a
  ///     transaction is active on this instance. SQLite forbids **both**
  ///     halves of this call inside one: `PRAGMA journal_mode=WAL`
  ///     cannot switch journal modes there, and `PRAGMA synchronous`
  ///     answers *"Safety level may not be changed inside a
  ///     transaction"* (measured). So the call could only ever verify,
  ///     never establish — and before this guard it did neither
  ///     consistently: on a database already in WAL the journal-mode
  ///     statement was a silent no-op success, so the same call
  ///     "succeeded" or failed purely on the journal mode it happened to
  ///     find. Rejecting it up front, before any pragma runs, makes both
  ///     configurations behave alike. Commit or roll back first.
  ///   - [DbasSqliteErrorCode.enableWalFailed] — WAL could not be
  ///     activated (read-only media, unsupported VFS, or — on web —
  ///     pool init silently failing to set WAL).
  ///   - [DbasSqliteErrorCode.walSynchronousFullFailed] /
  ///     [DbasSqliteErrorCode.walAutoCheckpointFailed] — WAL was
  ///     activated but its policy could not be established. **The
  ///     connection is left open**, unlike the open path, which tears
  ///     its half-built pool down: this one is live and may hold
  ///     statements, readers and a transaction that are not this
  ///     method's to destroy. The database is in WAL under an unknown
  ///     fold policy, which is why the failure is loud — a caller that
  ///     cannot proceed on those terms should close it.
  Future<void> enableWal() async {
    final writer = _db;
    if (writer == null) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.enableWalDatabaseNotOpened,
        'Database is not opened.',
      );
    }
    if (_isInTransaction) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.enableWalInsideTransaction,
        'Cannot enable WAL inside a transaction: SQLite can neither '
        'switch journal modes nor change the safety level there, so the '
        'call could not establish the WAL writer policy it promises. '
        'Commit or roll back first.',
      );
    }
    final rc = await _platform.enableWal(writer);
    if (rc != sqliteOk) {
      final err = _platform.getLastDbError(writer) ?? 'rc=$rc';
      final primary = _platform.getErrorCode(writer) ?? rc;
      throw DbasSqliteException.sqlite(
        DbasSqliteErrorCode.enableWalFailed,
        'enableWal failed: $err',
        sqliteCode: primary,
        sqliteUniqueCode: _platform.getUniqueErrorCode(writer),
      );
    }
    // Only reached once the connection really is in WAL, so the policy
    // always lands on a journal mode that has something for it to govern.
    await _pinWalWriterSettings(writer, caller: 'enableWal');
  }

  // ── WAL checkpoint ───────────────────────────────────────────────────

  /// Folds committed WAL frames into the main `.db` file and reports
  /// **exactly how far it got**.
  ///
  /// Callers rarely need this: a pooled open sets
  /// `PRAGMA wal_autocheckpoint=1` on the writer, so every commit — and
  /// every bare `INSERT`/`UPDATE`/`DELETE`, which is an implicit
  /// transaction that commits — already folds, and [closeDb] and
  /// [streamCopyDb] fold on their own. Reach for it when you are about
  /// to read, copy or ship the main `.db` file **by other means** and
  /// need to know, not assume, that the data is in there.
  ///
  /// Runs `PRAGMA wal_checkpoint(PASSIVE)` on the **writer** connection,
  /// holding the writer lock for the duration so it cannot interleave
  /// with an in-flight write. PASSIVE is the only mode that cannot
  /// stall; see [_runWalCheckpoint] for the measurement behind that.
  ///
  /// **An incomplete fold is not an error.** A reader holding a WAL
  /// snapshot pins every frame above it and no mode can fold those; they
  /// fold at the next opportunity. That is why this returns a
  /// [DbasSqliteCheckpointResult] instead of `void` or a `bool`: read
  /// [DbasSqliteCheckpointResult.isComplete] (`checkpointed == log`) to
  /// tell a full fold from a partial one.
  /// [DbasSqliteCheckpointResult.busy] **cannot** tell you that — a
  /// PASSIVE checkpoint that folds nothing still reports `busy: 0` and
  /// `SQLITE_OK`.
  ///
  /// Throws:
  ///   - [DbasSqliteErrorCode.checkpointDatabaseNotOpened] — the
  ///     database is not open.
  ///   - [DbasSqliteErrorCode.checkpointInsideTransaction] — a
  ///     transaction is active on this instance. SQLite refuses to
  ///     checkpoint a connection that holds one, so the call would fold
  ///     nothing; failing loudly beats returning a zero that looks like
  ///     a pinned-reader shortfall. Commit or roll back first.
  ///   - [DbasSqliteErrorCode.checkpointDatabaseClosedWaitingLock] — the
  ///     database was closed while this call waited for the writer lock.
  ///   - [DbasSqliteErrorCode.checkpointPrepareFailed] /
  ///     [DbasSqliteErrorCode.checkpointFailed] — the pragma itself
  ///     could not be prepared or stepped.
  Future<DbasSqliteCheckpointResult> checkpoint() async {
    if (!isOpened()) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.checkpointDatabaseNotOpened,
        'Database is not opened.',
      );
    }
    if (_isInTransaction) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.checkpointInsideTransaction,
        'Cannot checkpoint inside a transaction: SQLite refuses to '
        'checkpoint a connection that holds an open transaction, so the '
        'call would fold nothing. Commit or roll back first.',
      );
    }
    await _acquireWriterLock();
    try {
      if (!isOpened()) {
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.checkpointDatabaseClosedWaitingLock,
          'Database was closed while waiting for writer lock.',
        );
      }
      return await _runWalCheckpoint();
    } finally {
      _releaseWriterLock();
    }
  }

  /// Issues `PRAGMA wal_checkpoint(PASSIVE)` on the writer connection
  /// and parses its single `(busy, log, checkpointed)` row.
  ///
  /// The caller MUST already own the writer connection — either holding
  /// the writer lock ([checkpoint], [streamCopyDb]) or running inside
  /// [closeDb]'s teardown, where both wait queues have been cancelled
  /// and no Dart-level holder remains.
  ///
  /// **PASSIVE is not a default that may be relaxed.** `TRUNCATE` folds
  /// exactly the same frames but waits out the entire `busy_timeout`
  /// first whenever a reader pins the WAL — measured against this
  /// library, `PASSIVE → (busy: 0, log: 10, checkpointed: 0)` in ~0 ms
  /// versus `TRUNCATE → (busy: 1, log: 10, checkpointed: 0)` in
  /// **5034 ms**. All the blocking modes buy is resetting the `-wal`
  /// file's length; nothing in this library needs that, and every caller
  /// here sits on a latency budget.
  ///
  /// The pragma goes through the prepare/step/finalize path rather than
  /// `executeSql` because `executeSql` returns only an rc and **discards
  /// result rows** — and the row is the entire point. The statement is
  /// deliberately not registered in `_activeStatements`: it is finalized
  /// in this method's `finally`, and [closeDb] calls this AFTER its
  /// statement sweep.
  ///
  /// Throws [DbasSqliteErrorCode.checkpointPrepareFailed] when the
  /// pragma cannot be prepared, and [DbasSqliteErrorCode.checkpointFailed]
  /// when the step yields anything other than a three-column row. An
  /// **incomplete** fold is not a failure — see
  /// [DbasSqliteCheckpointResult.isComplete].
  Future<DbasSqliteCheckpointResult> _runWalCheckpoint() async {
    const sql = 'PRAGMA wal_checkpoint(PASSIVE)';
    final conn = _db!;
    final prep = await _platform.prepareQuery(conn, sql);
    if (prep.handle == sqliteInvalidStmtHandle) {
      final err = _platform.getLastDbError(conn) ?? 'unknown error';
      final primary = _platform.getErrorCode(conn);
      final msg = 'Failed to prepare "$sql" on the writer connection: $err';
      throw primary != null
          ? DbasSqliteException.sqlite(
              DbasSqliteErrorCode.checkpointPrepareFailed,
              msg,
              sqliteCode: primary,
              sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
            )
          : DbasSqliteException.dart(
              DbasSqliteErrorCode.checkpointPrepareFailed, msg);
    }
    try {
      final cache = RowData();
      final rc = await _platform.readRowAndCache(conn, prep.handle, cache);
      final columns = cache.columns;
      if (rc != sqliteRow || columns == null || columns.length < 3) {
        final err = _platform.getLastStmtError(conn, prep.handle) ??
            _platform.getLastDbError(conn) ??
            'rc=$rc';
        final primary = _platform.getErrorCode(conn);
        final msg = '"$sql" produced no (busy, log, checkpointed) row '
            '(rc=$rc, columns=${columns?.length ?? 0}): $err';
        throw primary != null
            ? DbasSqliteException.sqlite(
                DbasSqliteErrorCode.checkpointFailed,
                msg,
                sqliteCode: primary,
                sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
              )
            : DbasSqliteException.dart(
                DbasSqliteErrorCode.checkpointFailed, msg);
      }
      // A database that is NOT in WAL mode reports (0, -1, -1) here —
      // a legitimate "there was no WAL to fold", which
      // [DbasSqliteCheckpointResult.isComplete] reads as complete.
      return DbasSqliteCheckpointResult(
        busy: toIntSafe(columns[0].value),
        log: toIntSafe(columns[1].value),
        checkpointed: toIntSafe(columns[2].value),
      );
    } finally {
      await _platform.finalizeStmt(conn, prep.handle);
    }
  }

  /// Best-effort PASSIVE checkpoint for the two paths that are about to
  /// expose the raw `.db` file — [closeDb] and [streamCopyDb]. **Never
  /// throws.**
  ///
  /// Both outcomes it can report are logged via `dart:developer` rather
  /// than raised, for different reasons:
  ///
  ///   - **Incomplete fold** (`isComplete == false`): not an error at
  ///     all. A reader pinning a WAL snapshot blocks the frames above
  ///     it; they fold at the next opportunity and nothing committed is
  ///     lost. Throwing would turn a recoverable, self-healing state
  ///     into a failed [closeDb] / [streamCopyDb], and retrying is
  ///     pointless — no checkpoint mode can fold a pinned frame, and
  ///     the blocking ones burn the whole `busy_timeout` proving it.
  ///   - **Outright failure**: [closeDb] must reach `closePool` no
  ///     matter what, exactly as it already does for a failed
  ///     `rollback()` or a failed statement close — aborting teardown
  ///     would leak the pool, the instance-cache entry and OS handles,
  ///     which is strictly worse than an unfolded WAL that the next
  ///     open will fold anyway.
  ///
  /// **Silence is what caused this class of bug**, so every non-ideal
  /// outcome is logged with the triple and its consequence spelled out.
  /// Callers that need a hard guarantee have [checkpoint], which
  /// reports through its return value instead.
  ///
  /// [takeWriterLock] is `false` only from [closeDb], where `_closing`
  /// is already latched — [_acquireWriterLock] rejects outright at that
  /// point, and it does not need to be held: [closeDb] has already
  /// cancelled both wait queues, so no Dart-level holder remains.
  Future<void> _checkpointBeforeRawFileAccess(
    String context, {
    required bool takeWriterLock,
  }) async {
    if (!isOpened()) return;
    if (_isInTransaction) {
      developer.log(
        '$context: skipped the WAL checkpoint — a transaction is still '
        'open and SQLite refuses to checkpoint a connection holding one. '
        'Committed frames may remain in the -wal, so a read of the main '
        '.db file alone can be missing them.',
        name: 'dbas_sqlite.DbasSqlite',
      );
      return;
    }
    var lockHeld = false;
    try {
      if (takeWriterLock) {
        await _acquireWriterLock();
        lockHeld = true;
      }
      final result = await _runWalCheckpoint();
      if (!result.isComplete) {
        developer.log(
          '$context: WAL checkpoint folded ${result.checkpointed} of '
          '${result.log} frame(s) (busy=${result.busy}). A reader is '
          'holding a WAL snapshot, so the remaining frames stay in the '
          '-wal and fold at the next opportunity — committed data is NOT '
          'lost, but a copy of the main .db file alone would be missing '
          'them right now.',
          name: 'dbas_sqlite.DbasSqlite',
        );
      }
    } catch (e, st) {
      developer.log(
        '$context: WAL checkpoint failed; continuing. Committed frames '
        'may still be in the -wal.',
        name: 'dbas_sqlite.DbasSqlite',
        error: e,
        stackTrace: st,
      );
    } finally {
      // Only release what we actually took — a failed acquire never
      // held the lock, and releasing it would hand the lock to a queued
      // waiter nobody will ever release it for.
      if (lockHeld) _releaseWriterLock();
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

  // ── Multi-statement script ───────────────────────────────────────────

  /// Runs a **whole SQL script** — every statement in [sql], not just
  /// the first.
  ///
  /// This is the **only** entry point in this library that executes more
  /// than one statement per call. [prepareQuery] +
  /// [DbasSqliteStatement.executeSql] prepare, step and finalize exactly
  /// ONE statement and **silently discard everything after the first
  /// `;`**: measured, `CREATE TABLE …; CREATE UNIQUE INDEX …;
  /// PRAGMA foreign_keys = ON;` through that path returns `rc=0` with no
  /// exception and no log, and leaves the table present, the index
  /// **absent** and `foreign_keys` still `0`. Use `executeScript`
  /// whenever [sql] may hold more than one statement — runtime DDL plus
  /// its indexes, a migration step, an open-time pragma block.
  ///
  /// Routes to the C `ExecuteSql` entry point (`sqlite3_exec`), which
  /// iterates the statements itself. **Nothing splits on `;` in Dart**,
  /// and nothing may: `sqlite3_complete` is not exported by the shipped
  /// binary, so a Dart detector would have to hand-roll a lexer over
  /// quoted literals, bracketed identifiers, comments, blob literals and
  /// `BEGIN … END` trigger bodies — and any script carrying a `CHECK`
  /// body of arbitrary user SQL would be shredded by a naive split.
  ///
  /// **NOT ATOMIC on its own — read this twice.** Execution stops at the
  /// first statement that fails, and every statement before it has
  /// already run. Outside a transaction each of those is its own
  /// implicit transaction, so they are already **committed** and nothing
  /// can take them back. A script that must be all-or-nothing MUST be
  /// wrapped by the caller:
  ///
  /// ```dart
  /// await db.transaction((tx) => tx.executeScript(migrationSql));
  /// ```
  ///
  /// **Allowed inside a transaction**, deliberately — this does NOT copy
  /// [vacuum]'s or [checkpoint]'s in-transaction guard. Those two reject
  /// because SQLite itself refuses them there, so the call could only
  /// pretend to work. A script has no such objection, and wrapping it in
  /// a transaction is the *only* way to get the atomicity above; a guard
  /// here would leave callers with nothing but the unsafe mode. Inside a
  /// transaction the call registers as a reentrant writer user rather
  /// than taking the writer lock — [beginTransaction] already holds it
  /// and the queue is FIFO, so re-acquiring would park behind itself.
  ///
  /// **No bindings.** `sqlite3_exec` has no bind surface, so [sql] must
  /// be complete text. Parameterised SQL belongs on [prepareQuery];
  /// never interpolate untrusted values into a script.
  ///
  /// **Result rows are discarded.** The `sqlite3_exec` callback is
  /// `nullptr`, so a `SELECT` in the script runs and yields nothing. To
  /// read rows use [prepareQuery] with
  /// [DbasSqliteStatement.executeReader] — that is exactly why
  /// `PRAGMA wal_checkpoint` deliberately avoids this path (see
  /// [checkpoint]): its `(busy, log, checkpointed)` row is the point.
  ///
  /// Returns the connection's `sqlite3_changes64` read **after** the
  /// script, so it can stand in for [DbasSqliteStatement.executeSql]'s
  /// return. The C header names the connection-scoped counters as
  /// existing precisely for `ExecuteSql` callers — no statement handle
  /// exists in this flow. It is therefore the count of the **last
  /// row-changing statement** in the script, not a total: a script
  /// ending in DDL or a `SELECT` still reports the last
  /// `INSERT`/`UPDATE`/`DELETE`, and a script containing none at all
  /// reports whatever the connection last left there, which may predate
  /// this call.
  ///
  /// **Web caveat.** The worker's `exec` action refuses with
  /// `SQLITE_BUSY` — *"Cannot write while a read statement is open on
  /// this worker"* — whenever the writer worker still has an open
  /// statement. In practice that means a live [DbasSqliteReader] on the
  /// **writer** connection blocks a script: readers route to the writer
  /// once the current transaction has performed a write, so a script
  /// issued mid-transaction while a cursor from that same transaction is
  /// still open fails on web where it succeeds natively. Close the
  /// reader first. This is not new to `executeScript` — every
  /// `executeSql`-routed verb ([beginTransaction], [commit], [rollback],
  /// [vacuum]) shares it — but a script is the call most likely to be
  /// issued in the middle of other work.
  ///
  /// Throws:
  ///   - [DbasSqliteErrorCode.executeScriptDatabaseNotOpened] — the
  ///     database is not open.
  ///   - [DbasSqliteErrorCode.executeScriptDatabaseClosedWaitingLock] —
  ///     the database was closed while this call waited for the writer
  ///     lock.
  ///   - [DbasSqliteErrorCode.executeScriptFailed] — a statement in the
  ///     script failed. The message carries `sqlite3_exec`'s `errMsg`,
  ///     which names the offending statement; everything before it has
  ///     already run.
  Future<int> executeScript(String sql) async {
    if (!isOpened()) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.executeScriptDatabaseNotOpened,
        'Database is not opened.',
      );
    }
    // Mirrors DbasSqliteStatement.executeSql's lock decision, and for
    // the same reason: inside a transaction the writer lock is already
    // held for the transaction's whole lifetime, so re-acquiring it on a
    // FIFO queue would deadlock against ourselves. Registering instead
    // is what lets `commit()` see that this dispatch is still live on
    // the writer connection.
    final lockHeld = _isInTransaction;
    ReentrantWriterOpToken? reentrantOp;
    if (lockHeld) {
      reentrantOp = beginReentrantWriterOpInternal();
    } else {
      await _acquireWriterLock();
    }
    try {
      if (!isOpened()) {
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.executeScriptDatabaseClosedWaitingLock,
          'Database was closed while waiting for writer lock.',
        );
      }
      final conn = _db!;
      // A script is assumed to write: it is the DDL/DML door. Marking
      // before dispatch is conservative in the same way `executeSql`'s
      // is — a failed script still routes later reads through the writer,
      // which is slower but never incorrect.
      markTransactionWriteInternal();
      // Ordering is **load-bearing**: start the dispatch WITHOUT awaiting
      // so its still-pending future reaches the database before this
      // method suspends. `rollback()` drains that future instead of
      // issuing ROLLBACK on top of it. Registration, mark and dispatch
      // all run in this one synchronous turn, so no other flow can
      // observe a half-registered script.
      final dispatch = _platform.executeSql(conn, sql);
      if (reentrantOp != null) {
        trackReentrantWriterOpDispatchInternal(reentrantOp, dispatch);
      }
      final rc = await dispatch;
      if (rc != sqliteOk) {
        final err = _platform.getLastDbError(conn) ?? 'rc=$rc';
        final primary = _platform.getErrorCode(conn) ?? rc;
        throw DbasSqliteException.sqlite(
          DbasSqliteErrorCode.executeScriptFailed,
          'Script failed (rc=$rc): $err. Execution stopped there; every '
          'statement before it has already run, and outside a transaction '
          'those are already committed.',
          sqliteCode: primary,
          sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
        );
      }
      return _platform.getAffectedRows(conn);
    } finally {
      if (reentrantOp != null) {
        endReentrantWriterOpInternal(reentrantOp);
      } else {
        _releaseWriterLock();
      }
    }
  }

  /// Rebuilds the database file via VACUUM. Cannot run inside a
  /// transaction.
  ///
  /// Single-statement by nature — for a multi-statement string use
  /// [executeScript].
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
