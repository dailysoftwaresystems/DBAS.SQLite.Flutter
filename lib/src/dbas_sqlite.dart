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

/// Opaque handle for one operation that has crossed from Dart into
/// native code, handed out by [DbasSqlite.beginNativeOpInternal] and
/// given back to [DbasSqlite.endNativeOpInternal].
///
/// Carries only the operation's process-unique `id` — a record rather
/// than a bare `int` so it cannot be confused with the pointers and
/// handles this class passes around. The registration's label lives on
/// the registry entry, which is what
/// [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout] reads; nothing
/// needs it here. No `generation` either, unlike
/// [ReentrantWriterOpToken]: this registry is not scoped to a
/// transaction, so there is no epoch a token could outlive.
typedef NativeOpToken = ({int id});

/// One registered operation that is inside native code and holds native
/// resources no other tracked owner can see yet — a checked-out pool
/// reader, a live `sqlite3_stmt`, an in-flight worker dispatch. Lives in
/// `DbasSqlite._nativeOps` between [DbasSqlite.beginNativeOpInternal]
/// and [DbasSqlite.endNativeOpInternal].
class _NativeOp {
  _NativeOp(this.label);

  /// What is in flight, for the drain-timeout diagnostic.
  final String label;

  /// How long this operation has been inside native code. Reported
  /// alongside [label] on a drain timeout so the diagnostic can tell
  /// "one operation stuck for the whole window" from "operations
  /// churning and the drain never converging" — the same 30 s message
  /// otherwise describes both. Monotonic, never a wall clock.
  final Stopwatch elapsed = Stopwatch()..start();

  /// Completed — **never** with an error — by
  /// [DbasSqlite.endNativeOpInternal]. `_drainNativeOps` awaits this
  /// instead of polling, so an operation that hands back wakes the drain
  /// immediately, and the drain can never inherit a failure that the
  /// operation's real caller is already awaiting.
  final Completer<void> done = Completer<void>();
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

  /// Deadline for [closeDb]'s wait on operations that already crossed
  /// into native code (see [beginNativeOpInternal]). Sized like the two
  /// waits above because it covers the same worst case: a dispatch
  /// queued behind a busy worker isolate.
  ///
  /// Reaching it is a bug in the calling code — an operation was still
  /// running when the database was closed and never handed back — so it
  /// surfaces as [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout]
  /// rather than letting teardown proceed over live native resources.
  static const int kNativeOpDrainTimeoutMs = 30000;

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

  /// Test-only override for [kNativeOpDrainTimeoutMs]. Same rationale as
  /// [debugPoolAcquireTimeoutMs] — the drain-timeout test would
  /// otherwise pause for 30 s on every run. Reset it to `null` in a
  /// `finally`; it is static, so a leaked override would shorten every
  /// later test's teardown wait.
  @visibleForTesting
  static int? debugNativeOpDrainTimeoutMs;

  /// Where this package publishes diagnostics that have **no other way
  /// out**. `null` by default; wire it once, at app start.
  ///
  /// ```dart
  /// DbasSqlite.onDiagnostic = (message) => myLogger.warn(message);
  /// ```
  ///
  /// Every such diagnostic also goes to `dart:developer`'s `log`, but
  /// that sink reaches nobody in the configurations that matter:
  /// `developer.log` publishes to the VM service `Logging` stream and the
  /// message is **discarded when no service client is subscribed**. A
  /// release build on a device has no VM service at all, and a
  /// `flutter test` run has no client attached — so a `flutter run` debug
  /// session is the only place those messages are visible, and it is the
  /// one place the problems they describe do not usually happen.
  ///
  /// What reports through here:
  ///   - [DbasSqliteReader.close]'s stall report — the one wait in this
  ///     library that is deliberately **unbounded** and that no timeout
  ///     will ever surface, so a teardown wedged behind a step that never
  ///     hands back is otherwise indistinguishable from a slow one;
  ///   - [closeDb]'s count of statements that failed to finalize, which
  ///     on the pool path leaks a handle without failing the close; and
  ///   - a `poolReleaseReader` that threw inside [setBusyTimeout], which
  ///     strands a reader `ClosePool` will block on.
  ///
  /// All three describe resources that are already lost or about to
  /// wedge, on paths whose return value cannot say so.
  ///
  /// **Called synchronously from teardown paths, and it must BE
  /// synchronous.** The type is `void Function(String)`, and Dart accepts
  /// an `async` body there — but the returned future is dropped, so a
  /// failure inside one escapes the guard below as an unhandled
  /// asynchronous error instead of being contained. Only a *synchronous*
  /// throw is covered by the "a consumer's logger must not be able to
  /// break a database close" guarantee. Hand the message to something
  /// that buffers, and do the awaiting elsewhere.
  ///
  /// A *slow* sink delays whatever is reporting, so keep it cheap and
  /// non-blocking. It is static: set it once rather than per instance,
  /// and reset it in a `finally` / `addTearDown` in tests.
  static void Function(String message)? onDiagnostic;

  /// Publishes [message] to `dart:developer` under [name] **and** to
  /// [onDiagnostic]. Package-internal — see [onDiagnostic] for why both
  /// sinks exist and why a throwing consumer sink is swallowed here.
  static void reportDiagnosticInternal(String message, {required String name}) {
    developer.log(message, name: name);
    final sink = onDiagnostic;
    if (sink == null) return;
    try {
      sink(message);
    } catch (e, st) {
      // Swallowed rather than rethrown: this runs inside teardown, and a
      // consumer logger that throws must not take the close down with it.
      //
      // **The escape hatch has to survive its own failure.**
      // `developer.log` is the sink this whole mechanism exists to
      // replace — dropped whenever no VM service client is subscribed,
      // i.e. every release build and every `flutter test` run — so
      // falling back to it alone means a consumer whose logger was torn
      // down BEFORE the database (a common shutdown order, and shutdown
      // is exactly when a stall report fires) learns nothing at all
      // about a wedged teardown. `Zone.current.print` reaches logcat /
      // oslog in a release build and stdout under `flutter test`, so the
      // ORIGINAL message goes out through it too, not just the failure.
      developer.log(
        'DbasSqlite.onDiagnostic threw while reporting a diagnostic; the '
        'message above was still published to dart:developer and the '
        'operation that reported it continues.',
        name: name,
        error: e,
        stackTrace: st,
      );
      try {
        Zone.current.print(
          '[$name] DbasSqlite.onDiagnostic threw ($e); the diagnostic it '
          'was given follows so it is not lost: $message',
        );
      } catch (_) {
        // A zone whose own print handler throws leaves nothing left to
        // report through. Teardown still continues, which is the whole
        // guarantee this handler exists to keep.
      }
    }
  }

  /// Test-only observation point, invoked **synchronously** by [closeDb]
  /// immediately before it dispatches the destructive `closePool` /
  /// `closeDb` call — i.e. at the exact instant the native connection
  /// stops being safe to touch.
  ///
  /// Exists so a test can assert the ordering invariant this release is
  /// built on ([debugInFlightNativeOpCount] is zero by then) at the only
  /// moment where it matters. Synchronous by construction: teardown must
  /// not gain a suspension point that production code does not have.
  /// Reset it to `null` in a `finally` / `addTearDown`; it is static.
  @visibleForTesting
  static void Function(DbasSqlite db)? debugBeforeDestructiveClose;

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

  /// Every operation currently inside NATIVE code on this connection,
  /// keyed by the operation id inside [NativeOpToken]. Registered by
  /// [beginNativeOpInternal] and drained by [closeDb] via
  /// [_drainNativeOps].
  ///
  /// Deliberately wider than [_reentrantWriterOps], which the writer-lock
  /// machinery uses to answer "may this transaction COMMIT?" and
  /// therefore only ever tracks writer users inside a transaction. This
  /// one is connection-wide and route-blind — a pool read outside any
  /// transaction registers here too — because the question teardown asks
  /// is different: "is anything still holding native resources?"
  final Map<int, _NativeOp> _nativeOps = {};

  /// Source of process-unique ids for [_nativeOps]. Ids are never
  /// reused, so a token can only ever match the one operation it was
  /// handed out for.
  int _nativeOpSeq = 0;

  int? _poolPtr;

  /// The pool pointer [closeDb] is currently dispatching `closePool` on,
  /// and `null` at every other moment. **A retained pointer, not a second
  /// source of truth**: [_poolPtr] is cleared before that dispatch — so
  /// nothing can route a new read into a pool that is going away, and a
  /// second concurrent [closeDb] cannot dispatch `closePool` on the same
  /// pointer twice — while the release path below still needs a live
  /// pointer for the whole duration of the call.
  ///
  /// That is the asymmetry: the C pool struct is not freed until
  /// `ClosePool` returns, and the one thing `ClosePool` blocks waiting
  /// for is `PoolReleaseReader`. A [releaseReaderConnectionInternal]
  /// arriving in that window — a reader whose close was already in
  /// progress when teardown started — must reach the pool, or nothing
  /// ever hands that reader back and `ClosePool` waits forever with no
  /// timeout on the path.
  int? _closingPoolPtr;

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

  /// Non-null while a [closeDb] call on this instance is in flight.
  /// Concurrent [closeDb] callers await this single future instead of
  /// each running their own teardown — see [closeDb] for why the
  /// capture-and-null of [_poolPtr] is not sufficient on its own.
  Future<void>? _closingDb;

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
  /// **Blocks until the connection is quiescent.** Callers that are only
  /// parked in Dart are rejected outright ([_closing], plus the two
  /// wait-queue cancellations), but an operation that has already
  /// crossed into native code cannot be recalled — it is waited for, via
  /// [_drainNativeOps]. That drain is bounded by
  /// [kNativeOpDrainTimeoutMs]; on expiry this method **throws**
  /// [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout] and leaves the
  /// connection open rather than destroying it under live native work.
  /// That is a real behaviour change: `closeDb()` on a database with an
  /// un-awaited call still in flight now takes as long as that call
  /// does, instead of racing it.
  ///
  /// **[kNativeOpDrainTimeoutMs] bounds that drain, not this method.**
  /// The statement sweep that runs after it closes every open reader,
  /// and [DbasSqliteReader.close] waits for that reader's in-flight
  /// `readRow` steps **unboundedly** — see its docs for why a timeout
  /// there could only expire into the corruption it exists to prevent.
  /// So `closeDb()` on a connection with a parked reader step blocks for
  /// as long as that step takes, with no ceiling; the reader reports a
  /// stall every [DbasSqliteReader.kStepDrainStallReportMs] instead of
  /// failing (through [onDiagnostic], which is the only sink that
  /// reaches a release build).
  ///
  /// **The sweep JOINS a close that is already running** rather than
  /// skipping it. `close()` on both a statement and a reader latches
  /// `isClosed` synchronously and then suspends, so an un-awaited
  /// `reader.close()` leaves a reader that reports itself closed while
  /// it still holds a checked-out pool connection — and skipping it
  /// there means nothing ever releases that connection and `ClosePool`
  /// waits on it forever. Both closes are join-idempotent for this
  /// reason; see [DbasSqliteStatement.close].
  ///
  /// If the in-flight `rollback()` itself fails (e.g. the connection
  /// is already in a corrupt state at the SQLite layer), the failure
  /// is logged via `dart:developer` and teardown continues — otherwise
  /// a single rollback failure would skip statement cleanup, queue
  /// cancellation, and pool close, leaving the cache and OS resources
  /// dangling. Code that needs to react to a failed rollback must call
  /// `rollback()` explicitly before `closeDb()`.
  ///
  /// **Single-flight**, mirroring [openDb]'s `_opening`: a second call
  /// arriving while a teardown is in flight JOINS it and returns when
  /// that teardown does — with its outcome, error included. Without the
  /// join a second call walked this whole method while the first was
  /// suspended inside `closePool`, because every step short-circuits by
  /// then (`rollback` on `!_isInTransaction`, the drain on an empty
  /// registry, the sweep on an already-cleared statement set, the
  /// checkpoint on `!isOpened()`) — and it reported **success while the
  /// pool was still being destroyed**, which is a lie a caller may act
  /// on, `dropDb()` on a live pool being the obvious one. Nothing
  /// re-enters this method from inside teardown, so the join can never
  /// wait on itself: [dropDb] calls it only behind `isOpened()`, and
  /// [attachDb] / [attachStreamDb] only from outside a close.
  Future<void> closeDb() async {
    final inFlight = _closingDb;
    if (inFlight != null) {
      // Deliberately NOT the reopen-recursion [openDb] runs after its
      // join. That exists because the open it waited on may have failed
      // and left the instance closed; here a failed teardown surfaces as
      // the error this `await` rethrows, and a successful one has left
      // the connection in exactly the state this caller asked for.
      await inFlight;
      return;
    }
    // [_performClose] is `async`, so it runs synchronously up to its
    // first `await` and nothing can observe the gap before the marker is
    // published — the same reasoning [openDb] relies on.
    final close = _performClose();
    _closingDb = close;
    try {
      await close;
    } finally {
      // Clear the marker only if it still points at this close — an
      // open→close in the meantime could have replaced it.
      if (identical(_closingDb, close)) _closingDb = null;
    }
  }

  /// The teardown body of [closeDb]. Always run through that method's
  /// single-flight guard, so at most one teardown runs per instance at a
  /// time.
  Future<void> _performClose() async {
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

    // Wait for every operation that already crossed into NATIVE code to
    // hand back — BEFORE the statement sweep below.
    //
    // The `_closing` latch and the two queue cancellations above stop
    // everything that has not started; neither can recall a call that is
    // already inside the C pool. The reachable case is a read suspended
    // anywhere in `executeReader`'s prepare window: it has been handed a
    // connection and has a live `sqlite3_stmt`, yet `_activeReader` is
    // still null — so the sweep's `stmt.close()` would await NOTHING
    // while latching `_closed`, and `_activeStatements.clear()` would
    // then disown it outright. What happens next is decided only by
    // which connection the read was routed to, and both outcomes are
    // fatal:
    //   - POOL reader: nothing ever calls `PoolReleaseReader` for it, so
    //     `ClosePool` — which blocks until every checked-out reader is
    //     back — waits forever and `closeDb` never returns.
    //   - WRITER (read-your-writes: inside a transaction, after a
    //     write): the writer is NOT checkout-tracked on the C side, so
    //     `ClosePool` proceeds and force-closes it via
    //     `closeDbCore(force=true)`, finalizing the live `sqlite3_stmt`
    //     and freeing the `SQLiteDb`. The reader `executeReader` then
    //     hands its caller points at freed memory.
    // One registration at `executeReader`'s entry — before its routing
    // decision — is what covers both by construction rather than by two
    // parallel special cases.
    //
    // **Before the sweep, not after**, and that ordering is as
    // load-bearing as the checkpoint ordering below: the drain returns
    // once each operation has handed its resources to an owner this
    // method can find, which for a read that got far enough is the
    // `_activeReader` on its STILL-TRACKED statement. The sweep is what
    // then closes it. Drain after the sweep instead and that reader
    // hangs off a statement the sweep has already disowned — nothing
    // closes it, and on the pool route nothing releases its reader.
    await _drainNativeOps();

    // Close every still-open statement (which closes its active reader
    // if any). List.of() snapshots the set since close() mutates it.
    //
    // `close()` on both the statement and the reader is JOIN-idempotent,
    // and that is what makes this sweep complete rather than merely
    // thorough: a close already running — `unawaited(reader.close())`
    // before a logout, say — has latched `isClosed` synchronously while
    // still holding a checked-out pool connection, so a sweep that
    // treated the flag as "nothing to do" would disown that reader on the
    // `clear()` below and leave `ClosePool` waiting on it forever.
    int stmtCloseFailures = 0;
    for (final stmt in List.of(_activeStatements)) {
      try {
        // The teardown-flavoured close: identical work, but it lets a
        // `readerClosedDuringScan` raised afterwards say the database was
        // closed under the scan rather than guess between that and a
        // deliberate `close()` the consumer made themselves.
        await stmt.closeForTeardownInternal();
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

    // Reported to the consumer sink, not just counted.
    //
    // The count below is read only by the SINGLE-CONNECTION branch, and
    // only when `CloseDb` came back `SQLITE_BUSY`. On the POOL path — the
    // default for every real app — `ClosePool` force-closes and this
    // method returns SUCCESS, so a statement that failed to finalize
    // leaks (possibly with the pool reader its abandoned reader still
    // holds) with nothing in the return value to say so. `developer.log`
    // above is not a report: it is dropped whenever no VM service client
    // is subscribed, which is every release build and every
    // `flutter test` run. See [onDiagnostic].
    if (stmtCloseFailures > 0) {
      reportDiagnosticInternal(
        'closeDb("$dbName"): $stmtCloseFailures tracked statement(s) failed '
        'to finalize during the teardown sweep. Their native handles — and, '
        'for a statement with an open reader, the pool connection it had '
        'checked out — may be leaked for the lifetime of the process. On the '
        'pool path this does NOT fail closeDb(): ClosePool force-closes and '
        'this call still returns normally, so this report is the only signal '
        'you get. See the prior dart:developer entries for the underlying '
        'errors.',
        name: 'dbas_sqlite.DbasSqlite',
      );
    }

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
    //   2. the native-operation drain above — a read still in
    //      `executeReader`'s prepare window is not yet an open reader,
    //      so the sweep in step 3 cannot see it at all.
    //   3. the statement sweep above — an open reader pins a WAL
    //      snapshot and blocks the fold of every frame above it.
    //   4. this checkpoint.
    //   5. `closePool` / `closeDb` below.
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

    // Capture the handles into locals, and null `_db` BEFORE awaiting the
    // destructive dispatch below.
    //
    // Both `closePool` and `closeDb` run on a worker isolate, so this
    // method is suspended across them while the MAIN isolate keeps
    // running. Anything that reads `_db` in that window — `isOpened()`,
    // `getTotalChanges()`, `getDbFileName()`, all of which call straight
    // into FFI — would otherwise hand a pointer the C side is in the
    // middle of freeing back into a native call. Nulled first, those
    // callers see a CLOSED connection, which is a state they already
    // handle, instead of a freed one.
    //
    // **The pool pointer is nulled here too — but RETAINED in
    // [_closingPoolPtr] for the duration of the dispatch.** The C pool
    // struct is not freed until `ClosePool` returns, and the one thing
    // `ClosePool` is blocked waiting for is `PoolReleaseReader`, so a
    // `releaseReaderConnectionInternal` arriving in that window must
    // still find a live pointer to release through — dropping it makes
    // that release return early, which leaks the checked-out reader and
    // wedges the very call that is waiting for it, with no timeout on
    // this path. Clearing `_poolPtr` itself is what keeps a second,
    // concurrent `closeDb()` from dispatching `closePool` on the same
    // pointer twice, and what stops a new read routing into a pool that
    // is going away.
    //
    // **Only the branch that actually OWNS a pointer publishes one.** The
    // capture-and-null above is atomic against the event loop, so a
    // second teardown reaching here finds `poolPtr == null` — and an
    // unconditional assignment would then clear the retained pointer out
    // from under the first, for the whole `closePool` window, restoring
    // exactly the silent early return in
    // [releaseReaderConnectionInternal] that retaining it removes.
    // [closeDb]'s single-flight join is what stops a second teardown
    // arriving here at all; this keeps the invariant local to the code
    // that depends on it. The `finally` below needs no such condition —
    // it only runs on the branch that published a non-null pointer.
    final poolPtr = _poolPtr;
    final conn = _db;
    _poolPtr = null;
    if (poolPtr != null) _closingPoolPtr = poolPtr;
    _db = null;

    // Invoked here, after the nulling and before the dispatch, so a test
    // observes the state teardown is actually about to run under.
    final debugHook = debugBeforeDestructiveClose;
    if (debugHook != null) debugHook(this);

    if (poolPtr != null) {
      try {
        await _platform.closePool(dbName, poolPtr);
      } finally {
        // The pointer stops being releasable exactly when `ClosePool`
        // returns and the struct is freed — not a moment earlier. In the
        // `finally` so a failed `closePool` cannot leave a dangling
        // pointer behind for a later release to use.
        _closingPoolPtr = null;
      }
    } else if (conn != null) {
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
      final rc = await _platform.closeDb(conn, checkpoint: false);
      if (rc == sqliteBusy) {
        // `conn` is still a VALID pointer here: the strict close path
        // refuses (force=false) rather than freeing when a handle is
        // live, so the diagnostics below read a connection that is very
        // much alive. Fall back to the observed `rc` when the helpers
        // return null (the C lib didn't queue an active error on it).
        final err = _platform.getLastDbError(conn) ?? 'live handles';
        final primaryRc = _platform.getErrorCode(conn) ?? rc;
        final uniqueRc = _platform.getUniqueErrorCode(conn);
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
  /// [DbasSqliteErrorCode.setBusyTimeoutReaderFailed], and
  /// [DbasSqliteErrorCode.readerSlotWaitCancelled] once [closeDb] has
  /// started — reconfiguring a connection that is being torn down would
  /// hold pool readers `ClosePool` is already blocked on.
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
    // Rejected during teardown, in the same shape as [_acquireWriterLock]
    // / [_acquireReaderSlot] and with the same code the pool-closing
    // branch below already uses.
    //
    // **This guard is what the registration below cannot do for itself.**
    // [_drainNativeOps] runs ONCE, and this method's only other gate is
    // the `_db == null` check above — which is still false at all three
    // of [closeDb]'s post-drain suspension points (the statement sweep,
    // the PASSIVE checkpoint, and the `closePool` await). Arriving in any
    // of them, this method would register into a registry nobody will
    // drain again and then check readers out of the C pool DIRECTLY,
    // bypassing the Dart-side semaphore — the one path where reaching
    // native code is not stopped by a second gate. `ClosePool` would then
    // block on readers this method is still holding.
    if (_closing) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.readerSlotWaitCancelled,
        'setBusyTimeout was cancelled: the database is closing.',
      );
    }
    if (kIsWeb) {
      // The JS pool does not expose a per-connection busy_timeout
      // accessor; the writer worker manages its own busy handling.
      // Returning silently here is consistent with the JS pool's
      // model — there is nothing to do.
      return;
    }
    // This method checks readers out of the C pool DIRECTLY, bypassing
    // the Dart-side reader-slot semaphore, so it is invisible to every
    // other teardown signal — a registration taken BEFORE the drain is
    // what stops a `closeDb` from calling `ClosePool` while slots are
    // held here, and the `_closing` guard above is what stops one being
    // taken after it. It wraps the whole method (the writer pragma
    // included) and is released by the OUTER `finally`, i.e. after every
    // `poolReleaseReader` below has run.
    final nativeOp = beginNativeOpInternal('setBusyTimeout');
    try {
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

      // Captured into a local, and every call below goes through it.
      // `_poolPtr` is nulled by [closeDb] once its `closePool` returns,
      // and this method suspends on a worker dispatch per slot — so
      // reading the field again in the release loop below could throw a
      // `TypeError` out of a `finally`, leaking every reader already
      // acquired and wedging the `ClosePool` that is waiting for them.
      final poolPtr = _poolPtr;
      if (poolPtr == null || _readerPoolSize == 0) return;

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
              await delegate.poolAcquireReaderBlocking(poolPtr, acquireMs);
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
          // Guarded PER ITERATION. `poolReleaseReader` is a synchronous
          // FFI call, so a throw from the first one would abort this loop
          // and strand every reader after it — the same leak the captured
          // `poolPtr` local above exists to prevent, through a different
          // door, and every stranded reader is one `ClosePool` blocks on
          // with no timeout. Report and keep releasing.
          try {
            delegate.poolReleaseReader(poolPtr, readerPtr);
          } catch (e) {
            reportDiagnosticInternal(
              'setBusyTimeout("$dbName"): releasing pool reader $readerPtr '
              'failed ($e). That reader stays checked out for the lifetime '
              'of the process and ClosePool will block on it, so a later '
              'closeDb() may never return. The remaining readers this call '
              'holds are still being released.',
              name: 'dbas_sqlite.DbasSqlite',
            );
          }
        }
      }
    } finally {
      endNativeOpInternal(nativeOp);
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
  ///   - [DbasSqliteErrorCode.writerLockWaitCancelled] — [closeDb] has
  ///     started. Moving a connection that is being torn down into WAL
  ///     would dispatch the switch and its pragmas against the writer
  ///     `closePool` is about to force-close.
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
    // Rejected during teardown, in the same shape as [_acquireWriterLock]
    // and with the code that lock already uses.
    //
    // **This guard is what the registration below cannot do for itself**,
    // and this was the last registry caller without a second gate.
    // [_drainNativeOps] runs ONCE; `checkpoint`, `vacuum`,
    // `beginTransaction`, `commit` and `executeScript` all take
    // [_acquireWriterLock] first, which rejects while [_closing], and
    // [setBusyTimeout] carries an explicit guard for the same reason.
    // This method's only other gate is the `_db == null` check above,
    // which is still false at every one of [closeDb]'s post-drain
    // suspension points (the statement sweep, the PASSIVE checkpoint and
    // the `closePool` await). Arriving in one of them it would register
    // into a registry nobody will drain again and then dispatch the
    // journal-mode switch and both writer pragmas — several worker
    // round-trips on `_db` — against the very writer `closePool` is on
    // its way to force-close via `closeDbCore(force=true)`.
    if (_closing) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.writerLockWaitCancelled,
        'enableWal was cancelled: the database is closing.',
      );
    }
    // Covers the journal-mode switch AND the writer-policy pragmas that
    // follow it — both are dispatches on the live writer connection, and
    // a teardown that slipped between them would leave the database in
    // WAL under an unpinned fold policy on a connection being freed.
    final nativeOp = beginNativeOpInternal('enableWal');
    try {
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
      // always lands on a journal mode that has something for it to
      // govern.
      await _pinWalWriterSettings(writer, caller: 'enableWal');
    } finally {
      endNativeOpInternal(nativeOp);
    }
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
    final nativeOp = beginNativeOpInternal('checkpoint');
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
      endNativeOpInternal(nativeOp);
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
  ///
  /// **`takeWriterLock: false` is also the one native call in this class
  /// that deliberately does NOT register with [beginNativeOpInternal].**
  /// It is reachable only from [closeDb], *after* that method's
  /// [_drainNativeOps] — registering would make teardown wait on itself,
  /// forever. `takeWriterLock: false` already means "I am teardown", so
  /// the exemption rides on the flag that already carries that meaning
  /// rather than on a second one that could drift away from it.
  /// (Everything a `DbasSqliteReader` does is also unregistered, for an
  /// unrelated reason — [_drainNativeOps] lists both exemptions.)
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
    NativeOpToken? nativeOp;
    try {
      if (takeWriterLock) {
        await _acquireWriterLock();
        lockHeld = true;
        // Registered on this path only — see the [takeWriterLock] docs.
        nativeOp = beginNativeOpInternal('checkpoint($context)');
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
      // waiter nobody will ever release it for. Same for the
      // registration, which only exists on the `takeWriterLock` path.
      if (nativeOp != null) endNativeOpInternal(nativeOp);
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
    // Registered after the lock is granted, not before: a caller still
    // parked on the FIFO queue has not reached native code, and
    // `closeDb` rejects it through `_cancelWriterWaitQueue` instead.
    final nativeOp = beginNativeOpInternal('beginTransaction');
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
    } finally {
      endNativeOpInternal(nativeOp);
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
    final nativeOp = beginNativeOpInternal('commit');
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
    } finally {
      // The recovery `rollback()` in the catch above registers its own
      // operation and ends it before this line is reached, so the two
      // nest cleanly rather than sharing a registration.
      endNativeOpInternal(nativeOp);
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
    // `closeDb` calls this BEFORE its native-operation drain, so the
    // registration here is always ended before that drain starts — it
    // can never wait on itself.
    final nativeOp = beginNativeOpInternal('rollback');
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
      endNativeOpInternal(nativeOp);
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
    // Assigned at the dispatch below, not here: everything before it is
    // a Dart-side wait, which `closeDb` cancels rather than drains.
    NativeOpToken? nativeOp;
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
      nativeOp = beginNativeOpInternal(
          nativeOpLabelInternal('executeScript', sql));
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
      if (nativeOp != null) endNativeOpInternal(nativeOp);
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
    final nativeOp = beginNativeOpInternal('vacuum');
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
      endNativeOpInternal(nativeOp);
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

  // ── Native-operation registry ────────────────────────────────────────

  /// Registers an operation that is crossing from Dart into NATIVE code
  /// and will hold native resources no other tracked owner can see — a
  /// checked-out pool reader, a live `sqlite3_stmt`, an in-flight worker
  /// dispatch on the writer.
  ///
  /// [closeDb] drains this registry ([_drainNativeOps]) before it folds
  /// the WAL and destroys the connection. **That drain is the only thing
  /// covering work which is already past every Dart-side gate.**
  /// [_closing] rejects operations that have not started, and
  /// [_cancelReaderSlotWaitQueue] / [_cancelWriterWaitQueue] reject the
  /// ones parked in Dart — but nothing on the Dart side can recall a
  /// call that is already inside the C pool.
  ///
  /// **This method deliberately does NOT reject while [_closing].** A
  /// blanket guard here would look like it makes [_drainNativeOps]'
  /// termination invariant true, but it would break teardown itself:
  /// [closeDb] latches [_closing] and then calls [rollback], which
  /// registers here. The guard belongs on the individual callers that can
  /// still acquire native resources after the drain has run —
  /// [setBusyTimeout] carries one for exactly that reason.
  ///
  /// [label] should name the call (and, for a statement, its SQL): it is
  /// what [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout] reports.
  ///
  /// **Pair every call with exactly one [endNativeOpInternal] in a
  /// `finally`, and un-register only once the operation's native
  /// resources have either been released or handed to an owner [closeDb]
  /// can find** — the tracked statement's `_activeReader` for a read
  /// that completed, the returned pool reader for one that bailed out.
  /// Un-registering any earlier reopens the exact window this registry
  /// exists to close.
  NativeOpToken beginNativeOpInternal(String label) {
    final id = ++_nativeOpSeq;
    _nativeOps[id] = _NativeOp(label);
    return (id: id);
  }

  /// Builds a registry [label] that carries the SQL as well as the verb,
  /// truncated because
  /// [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout] names every
  /// outstanding label and a script can be arbitrarily long. One shared
  /// derivation so every registered statement-bearing call reports the
  /// same way — the diagnostic has to say WHICH call never handed back,
  /// not just what kind, and [executeScript] (the DDL/migration door, and
  /// the most likely long-running native call there is) needs that most.
  static String nativeOpLabelInternal(String verb, String sql) {
    final trimmed = sql.length <= 80 ? sql : '${sql.substring(0, 77)}...';
    return '$verb($trimmed)';
  }

  /// Un-registers the operation [token] identifies and wakes any
  /// [_drainNativeOps] parked on it. An unknown token is ignored, so a
  /// double un-registration is a harmless no-op.
  ///
  /// No epoch match here, unlike [endReentrantWriterOpInternal]: this
  /// registry is not scoped to a transaction and ids are never reused,
  /// so a late un-registration can only ever match the one operation it
  /// was handed out for.
  void endNativeOpInternal(NativeOpToken token) {
    final op = _nativeOps.remove(token.id);
    if (op == null || op.done.isCompleted) return;
    op.done.complete();
  }

  /// Number of operations currently registered as in flight inside
  /// native code. Test-only seam: the invariant this release rests on is
  /// that [closeDb] reaches its destructive `closePool` / `closeDb`
  /// dispatch only once this is zero, and there is no other way to
  /// observe that from outside.
  @visibleForTesting
  int get debugInFlightNativeOpCount => _nativeOps.length;

  /// Waits until nothing is inside native code any more.
  ///
  /// **Loops rather than awaiting one snapshot**, for the same reason
  /// [_drainReentrantWriterOps] does: an operation that hands back can
  /// let another one through, and draining "everything that was in
  /// flight when we started" would leave exactly the one that arrived
  /// last un-drained. Each operation clears itself from [_nativeOps] as
  /// it ends, so the loop makes strict progress.
  ///
  /// A caller resuming from the operation that just handed back wins the
  /// race against [closeDb]'s statement sweep, so an `executeReader` that
  /// returned successfully during teardown delivers an OPEN reader and
  /// the sweep closes it afterwards. That falls out of the shapes rather
  /// than being enforced: waking this drain costs strictly more hops
  /// (`Future.wait` → deadline → loop → return → [closeDb] resumes) than
  /// returning through `executeReader` does. It is a real property — a
  /// reader closed before its caller's first `readRow()` would report an
  /// empty result set with no error at all — so it is pinned by a test
  /// rather than left to be rediscovered; a change that inverts the
  /// ordering fails there instead of silently shipping.
  ///
  /// **Two bodies of native work deliberately stay out of this registry,
  /// and both are covered by something else — a third would not be.**
  ///
  ///   1. The PASSIVE checkpoint [closeDb] runs on its way out
  ///      (`_checkpointBeforeRawFileAccess(takeWriterLock: false)`) is
  ///      reached only *after* this drain, so registering it would make
  ///      teardown wait on itself, forever. (The `takeWriterLock: true`
  ///      form is a normal caller and DOES register.)
  ///   2. Everything a [DbasSqliteReader] does: `readRow`'s steps against
  ///      a live `sqlite3_stmt`, and the `finalizeStmt` +
  ///      `poolReleaseReader` its `onClose` runs. It stays out because a
  ///      reader IS a tracked owner — the statement sweep below reaches
  ///      it through its statement's `_activeReader` and closes it, and
  ///      `DbasSqliteReader.close` waits for its own outstanding steps
  ///      before letting `onClose` finalize the handle.
  ///
  ///      **That cover holds only because both closes are
  ///      join-idempotent.** `close()` latches `isClosed` synchronously
  ///      and then suspends, so for a stretch of its life a reader that
  ///      still owns a checked-out pool connection already reports itself
  ///      closed. A sweep that read that flag as "nothing to do" would
  ///      skip exactly the case this exemption claims to cover, and
  ///      nothing else is waiting for that work — see
  ///      [DbasSqliteStatement.close].
  ///
  ///      Registering per row would also make the drain block for a whole
  ///      table scan rather than for work already in native code: an open
  ///      cursor would register a fresh operation for every row, and this
  ///      loop only ends when the registry is empty.
  ///
  /// **Termination is not assumed, and is not implied by [_closing]
  /// either.** Several sites still register ABOVE any `_closing` check,
  /// so a new operation genuinely can appear after this drain has
  /// started. Rather than an exhaustive count — the previous one was
  /// wrong twice — the rule is: **every REENTRANT (in-transaction) writer
  /// branch registers without a `_closing` check**, because the check
  /// lives in [_acquireWriterLock] and those branches deliberately do not
  /// take it. That is `DbasSqliteStatement._executeSqlNative` and
  /// [executeScript] on their `lockHeld == true` paths, plus [commit],
  /// which is gated on `_isInTransaction` + `isOpened()` +
  /// `_assertNoInFlightWriterUsers()` and on nothing else. All three stay
  /// reachable in the window between [closeDb] latching [_closing] and
  /// its `rollback()` clearing `_isInTransaction`. `executeReader`
  /// registers above its own gate too — synchronously at entry,
  /// deliberately above its routing branch — though the slot/lock gate
  /// rejects it immediately afterwards and its `finally` clears the
  /// registration.
  ///
  /// The two callers that reach native code with no second gate at all
  /// both carry an explicit [_closing] guard for exactly that reason:
  /// [setBusyTimeout] and [enableWal].
  ///
  /// So this LOOPS instead of awaiting one snapshot, and it makes
  /// progress rather than terminating by construction: each operation
  /// clears itself from [_nativeOps] as it ends. What bounds the whole
  /// thing is [kNativeOpDrainTimeoutMs] ([debugNativeOpDrainTimeoutMs] in
  /// tests) — nothing else.
  ///
  /// **On expiry it THROWS**
  /// [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout], naming the
  /// outstanding labels, and teardown stops there. Logging and
  /// proceeding is not an option and is not a milder one: the very next
  /// steps free the connection those operations are still using, so
  /// continuing IS the use-after-free. Failing the close leaks a handle,
  /// which is strictly better — the process stays alive, the error names
  /// what wedged it, and [closeDb] can simply be called again once the
  /// outstanding work finishes.
  Future<void> _drainNativeOps() async {
    if (_nativeOps.isEmpty) return;
    final timeoutMs = debugNativeOpDrainTimeoutMs ?? kNativeOpDrainTimeoutMs;
    final elapsed = Stopwatch()..start();
    while (true) {
      final pending = <Future<void>>[
        for (final op in _nativeOps.values) op.done.future,
      ];
      if (pending.isEmpty) return;
      final remaining = timeoutMs - elapsed.elapsedMilliseconds;
      if (remaining <= 0) throw _nativeOpDrainTimeout(timeoutMs);
      try {
        await Future.wait(pending).timeout(Duration(milliseconds: remaining));
      } on TimeoutException {
        throw _nativeOpDrainTimeout(timeoutMs);
      }
    }
  }

  /// The [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout] failure, built
  /// from whatever is still registered at the moment it is raised.
  DbasSqliteException _nativeOpDrainTimeout(int timeoutMs) {
    // Each label carries how long THAT operation has been in flight: an
    // age close to the whole window means one call never handed back,
    // while a set of young ages means operations kept arriving and the
    // drain never converged. Same message, two very different bugs.
    final outstanding = _nativeOps.values
        .map((op) => '${op.label} [in flight ${op.elapsed.elapsedMilliseconds}ms]')
        .join('; ');
    return DbasSqliteException.dart(
      DbasSqliteErrorCode.closeDbNativeOpDrainTimeout,
      'closeDb("$dbName") timed out after ${timeoutMs}ms waiting for '
      '${_nativeOps.length} operation(s) still inside native code: '
      '$outstanding. Teardown was ABORTED rather than continued — the '
      'next step destroys the connection those operations are still '
      'using, so proceeding would free memory they are about to touch. '
      'The connection is left open and still marked closing; await every '
      'in-flight call, then call closeDb() again.',
    );
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
  ///
  /// Falls back to [_closingPoolPtr] when [_poolPtr] is already gone:
  /// during [closeDb]'s `closePool` dispatch the pool is still alive and
  /// this call is precisely what it is blocked waiting for. See
  /// [_closingPoolPtr].
  void releaseReaderConnectionInternal(int readerPtr) {
    final poolPtr = _releasablePoolPtr;
    if (poolPtr == null) return;
    _platform.poolReleaseReader(dbName, poolPtr, readerPtr);
    _releaseReaderSlot();
  }

  /// The pool pointer a release may still be routed through right now:
  /// [_poolPtr] while the database is open, [_closingPoolPtr] while
  /// [closeDb]'s destructive dispatch is in flight, `null` once the pool
  /// is really gone.
  ///
  /// **One derivation, read by both the production path and the test
  /// seam** — see [debugReleasablePoolPtr]. Written out twice, the seam
  /// would only ever re-derive the same expression, so an assertion on it
  /// would prove a pointer was RETAINED and never that the release
  /// CONSULTS it: dropping the fallback from
  /// [releaseReaderConnectionInternal] alone would leave every test
  /// green. Sharing the member is what makes that revert reddens a test.
  int? get _releasablePoolPtr => _poolPtr ?? _closingPoolPtr;

  /// The pool pointer [releaseReaderConnectionInternal] would use right
  /// now — literally the same [_releasablePoolPtr] that method reads,
  /// never a copy of its expression.
  ///
  /// Test-only seam. The property it exists for — "a reader release
  /// arriving while `ClosePool` is blocked can still reach the pool" —
  /// has no other observable, and the only behavioural way to check it
  /// is to let a release be dropped and watch teardown wedge, which
  /// fails a suite by hanging it rather than by failing an assertion.
  @visibleForTesting
  int? get debugReleasablePoolPtr => _releasablePoolPtr;
}
