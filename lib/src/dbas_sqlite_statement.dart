import 'dart:async';
import 'dart:developer' as developer;

import 'package:decimal/decimal.dart';
import 'package:flutter/foundation.dart';

import 'package:dbas_sqlite/src/dbas_sqlite.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_db.dart'
    if (dart.library.js_interop) 'package:dbas_sqlite/src/stub/dbas_sqlite_db_stub.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_platform.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_reader.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_row_cache.dart';
import 'package:dbas_sqlite/src/exceptions/dbas_sqlite_exception.dart';

/// A prepared SQL statement — **exactly one** statement.
///
/// Owns its own bind buffers (positional + named) and dispatches
/// execution through the platform layer. Use [DbasSqlite.prepareQuery]
/// to obtain one — never construct directly.
///
/// **One statement per object, and the limit is silent.** The whole
/// lifecycle is one `sqlite3_prepare_v2` / one step / one
/// `sqlite3_finalize`, and the C layer passes the prepare a `nullptr`
/// tail pointer, so everything after the first `;` in the SQL is
/// discarded before SQLite ever sees it — with no rc, no exception and
/// no log. For a multi-statement script use [DbasSqlite.executeScript].
///
/// The native handle is allocated lazily at execute time on the
/// connection appropriate for the execution mode (writer for
/// [executeSql], pool reader for [executeReader] outside transactions,
/// writer inside transactions). Bind values are buffered Dart-side
/// and replayed onto the freshly-prepared handle on every execute,
/// so the same statement object can be reused with new values.
///
/// On failure, the bind buffers are preserved so the caller can fix
/// one slot and retry without re-binding everything else.
///
/// Closing the statement closes any active reader. Closing the owning
/// [DbasSqlite] auto-closes every still-open statement.
class DbasSqliteStatement {
  /// Test-only rendezvous inside `executeReader`'s prepare window —
  /// after the pool reader and the native statement handle have been
  /// acquired, but before ownership is transferred to the
  /// [DbasSqliteReader] that `_activeReader` points at. `null` in
  /// production; the awaited call is the only cost when it is set.
  ///
  /// Exists because that window is the one stretch of `executeReader`
  /// no other seam can observe: the read is already holding native
  /// resources, yet `_activeReader` is still `null`, so [close] — and
  /// therefore `closeDb`'s statement sweep — sees nothing to await.
  /// Reset it to `null` in a `finally` / `addTearDown`; it is static,
  /// so a leaked hook would park every later test's first read.
  @visibleForTesting
  static Future<void> Function()? debugBeforeReaderTransfer;

  final DbasSqlite _db;
  final DbasSqlitePlatform _platform;
  final String _sql;

  List<Object?> _positionalBinds = [];
  Map<String, Object?> _namedBinds = {};

  DbasSqliteReader? _activeReader;
  bool _closed = false;
  int _lastAffectedRows = -1;
  int _lastInsertedId = -1;
  String? _lastError;
  int? _lastErrorCode;
  int? _lastUniqueErrorCode;

  /// Whether [_activeReader] is bound to the writer connection. Written
  /// together with [_activeReader] so the pair can never disagree; read
  /// through [hasOpenWriterReaderInternal].
  bool _activeReaderUsesWriter = false;

  /// Internal — see [DbasSqlite.prepareQuery].
  DbasSqliteStatement.internal(
    this._db,
    this._platform,
    this._sql,
  );

  /// The SQL text used to prepare this statement.
  String get sql => _sql;

  /// `true` after [close] has been called or after the database
  /// closed and invalidated this statement.
  bool get isClosed => _closed;

  /// `true` while this statement has a reader open **and** that reader
  /// is bound to the writer connection — the case where a `COMMIT`
  /// would end the transaction, and hand the writer lock on, underneath
  /// a live cursor. `false` for readers bound to a pool connection (a
  /// WAL pool read doesn't touch the writer connection) and whenever no
  /// reader is open on this statement.
  ///
  /// Internal seam for [DbasSqlite.commit]'s pre-flight check. Safe for
  /// it to reach this only through `_activeStatements`: a statement is
  /// removed from that set exactly once, on the last line of [close],
  /// and [close] first awaits `reader.close()` — which flips the
  /// reader's `isClosed` synchronously — so this getter is already
  /// `false` before the statement can leave the set.
  bool get hasOpenWriterReaderInternal =>
      _activeReader != null &&
      !_activeReader!.isClosed &&
      _activeReaderUsesWriter;

  // ── Bindings (positional, fluent) ────────────────────────────────────

  DbasSqliteStatement bindNull(int index) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = null;
    return this;
  }

  DbasSqliteStatement bindBool(int index, bool value) =>
      bindInt(index, value ? 1 : 0);

  DbasSqliteStatement bindInt(int index, int value) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = value;
    return this;
  }

  DbasSqliteStatement bindFloat(int index, double value) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = value;
    return this;
  }

  DbasSqliteStatement bindDouble(int index, double value) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = value;
    return this;
  }

  DbasSqliteStatement bindDecimal(int index, Decimal value) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = value;
    return this;
  }

  DbasSqliteStatement bindText(int index, String value) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = value;
    return this;
  }

  DbasSqliteStatement bindBlob(int index, Uint8List value) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = value;
    return this;
  }

  DbasSqliteStatement bindEnum(int index, Enum value) {
    _ensurePositionalSlot(index);
    _positionalBinds[index - 1] = value;
    return this;
  }

  /// Replaces the entire positional buffer.
  DbasSqliteStatement bindParameters(List<Object?> params) {
    _positionalBinds = List.of(params);
    return this;
  }

  void _ensurePositionalSlot(int index) {
    while (_positionalBinds.length < index) {
      _positionalBinds.add(null);
    }
  }

  // ── Bindings (named, fluent) ─────────────────────────────────────────

  DbasSqliteStatement bindNameNull(String name) {
    _namedBinds[name] = null;
    return this;
  }

  DbasSqliteStatement bindNameBool(String name, bool value) =>
      bindNameInt(name, value ? 1 : 0);

  DbasSqliteStatement bindNameInt(String name, int value) {
    _namedBinds[name] = value;
    return this;
  }

  DbasSqliteStatement bindNameFloat(String name, double value) {
    _namedBinds[name] = value;
    return this;
  }

  DbasSqliteStatement bindNameDouble(String name, double value) {
    _namedBinds[name] = value;
    return this;
  }

  DbasSqliteStatement bindNameDecimal(String name, Decimal value) {
    _namedBinds[name] = value;
    return this;
  }

  DbasSqliteStatement bindNameText(String name, String value) {
    _namedBinds[name] = value;
    return this;
  }

  DbasSqliteStatement bindNameBlob(String name, Uint8List value) {
    _namedBinds[name] = value;
    return this;
  }

  DbasSqliteStatement bindNameEnum(String name, Enum value) {
    _namedBinds[name] = value;
    return this;
  }

  /// Replaces the entire named buffer.
  ///
  /// **Note:** at execute time, named parameters that don't appear in
  /// the prepared SQL are silently skipped (SQLITE_RANGE), matching
  /// `Microsoft.Data.Sqlite` behaviour. Set
  /// [DbasSqlite.throwOnMissingNamedParams] to `true` to convert
  /// missing names into an exception.
  DbasSqliteStatement bindNameParameters(Map<String, Object?> params) {
    _namedBinds = Map.of(params);
    return this;
  }

  // ── Execution: SQL (write / DDL / DML) ───────────────────────────────

  /// Executes the prepared statement as DML/DDL. Returns affected
  /// rows. Pass [params] / [nameParams] to replace the bind buffer
  /// before execution (mirroring the v2.3.x convenience shape).
  ///
  /// **Runs the FIRST statement of the SQL and nothing else.** One
  /// prepare, one step, one finalize — anything after the first `;` was
  /// already dropped at prepare time and this call reports success
  /// regardless. If the SQL can hold more than one statement, this is
  /// the wrong method: use [DbasSqlite.executeScript], which routes to
  /// `sqlite3_exec` and runs the whole script.
  ///
  /// The Dart-side bind buffer is preserved on failure — fix the
  /// offending value and call again without re-binding the rest.
  Future<int> executeSql({
    List<Object?>? params,
    Map<String, Object?>? nameParams,
  }) async {
    _checkUsable();
    // Snapshot the buffers so a thrown execute restores the previous
    // bind state — honouring the docstring promise that the caller
    // can fix one slot and retry without re-binding everything.
    final positionalSnapshot = List<Object?>.of(_positionalBinds);
    final namedSnapshot = Map<String, Object?>.of(_namedBinds);
    if (params != null) _positionalBinds = List.of(params);
    if (nameParams != null) _namedBinds = Map.of(nameParams);

    try {
      return await _executeSqlNative();
    } catch (_) {
      _positionalBinds = positionalSnapshot;
      _namedBinds = namedSnapshot;
      rethrow;
    }
  }

  Future<int> _executeSqlNative() async {
    // Reset error state up-front so the post-call accessors reflect
    // ONLY this execute. Mirrors the reader path's onClose, which
    // overwrites these fields when the iteration ends.
    _lastError = null;
    _lastErrorCode = null;
    _lastUniqueErrorCode = null;

    // `lockHeld` distinguishes two very different callers:
    //   - Outside a transaction: this call must acquire the writer lock
    //     itself — nobody else is holding the writer connection for us.
    //   - Inside a transaction: `beginTransaction` already holds the
    //     writer lock for the transaction's whole lifetime, so this call
    //     must NOT re-acquire it (the queue is FIFO — it would queue
    //     behind itself and deadlock). It registers as a REENTRANT user
    //     instead, so `commit()` can see that ending the transaction
    //     right now would hand the writer lock to the next FIFO waiter
    //     while this call's dispatch (prepare / bind / step / finalize)
    //     is still running on the connection. See [DbasSqlite.commit].
    final lockHeld = _db.isInTransaction;
    ReentrantWriterOpToken? reentrantOp;
    if (lockHeld) {
      reentrantOp = _db.beginReentrantWriterOpInternal();
    } else {
      await _db.acquireWriterLockInternal();
    }
    // Mark the transaction dirty up-front. Subsequent reads in the same
    // tx must route through the writer connection to observe this
    // statement's effects (read-your-writes). Setting before dispatch
    // is conservative: if the statement fails, later reads still go
    // through the writer — slower but never incorrect.
    _db.markTransactionWriteInternal();

    // Ordering is load-bearing: start the dispatch chain WITHOUT
    // awaiting it, so its still-pending future can be handed to the
    // database before this method suspends. `rollback()` DRAINS that
    // future instead of issuing ROLLBACK on top of it — a write whose
    // step lands after the ROLLBACK would otherwise commit itself in
    // autocommit mode and survive the rollback silently. See
    // [DbasSqlite.rollback]. Both calls run in this same synchronous
    // turn, so no other flow can observe an untracked registration.
    //
    // The native-operation registration joins that same synchronous
    // turn. It is deliberately NOT taken around the writer-lock acquire
    // above: a caller merely parked on the Dart-side FIFO queue has not
    // reached native code, and `closeDb` rejects it through
    // `_cancelWriterWaitQueue` rather than waiting for it.
    final nativeOp = _db.beginNativeOpInternal(_nativeOpLabel('executeSql'));
    final dispatch = _executeSqlDispatch();
    if (reentrantOp != null) {
      _db.trackReentrantWriterOpDispatchInternal(reentrantOp, dispatch);
    }
    try {
      return await dispatch;
    } finally {
      if (reentrantOp != null) {
        _db.endReentrantWriterOpInternal(reentrantOp);
      } else {
        _db.releaseWriterLockInternal();
      }
      _db.endNativeOpInternal(nativeOp);
    }
  }

  /// The prepare / bind / step / finalize dispatch chain behind
  /// [executeSql]. Split out of [_executeSqlNative] so that method can
  /// hand this still-pending future to the owning [DbasSqlite] before it
  /// suspends — see [_executeSqlNative] for why that ordering matters.
  /// Owns no lock and no registration: the caller does all of that.
  Future<int> _executeSqlDispatch() async {
    final conn = _db.dbInternal!;
    int handle = sqliteInvalidStmtHandle;
    try {
      final prepared = await _platform.prepareQuery(conn, _sql);
      handle = prepared.handle;
      if (handle == sqliteInvalidStmtHandle) {
        final err = _platform.getLastDbError(conn) ?? 'Unknown error.';
        // sqlite3_prepare_v2 failures DO populate sqlite3_extended_errcode
        // (and the web shim caches the worker-reported rc into the same
        // platform accessors). Surface both codes when available; only
        // fall through to `.dart()` when neither is resolvable.
        final primary = _platform.getErrorCode(conn);
        if (primary != null) {
          throw DbasSqliteException.sqlite(
            DbasSqliteErrorCode.executeSqlPrepareFailed,
            'It was not possible to prepare the query: $err',
            sqliteCode: primary,
            sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
          );
        }
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.executeSqlPrepareFailed,
          'It was not possible to prepare the query: $err',
        );
      }

      try {
        await _replayBinds(conn, handle);

        final cache = RowData();
        final rc = await _platform.readRowAndCache(conn, handle, cache);
        if (rc != sqliteOk && rc != sqliteRow && rc != sqliteDone) {
          final err = _platform.getLastStmtError(conn, handle) ??
              'Unknown error ($rc).';
          final primary = _platform.getErrorCode(conn) ?? rc;
          throw DbasSqliteException.sqlite(
            DbasSqliteErrorCode.executeSqlStepFailed,
            'It was not possible to run the query ($rc): $err',
            sqliteCode: primary,
            sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
          );
        }

        // Counters MUST be read BEFORE finalize. After finalize the
        // handle is removed from the C lib's liveStmts map and any
        // subsequent stmt-scoped accessor returns the stale-handle
        // sentinel (-1).
        _lastAffectedRows = _platform.getStmtAffectedRows(conn, handle);
        _lastInsertedId = _platform.getStmtLastInsertedId(conn, handle);
        return rc == sqliteRow ? 0 : _lastAffectedRows;
      } finally {
        if (handle != sqliteInvalidStmtHandle) {
          try {
            await _platform.finalizeStmt(conn, handle);
          } catch (e, st) {
            developer.log(
              'finalizeStmt failed during executeSql cleanup',
              name: 'dbas_sqlite.DbasSqliteStatement',
              error: e,
              stackTrace: st,
            );
          }
        }
      }
    } on DbasSqliteException catch (e) {
      // Capture the rcs onto the statement so post-failure callers
      // of getLastErrorCode / getLastUniqueErrorCode see the same
      // codes that are on the thrown exception, mirroring the
      // reader path's onClose behaviour.
      _lastError = e.message;
      _lastErrorCode = e.sqliteCode;
      _lastUniqueErrorCode = e.sqliteUniqueCode;
      rethrow;
    }
  }

  Future<void> _replayBinds(DbasSqliteDb conn, int handle) async {
    for (int i = 0; i < _positionalBinds.length; i++) {
      final index = i + 1;
      final value = _positionalBinds[i];
      final rc = await _bindPositional(conn, handle, index, value);
      if (rc != sqliteOk) {
        final err = _platform.getLastStmtError(conn, handle) ??
            'Bind failed at positional index $index ($rc).';
        final primary = _platform.getErrorCode(conn) ?? rc;
        throw DbasSqliteException.sqlite(
          DbasSqliteErrorCode.bindPositionalFailed,
          'Bind failed at positional index $index: $err',
          sqliteCode: primary,
          sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
        );
      }
    }
    for (final entry in _namedBinds.entries) {
      String name = entry.key;
      if (!name.startsWith(':') && !name.startsWith('@') && !name.startsWith(r'$')) {
        name = ':$name';
      }
      final rc = await _bindNamed(conn, handle, name, entry.value);
      if (rc == sqliteRange) {
        if (_db.throwOnMissingNamedParams) {
          final primary = _platform.getErrorCode(conn) ?? rc;
          throw DbasSqliteException.sqlite(
            DbasSqliteErrorCode.bindNamedParameterNotFound,
            "Named parameter '$name' not found in the prepared statement",
            sqliteCode: primary,
            sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
          );
        }
        continue;
      }
      if (rc != sqliteOk) {
        final err = _platform.getLastStmtError(conn, handle) ??
            'Bind failed for named parameter "$name" ($rc).';
        final primary = _platform.getErrorCode(conn) ?? rc;
        throw DbasSqliteException.sqlite(
          DbasSqliteErrorCode.bindNamedFailed,
          'Bind failed for "$name": $err',
          sqliteCode: primary,
          sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
        );
      }
    }
  }

  Future<int> _bindPositional(
      DbasSqliteDb conn, int handle, int index, Object? value) {
    if (value == null) return _platform.bindNull(conn, handle, index);
    if (value is bool) {
      return _platform.bindInt(conn, handle, index, value ? 1 : 0);
    }
    if (value is int) {
      // Route through Int64 for values outside Int32 range.
      if (value > 0x7fffffff || value < -0x80000000) {
        return _platform.bindInt64(conn, handle, index, value);
      }
      return _platform.bindInt(conn, handle, index, value);
    }
    if (value is double) return _platform.bindDouble(conn, handle, index, value);
    if (value is Decimal) {
      return _platform.bindText(conn, handle, index, value.toString());
    }
    if (value is String) return _platform.bindText(conn, handle, index, value);
    if (value is Uint8List) {
      return _platform.bindBlob(conn, handle, index, value);
    }
    if (value is List<int>) {
      return _platform.bindBlob(conn, handle, index, value);
    }
    if (value is Enum) {
      return _platform.bindInt(conn, handle, index, value.index);
    }
    throw DbasSqliteException.dart(
      DbasSqliteErrorCode.unsupportedPositionalBindType,
      'Unsupported type to SQLite bind: ${value.runtimeType}',
    );
  }

  Future<int> _bindNamed(
      DbasSqliteDb conn, int handle, String name, Object? value) {
    if (value == null) return _platform.bindNameNull(conn, handle, name);
    if (value is bool) {
      return _platform.bindNameInt(conn, handle, name, value ? 1 : 0);
    }
    if (value is int) {
      if (value > 0x7fffffff || value < -0x80000000) {
        return _platform.bindNameInt64(conn, handle, name, value);
      }
      return _platform.bindNameInt(conn, handle, name, value);
    }
    if (value is double) {
      return _platform.bindNameDouble(conn, handle, name, value);
    }
    if (value is Decimal) {
      return _platform.bindNameText(conn, handle, name, value.toString());
    }
    if (value is String) {
      return _platform.bindNameText(conn, handle, name, value);
    }
    if (value is Uint8List) {
      return _platform.bindNameBlob(conn, handle, name, value);
    }
    if (value is List<int>) {
      return _platform.bindNameBlob(conn, handle, name, value);
    }
    if (value is Enum) {
      return _platform.bindNameInt(conn, handle, name, value.index);
    }
    throw DbasSqliteException.dart(
      DbasSqliteErrorCode.unsupportedNamedBindType,
      'Unsupported type to SQLite named bind: ${value.runtimeType}',
    );
  }

  // ── Execution: reader ────────────────────────────────────────────────

  /// Executes the prepared statement as a SELECT and returns a
  /// [DbasSqliteReader] for row-by-row iteration.
  ///
  /// Routing is automatic and consistent across native and web. On
  /// both platforms each row is fetched lazily — native uses the
  /// FFI prepare/step/finalize lifecycle, web uses the worker's
  /// `prepareQuery` / `bindParams` / `readRow` / `readRows` /
  /// `finalizeStmt` RPC chain. `executeScalar` therefore issues
  /// exactly one `step` / `readRow` regardless of how many rows the
  /// SQL would otherwise produce.
  ///
  ///   - **Outside a transaction**: native uses a pool reader. Web's
  ///     pool returns the same single worker connection for any
  ///     reader-acquire (the web pool fronts one worker, no separate
  ///     reader workers), so the same routing logic resolves to that
  ///     connection.
  ///   - **Inside a transaction, before any `executeSql` runs**: same
  ///     as outside — pool reader on native, the writer worker on
  ///     web. Parallel `Future.wait([executeReader, ...])` issued
  ///     before the first write runs concurrently on native against
  ///     independent pool connections; on web the worker serialises
  ///     them through its single connection.
  ///   - **Inside a transaction, after any `executeSql` runs**: routes
  ///     through the writer connection (native) or the writer worker
  ///     (web), so the read observes the in-flight transaction's
  ///     uncommitted writes (read-your-writes).
  ///
  /// The "after any `executeSql`" detection is automatic: every
  /// [executeSql] flips an internal flag on the owning [DbasSqlite]
  /// for the rest of the transaction; `commit` / `rollback` reset it.
  ///
  /// Only one reader may be active per statement at a time. Throws a
  /// [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.readerAlreadyActive] if a reader is already
  /// active.
  Future<DbasSqliteReader> executeReader({
    List<Object?>? params,
    Map<String, Object?>? nameParams,
  }) async {
    _checkUsable();
    if (_activeReader != null && !_activeReader!.isClosed) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.readerAlreadyActive,
        'A reader from this statement is still active.',
      );
    }
    // Snapshot for restore-on-failure — same rationale as executeSql.
    final positionalSnapshot = List<Object?>.of(_positionalBinds);
    final namedSnapshot = Map<String, Object?>.of(_namedBinds);
    if (params != null) _positionalBinds = List.of(params);
    if (nameParams != null) _namedBinds = Map.of(nameParams);

    try {
      return await _executeReaderNative();
    } catch (_) {
      _positionalBinds = positionalSnapshot;
      _namedBinds = namedSnapshot;
      rethrow;
    }
  }

  Future<DbasSqliteReader> _executeReaderNative() async {
    // Register with the connection-wide native-operation registry HERE:
    // synchronously, before the first `await`, and — the load-bearing
    // part — before the routing decision immediately below.
    //
    // Everything after this line acquires native resources that no other
    // tracked owner can see until `_activeReader` is assigned near the
    // end: a checked-out pool reader (or the writer connection) plus a
    // live `sqlite3_stmt`. A `closeDb()` arriving while this method is
    // suspended anywhere in that window finds `_activeReader == null`,
    // so `close()` awaits nothing and the statement sweep disowns the
    // statement outright — after which the POOL route deadlocks
    // (`ClosePool` waits on a reader nobody will release) and the WRITER
    // route corrupts memory (the writer is not checkout-tracked, so
    // `ClosePool` force-closes it under this live handle). ONE
    // registration above the branch is what covers both by construction;
    // registering per-route would leave whichever route was written
    // second silently uncovered. See [DbasSqlite.beginNativeOpInternal].
    final nativeOp = _db.beginNativeOpInternal(_nativeOpLabel('executeReader'));
    try {
      return await _executeReaderRouted();
    } finally {
      // Un-registered LAST — after the inner `finally` has either handed
      // ownership to `_activeReader` (which `closeDb`'s statement sweep
      // can find and close) or unwound everything it acquired. Ending it
      // any earlier would let teardown proceed over a pool reader that
      // has not been returned yet.
      _db.endNativeOpInternal(nativeOp);
    }
  }

  /// Registry label for an in-flight native operation on this statement.
  /// Carries the SQL — truncated, since
  /// [DbasSqliteErrorCode.closeDbNativeOpDrainTimeout] names every
  /// outstanding label and a script can be arbitrarily long — so the
  /// diagnostic says WHICH call never handed back, not just what kind.
  String _nativeOpLabel(String verb) {
    final sql = _sql.length <= 80 ? _sql : '${_sql.substring(0, 77)}...';
    return '$verb($sql)';
  }

  /// The connection-routing, prepare, bind and reader-handoff body of
  /// [executeReader]. Split out of [_executeReaderNative] so the
  /// native-operation registration can wrap it whole — including the
  /// routing decision, which is what makes the pool and writer routes
  /// covered by the same registration.
  Future<DbasSqliteReader> _executeReaderRouted() async {
    // Use the writer connection only after a write has happened in the
    // current transaction (read-your-writes). Before any writes — or
    // outside a transaction — go through the pool so parallel reads
    // can run on independent connections. Single-connection mode (no
    // pool) inside a transaction also stays on the writer because the
    // writer lock is already held by `beginTransaction`; trying to
    // re-acquire it via the pool-reader fallback would deadlock.
    final useWriter = _db.isInTransaction &&
        (_db.transactionHasWritesInternal || _db.poolPtrInternal == null);
    final DbasSqliteDb conn;
    final Future<void> Function() releaseFn;
    ReentrantWriterOpToken? reentrantOp;

    if (useWriter) {
      conn = _db.dbInternal!;
      releaseFn = () async {};
      // Reentrant, exactly like the write path in `_executeSqlNative` —
      // deliberately skips `acquireWriterLockInternal` because
      // `beginTransaction` already holds the lock. Register the in-flight
      // dispatch NOW, before a `DbasSqliteReader` exists for `commit()`'s
      // pre-flight to find via `hasOpenWriterReaderInternal`; the
      // `finally` below un-registers it once the reader has taken over,
      // so the two signals hand off with no gap.
      //
      // No dispatch future is tracked: `rollback()` drains WRITES only.
      // A read cannot persist anything past a ROLLBACK, so there is
      // nothing for a drain to protect — see [DbasSqlite.rollback].
      reentrantOp = _db.beginReentrantWriterOpInternal();
    } else if (_db.poolPtrInternal != null) {
      final timeout = _db.poolAcquireTimeoutMsInternal;
      final readerPtr = await _db.acquireReaderConnectionInternal(timeout);
      if (readerPtr == 0) {
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.executeReaderPoolAcquireTimeout,
          'No pool reader became available within ${timeout}ms — '
          'all readers busy. Close in-flight readers or raise '
          'DbasSqlite.kPoolAcquireTimeoutMs.',
        );
      }
      conn = DbasSqliteDb(_db.dbName, readerPtr);
      releaseFn = () async {
        _db.releaseReaderConnectionInternal(readerPtr);
      };
    } else {
      // Single-connection fallback: use writer with the writer lock.
      await _db.acquireWriterLockInternal();
      conn = _db.dbInternal!;
      releaseFn = () async {
        _db.releaseWriterLockInternal();
      };
    }

    int handle = sqliteInvalidStmtHandle;
    bool transferred = false;
    try {
      final prepared = await _platform.prepareQuery(conn, _sql);
      handle = prepared.handle;
      if (handle == sqliteInvalidStmtHandle) {
        final err = _platform.getLastDbError(conn) ?? 'Unknown error.';
        final primary = _platform.getErrorCode(conn);
        if (primary != null) {
          throw DbasSqliteException.sqlite(
            DbasSqliteErrorCode.executeReaderPrepareFailed,
            'It was not possible to prepare the query: $err',
            sqliteCode: primary,
            sqliteUniqueCode: _platform.getUniqueErrorCode(conn),
          );
        }
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.executeReaderPrepareFailed,
          'It was not possible to prepare the query: $err',
        );
      }

      await _replayBinds(conn, handle);

      // Test-only rendezvous — see [debugBeforeReaderTransfer].
      final beforeReaderTransfer = debugBeforeReaderTransfer;
      if (beforeReaderTransfer != null) await beforeReaderTransfer();

      final reader = DbasSqliteReader.internal(
        conn: conn,
        handle: handle,
        platform: _platform,
        // Pre-populate the reader's cache with column metadata
        // captured at prepare time so getColumnCount / getColumnName
        // work BEFORE the first readRow call.
        initialColumnCount: prepared.columnCount,
        initialColumnNames: prepared.columnNames,
        onClose: () async {
          // Order is load-bearing: read counters BEFORE finalize, then
          // release. We track the first error and log subsequent ones
          // so no failure is silently dropped if multiple steps fail.
          Object? firstErr;
          StackTrace? firstStack;
          try {
            _lastAffectedRows = _platform.getStmtAffectedRows(conn, handle);
            _lastInsertedId = _platform.getStmtLastInsertedId(conn, handle);
            _lastError = _platform.getLastStmtError(conn, handle);
            _lastErrorCode = _platform.getErrorCode(conn);
            _lastUniqueErrorCode = _platform.getUniqueErrorCode(conn);
          } catch (e, st) {
            firstErr = e;
            firstStack = st;
            developer.log(
              'reader onClose: counter read failed',
              name: 'dbas_sqlite.DbasSqliteStatement',
              error: e,
              stackTrace: st,
            );
          }
          try {
            await _platform.finalizeStmt(conn, handle);
          } catch (e, st) {
            firstErr ??= e;
            firstStack ??= st;
            developer.log(
              'reader onClose: finalizeStmt failed',
              name: 'dbas_sqlite.DbasSqliteStatement',
              error: e,
              stackTrace: st,
            );
          }
          try {
            await releaseFn();
          } catch (e, st) {
            firstErr ??= e;
            firstStack ??= st;
            developer.log(
              'reader onClose: releaseFn failed',
              name: 'dbas_sqlite.DbasSqliteStatement',
              error: e,
              stackTrace: st,
            );
          }
          _activeReader = null;
          if (firstErr != null) {
            Error.throwWithStackTrace(firstErr, firstStack!);
          }
        },
      );
      _activeReader = reader;
      _activeReaderUsesWriter = useWriter;
      transferred = true;
      return reader;
    } finally {
      // Hand off from the in-flight-dispatch signal to the open-reader
      // signal: by now `_activeReader` is set (or the bailout below
      // tears everything down), so `commit()`'s pre-flight keeps seeing
      // this writer-connection user without a gap.
      if (reentrantOp != null) _db.endReentrantWriterOpInternal(reentrantOp);
      // If we never got far enough to transfer ownership to a reader,
      // unwind everything we acquired in this scope. The primary
      // error is already in flight; cleanup failures are logged so
      // they don't go unnoticed but don't replace the original error.
      if (!transferred) {
        if (handle != sqliteInvalidStmtHandle) {
          try {
            await _platform.finalizeStmt(conn, handle);
          } catch (e, st) {
            developer.log(
              'finalizeStmt failed during executeReader bailout',
              name: 'dbas_sqlite.DbasSqliteStatement',
              error: e,
              stackTrace: st,
            );
          }
        }
        try {
          await releaseFn();
        } catch (e, st) {
          developer.log(
            'releaseFn failed during executeReader bailout',
            name: 'dbas_sqlite.DbasSqliteStatement',
            error: e,
            stackTrace: st,
          );
        }
      }
    }
  }

  // ── Execution: scalar ────────────────────────────────────────────────

  /// Executes the prepared statement as a SELECT and returns the value
  /// of the first column of the first row.
  ///
  /// Same input parameters and connection routing as [executeReader]
  /// (see its docs for in-/out-of-transaction semantics — including the
  /// automatic switch to read-your-writes after any [executeSql] runs
  /// in the current transaction).
  ///
  /// Returns `null` when the query produces no rows, or when the first
  /// column of the first row is SQL NULL. The returned dynamic is
  /// typed by the column's SQLite type: `int` for INTEGER, `double`
  /// for FLOAT, `String` for TEXT, [Uint8List] for BLOB.
  ///
  /// **`null` means "no row / SQL NULL", and nothing else.** This method
  /// runs one [DbasSqliteReader.readRow], so it inherits that method's
  /// teardown contract: if the reader is torn down between
  /// [executeReader] returning and that first `readRow` — a
  /// [DbasSqlite.closeDb] statement sweep, or a [close] on this
  /// statement from elsewhere — the `readRow` throws
  /// [DbasSqliteErrorCode.readerClosedDuringScan] and this method
  /// throws it on, instead of reporting the empty result `null` would
  /// claim. Before 2.9.0 that case returned `null`, indistinguishable
  /// from a genuinely empty query.
  ///
  /// Closes both the underlying reader and this statement before
  /// returning, so the statement is single-use — calling any execute
  /// method on it afterwards throws a [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.statementClosed].
  Future<dynamic> executeScalar({
    List<Object?>? params,
    Map<String, Object?>? nameParams,
  }) async {
    final reader = await executeReader(
      params: params,
      nameParams: nameParams,
    );
    try {
      if (!await reader.readRow()) return null;
      return reader.getColumnValue(0);
    } finally {
      // readRow() auto-closes when there are no more rows; close() is
      // idempotent so calling it unconditionally is safe and keeps the
      // happy path symmetric with the empty-result path.
      if (!reader.isClosed) await reader.close();
      await close();
    }
  }

  // ── Per-stmt state ───────────────────────────────────────────────────

  /// Affected rows from the most recent successful execute. -1 if the
  /// statement has never been successfully stepped.
  int getAffectedRows() => _lastAffectedRows;

  /// rowid of the most recent successful insert through this
  /// statement. -1 if never successfully stepped or the SQL is not
  /// an INSERT.
  int getLastInsertedId() => _lastInsertedId;

  /// Most recent statement-scoped error message. `null` when no
  /// error is pending.
  String? getLastError() => _lastError;

  /// SQLite **primary** result code captured from the most recent
  /// execute on this statement (e.g. `19` for `SQLITE_CONSTRAINT`, `5`
  /// for `SQLITE_BUSY`).
  ///
  /// Populated by both execution paths:
  ///   - `executeSql` / `executeScalar` write the codes that the
  ///     thrown [DbasSqliteException] carried, so a caller can read
  ///     them again after catching the exception.
  ///   - `executeReader` writes the connection's error state at
  ///     reader-close time alongside [getLastError] /
  ///     [getAffectedRows] / [getLastInsertedId].
  ///
  /// `null` when no error was observed (last execute succeeded, or
  /// the statement has never been executed).
  int? getLastErrorCode() => _lastErrorCode;

  /// SQLite **extended** result code captured from the most recent
  /// execute on this statement (e.g. `2067` for
  /// `SQLITE_CONSTRAINT_UNIQUE`, `787` for
  /// `SQLITE_CONSTRAINT_FOREIGNKEY`). Populated by the same paths as
  /// [getLastErrorCode]. `null` when the platform didn't queue an
  /// extended rc.
  int? getLastUniqueErrorCode() => _lastUniqueErrorCode;

  // ── Lifecycle ────────────────────────────────────────────────────────

  /// Closes any active reader, clears the bind buffers, and marks the
  /// statement closed. Idempotent. Subsequent execute calls throw a
  /// [DbasSqliteException] with code [DbasSqliteErrorCode.statementClosed].
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    final reader = _activeReader;
    if (reader != null && !reader.isClosed) {
      try {
        await reader.close();
      } catch (e, st) {
        developer.log(
          'reader.close failed during statement close',
          name: 'dbas_sqlite.DbasSqliteStatement',
          error: e,
          stackTrace: st,
        );
      }
    }
    _positionalBinds = const [];
    _namedBinds = const {};
    _db.unregisterStatementInternal(this);
  }

  void _checkUsable() {
    if (_closed) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.statementClosed,
        'Statement is closed.',
      );
    }
    if (!_db.isOpened()) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.statementDatabaseNotOpened,
        'Database is not opened.',
      );
    }
  }
}
