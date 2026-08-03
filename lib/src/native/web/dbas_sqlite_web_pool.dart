import 'dart:async';
import 'dart:developer' as developer;
import 'dart:js_interop';
import 'dart:typed_data';
import 'package:web/web.dart' as web;

import 'package:dbas_sqlite/src/dbas_sqlite_row_cache.dart';
import 'package:dbas_sqlite/src/stub/dbas_sqlite_db_stub.dart'
    show sqliteRow, sqliteDone;

import 'dbas_sqlite_web_columns.dart';

const _workerUrl = 'assets/packages/dbas_sqlite/web/libs/dbas_sqlite_worker.js';
// Relative to the worker script location (same directory), not the page root.
// importScripts() inside the worker resolves URLs relative to the worker URL.
const _libUrl = 'dbas_sqlite.js';

/// Raw property accessor for JS objects (avoids dartify on complex types
/// like ReadableStream that would lose their identity).
@JS()
extension type _JSObj._(JSObject _) implements JSObject {
  external JSAny? operator [](String key);
  external void operator []=(String key, JSAny? value);
}

/// **Internal** — implementation detail of the web shim. Carries the
/// SQLite rc / extended rc reported by the worker's `postErr` envelope.
///
/// Every error path through [DbasSqliteWebPool] rejects with one of
/// these. The only consumer is [DbasSqliteNativeWeb._captureError],
/// which downcasts to read the rcs and stores them on the public
/// [DbasSqliteException]. The type is not exported from the package
/// barrel and never escapes the platform boundary (the web shim's
/// `executeSql` returns a non-OK rc on failure rather than rethrowing).
class DbasSqliteWebWorkerError implements Exception {
  /// Human-readable error text from the worker's `message` field. The
  /// worker's structured error `code` (e.g. `'WORKER_CRASHED'`,
  /// `'SQLITE_BUSY'`) is folded in by [_workerErrorFromJsError] when
  /// present, prefixed in square brackets, so this string carries both
  /// the symbolic kind and the human message.
  final String message;

  /// SQLite **primary** result code reported as the `rc` field of the
  /// worker's `postErr` envelope (e.g. 19 for `SQLITE_CONSTRAINT`,
  /// 5 for `SQLITE_BUSY`). `null` when the worker omitted it —
  /// typically a Dart-side / protocol failure that never reached the
  /// C library.
  final int? sqliteCode;

  /// SQLite **extended** result code reported as the `extendedRc`
  /// field of the worker's `postErr` envelope (e.g. 2067 for
  /// `SQLITE_CONSTRAINT_UNIQUE`). `null` when the worker omitted it
  /// or when the C library reported only the primary rc.
  final int? sqliteUniqueCode;

  DbasSqliteWebWorkerError(this.message,
      {this.sqliteCode, this.sqliteUniqueCode});

  @override
  String toString() {
    if (sqliteCode == null && sqliteUniqueCode == null) return message;
    final buf = StringBuffer(message)..write(' [');
    if (sqliteCode != null) buf.write('rc=$sqliteCode');
    if (sqliteUniqueCode != null) {
      if (sqliteCode != null) buf.write(', ');
      buf.write('extendedRc=$sqliteUniqueCode');
    }
    buf.write(']');
    return buf.toString();
  }
}

/// Builds a [DbasSqliteWebWorkerError] from the dartified `err` map
/// returned in the worker's `error` envelope. The map shape is
/// `{code, message, rc?, extendedRc?}` per `postErr` in the worker.
/// The structured `code` (e.g. `'WORKER_CRASHED'`, `'POOL_CLOSED'`)
/// is prefixed onto the message in `[code] ` form so callers don't
/// lose the symbolic kind. Falls back to `err.toString()` when the
/// shape is non-Map (defensive).
DbasSqliteWebWorkerError _workerErrorFromJsError(Object? err) {
  if (err is Map) {
    final codeRaw = err['code'];
    final codeStr = codeRaw is String ? '[$codeRaw] ' : '';
    final msg = '$codeStr${(err['message'] ?? err.toString())}';
    final rc = err['rc'];
    final extRc = err['extendedRc'];
    return DbasSqliteWebWorkerError(
      msg,
      sqliteCode: rc is num ? rc.toInt() : null,
      sqliteUniqueCode: extRc is num ? extRc.toInt() : null,
    );
  }
  return DbasSqliteWebWorkerError(err?.toString() ?? 'Unknown worker error');
}

/// Per-DB web pool backed by a single Web Worker running `dbas_sqlite_worker.js`.
///
/// The worker loads the WASM module, initializes OPFS, and opens the database.
/// Inside the worker, `createPool(readerCount)` creates a WAL connection pool
/// with 1 writer + N readers — all within the same WASM instance.
///
/// Communication follows the protocol from the DBAS.SQLite worker:
///   init, exec, query, batch, drop, attachStreamBegin/Chunk/End,
///   exportStream (Transferable + chunked fallback), streamCopy, close
class DbasSqliteWebPool {
  static final Map<String, DbasSqliteWebPool> _pools = {};
  static final Map<String, Future<DbasSqliteWebPool>> _pending = {};

  /// Monotonic per-pool diagnostic ID. Used in lifecycle logging so
  /// the user can correlate "pool created" / "pool closed" /
  /// "postMessage on pool X" events across a session. Exposed via
  /// [poolId] for cross-library logging in `dbas_sqlite_native_web.dart`.
  static int _poolIdSeq = 0;
  final int _poolId = ++_poolIdSeq;

  /// Public alias for the diagnostic [_poolId], readable from the
  /// platform shim across libraries.
  int get poolId => _poolId;

  /// Per-dbName barrier completed when a previously-live pool has
  /// fully released its OPFS [`FileSystemSyncAccessHandle`]s (the
  /// worker's `handleClose` awaits `closeOpfsHandles()` before
  /// responding, so the close response is the "OPFS released" signal).
  ///
  /// [create] awaits this barrier before deciding whether to reuse or
  /// spawn, which closes the OPFS-ownership race: the OPFS spec
  /// allows only one sync access handle per file per origin, so a
  /// fresh `init` running while the previous worker is still draining
  /// `closeOpfsHandles` would either race the drain (rare happy path)
  /// or invalidate the previous worker's still-open handles (the
  /// "file was already closed" warning observed in production). Pairs
  /// with [_pending] (which serialises concurrent creates).
  static final Map<String, Future<void>> _closing = {};

  /// Diagnostic count of prepared statements currently open on the
  /// worker for THIS pool (incremented on [prepareQueryStream] success,
  /// decremented on [finalizeStmt]). The worker rejects a write
  /// (`exec` / `BEGIN`) while its `_openStmts` is non-empty, so logging
  /// this count at write time pinpoints a read cursor that wasn't
  /// finalized before the write. Diagnostic only — does not gate.
  int _openStmtCount = 0;

  final String dbName;
  final web.Worker _worker;
  int _nextId = 0;
  final Map<int, Completer<dynamic>> _requests = {};

  /// Handlers for multi-message streaming protocols.
  /// These receive the raw [_JSObj] (not dartified) so they can handle
  /// non-dartifiable types like [ReadableStream].
  final Map<int, void Function(_JSObj)> _streamHandlers = {};
  bool _closed = false;

  /// True after [close] has been called. Surfaces pool liveness so the
  /// platform shim can detect when another holder closed the pool
  /// object this instance still references — `_pools` is keyed by
  /// `dbName`, so a sibling `DbasSqliteNativeWeb` for the same DB can
  /// call `close()` on the pool, leaving the original holder's `_pool`
  /// field pointing at a dead worker. Reading `isClosed` lets the shim
  /// detect this and trigger a fresh `create` on the next operation
  /// instead of dispatching through a worker that has been terminated.
  bool get isClosed => _closed;

  DbasSqliteWebPool._(this.dbName, this._worker) {
    _worker.onmessage = ((web.MessageEvent e) {
      final jsData = e.data;
      if (jsData == null || jsData.isUndefinedOrNull) return;
      final jsObj = jsData as _JSObj;

      final idProp = jsObj['id'];
      if (idProp == null || idProp.isUndefinedOrNull) return;
      final id = (idProp as JSNumber).toDartDouble.toInt();

      // Streaming handlers get raw JS (not dartified)
      final streamHandler = _streamHandlers[id];
      if (streamHandler != null) {
        streamHandler(jsObj);
        return;
      }

      // Normal single-response path
      final data = (jsData as JSObject).dartify();
      if (data is! Map) return;
      final completer = _requests.remove(id);
      if (completer == null) return;
      if (data.containsKey('error') && data['error'] != null) {
        completer.completeError(_workerErrorFromJsError(data['error']));
      } else {
        completer.complete(data['result']);
      }
    }).toJS;
    _worker.onerror = ((web.Event e) {
      // Capture the ErrorEvent's actual message / filename / lineno so
      // worker-load failures (404 on the script URL, syntax error in
      // the JS, COOP/COEP missing, etc.) surface a useful diagnostic
      // instead of the generic event type 'error'.
      String detail = e.type;
      if (e.isA<web.ErrorEvent>()) {
        final ev = e as web.ErrorEvent;
        final parts = <String>[];
        if (ev.message.isNotEmpty) parts.add(ev.message);
        if (ev.filename.isNotEmpty) parts.add(ev.filename);
        if (ev.lineno != 0) parts.add('line ${ev.lineno}');
        if (parts.isNotEmpty) detail = parts.join(' @ ');
      }
      final error = Exception('Web Worker error: $detail');
      for (final c in _requests.values) {
        if (!c.isCompleted) c.completeError(error);
      }
      _requests.clear();
      // Propagate to stream handlers via synthetic error message
      final handlers = Map.of(_streamHandlers);
      _streamHandlers.clear();
      for (final entry in handlers.entries) {
        try {
          final errMsg = <String, dynamic>{
            'id': entry.key,
            'error': {'code': 'WORKER_CRASHED', 'message': error.toString()},
          }.jsify() as _JSObj;
          entry.value(errMsg);
        } catch (handlerError) {
          developer.log(
            'DbasSqliteWebPool: failed to propagate worker crash to '
            'stream handler ${entry.key}',
            name: 'dbas_sqlite.DbasSqliteWebPool',
            error: handlerError,
          );
        }
      }
    }).toJS;
  }

  /// Get or create a pool for the given [dbName].
  ///
  /// Awaits any in-flight [close] for the same `dbName` before
  /// consulting the live-pool registry. Without that gate, the
  /// registry can briefly hold a still-closing pool whose worker has
  /// not yet released its OPFS sync access handle — spawning a fresh
  /// worker in that window collides with the dying handle and
  /// corrupts the file resolver. See [_closing] for the full
  /// rationale.
  static Future<DbasSqliteWebPool> create({
    required String dbName,
    int readerCount = 3,
  }) async {
    final closing = _closing[dbName];
    if (closing != null) {
      await closing;
    }

    if (_pools.containsKey(dbName)) {
      return _pools[dbName]!;
    }
    return _pending.putIfAbsent(dbName, () async {
      try {
        return await _doCreate(dbName: dbName, readerCount: readerCount);
      } finally {
        _pending.remove(dbName);
      }
    });
  }

  static Future<DbasSqliteWebPool> _doCreate({
    required String dbName,
    int readerCount = 3,
  }) async {
    final worker = web.Worker(_workerUrl.toJS);
    final pool = DbasSqliteWebPool._(dbName, worker);

    try {
      await pool.send('init', {
        'dbName': dbName.endsWith('.db') ? dbName : '$dbName.db',
        'role': 'writer',
        'libUrl': _libUrl,
      });
    } catch (e, st) {
      developer.log(
        '_doCreate(dbName=$dbName) pool id=${pool._poolId} init FAILED',
        name: 'dbas_sqlite.DbasSqliteWebPool',
        error: e,
        stackTrace: st,
      );
      rethrow;
    }

    _pools[dbName] = pool;
    return pool;
  }

  /// Send a command to the worker and await the single response.
  Future<dynamic> send(String action, [Map<String, dynamic>? payload]) {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer = Completer<dynamic>();
    _requests[id] = completer;
    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': action,
        'payload': payload ?? {},
      }.jsify());
    } catch (e) {
      _requests.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Execute a write statement.
  ///
  /// Uses raw JS property access for the result to correctly handle
  /// `lastInsertId` which may be a JS BigInt (Emscripten `long long`).
  Future<Map<String, dynamic>> exec(String sql, [dynamic params]) async {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer = Completer<dynamic>();
    // Use a stream handler to get raw JS access (avoids dartify losing BigInt)
    _streamHandlers[id] = (_JSObj jsData) {
      _streamHandlers.remove(id);
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        final err = (errorProp as JSObject).dartify();
        completer.completeError(_workerErrorFromJsError(err));
        return;
      }
      final resultProp = jsData['result'];
      if (resultProp != null && !resultProp.isUndefinedOrNull) {
        final jsResult = resultProp as _JSObj;
        final out = <String, dynamic>{
          'affectedRows': _jsToInt(jsResult['affectedRows']),
          'lastInsertId': _jsToInt(jsResult['lastInsertId']),
        };
        // Propagate `rows` if a future worker action shape includes
        // them. The current bundle's `exec` action returns counters
        // only; SELECTs go through the per-stmt streaming path
        // (`prepareQueryStream` / `bindParams` / `readRows` /
        // `finalizeStmt`) instead, which is how `Statement.executeReader`
        // operates on every platform after the v2.5.0 unification. This
        // branch is kept defensively so a worker upgrade that adds
        // rows here would not silently lose data.
        final rowsProp = jsResult['rows'];
        if (rowsProp != null && !rowsProp.isUndefinedOrNull) {
          final dartified = (rowsProp as JSObject).dartify();
          if (dartified is List) out['rows'] = dartified;
        }
        completer.complete(out);
      } else {
        completer.complete({'affectedRows': 0, 'lastInsertId': 0});
      }
    };
    final payload = <String, dynamic>{'sql': sql};
    if (params != null) payload['params'] = params;
    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'exec',
        'payload': payload,
      }.jsify());
    } catch (e) {
      _streamHandlers.remove(id);
      completer.completeError(e);
    }
    final result = await completer.future;
    if (result is Map) return Map<String, dynamic>.from(result);
    return {'affectedRows': 0, 'lastInsertId': 0};
  }

  /// Execute a read query. Returns all rows.
  ///
  /// **Eager**. Used internally for short PRAGMA / version probes
  /// where the result set is known to be tiny (e.g.
  /// `SELECT sqlite_version()`, `PRAGMA journal_mode`). User-facing
  /// SELECTs go through the per-stmt streaming path
  /// ([prepareQueryStream] + [bindParams] + [readRow] for the first
  /// step + [readRows] for subsequent chunks + [finalizeStmt]) which
  /// is what `DbasSqliteStatement.executeReader` and
  /// `DbasSqliteStatement.executeScalar` drive on every platform
  /// after the v2.5.0 unification. That path matches native FFI
  /// `executeReader` behaviour exactly: rows arrive incrementally,
  /// `executeScalar` issues exactly one `readRow` and then closes.
  Future<List<Map<String, dynamic>>> query(String sql, [dynamic params]) async {
    final payload = <String, dynamic>{'sql': sql};
    if (params != null) payload['params'] = params;
    final result = await send('query', payload);
    if (result is List) {
      return result.map((row) => Map<String, dynamic>.from(row as Map)).toList();
    }
    if (result == null) return [];
    throw StateError('Unexpected query result type: ${result.runtimeType}');
  }

  // ── Streaming SELECT (1:1 mirror of native FFI prepare/step/finalize) ──
  //
  // These primitives expose the worker's per-statement streaming
  // protocol (worker bundle v4.5.0: prepareQuery / bindParams /
  // readRow / readRows (chunked) / finalizeStmt + getStmtAffectedRows /
  // getStmtLastInsertedId). They are the web-side counterpart of the
  // native FFI `prepareQuery` / `bindParams` / `readRowAndCache` /
  // `finalizeStmt` calls.
  //
  // Statement handles cross postMessage as JS BigInts (the worker
  // stores them in a JS `Map` keyed by BigInt; sending them back as
  // `Number` would miss the lookup and produce `UNKNOWN_HANDLE`). We
  // keep the handle as a raw [JSAny] (carrying the JS BigInt) so it
  // round-trips back to the worker via `postMessage` without going
  // through Dart `BigInt` — the Dart SDK shipped with this Flutter
  // version (3.11.4) does not have the `BigInt.toJS` / `JSBigInt.toDart`
  // extensions yet, and a raw passthrough is more efficient anyway.

  /// Prepares a SQL statement on the worker and returns the raw JS
  /// handle (a `BigInt`, opaque to Dart) plus column metadata captured
  /// at prepare time. The worker holds the SHARED reader fence across
  /// the statement's lifetime; it is released by [finalizeStmt].
  Future<
      ({
        JSAny rawHandle,
        int columnCount,
        List<String> columnNames
      })> prepareQueryStream(String sql) {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer = Completer<
        ({
          JSAny rawHandle,
          int columnCount,
          List<String> columnNames
        })>();

    _streamHandlers[id] = (_JSObj jsData) {
      _streamHandlers.remove(id);
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        final err = (errorProp as JSObject).dartify();
        completer.completeError(_workerErrorFromJsError(err));
        return;
      }
      final resultProp = jsData['result'];
      if (resultProp == null || resultProp.isUndefinedOrNull) {
        completer.completeError(
            Exception('prepareQuery: worker returned no result for "$sql"'));
        return;
      }
      final result = resultProp as _JSObj;
      final handleRaw = result['handle'];
      if (handleRaw == null || handleRaw.isUndefinedOrNull) {
        completer.completeError(
            Exception('prepareQuery: response missing handle for "$sql"'));
        return;
      }
      final cc =
          ((result['columnCount'] as JSNumber).toDartDouble).toInt();
      final namesRaw = result['columnNames'];
      final names = <String>[];
      if (namesRaw != null && !namesRaw.isUndefinedOrNull) {
        final arr = (namesRaw as JSArray<JSAny?>).toDart;
        for (final n in arr) {
          if (n != null && !n.isUndefinedOrNull) {
            names.add((n as JSString).toDart);
          } else {
            names.add('');
          }
        }
      }
      _openStmtCount++;
      completer.complete((
        rawHandle: handleRaw,
        columnCount: cc,
        columnNames: names,
      ));
    };

    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'prepareQuery',
        'payload': <String, dynamic>{'sql': sql},
      }.jsify());
    } catch (e) {
      _streamHandlers.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Binds [params] (positional list or named map, same shape used by
  /// [exec] / [query]) onto the prepared [handle].
  ///
  /// On bind failure the worker does **not** auto-finalize — matching
  /// native FFI semantics. The caller is responsible for calling
  /// [finalizeStmt] to release the handle.
  Future<void> bindParams(JSAny rawHandle, dynamic params) {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer = Completer<void>();
    _requests[id] = completer;

    final payload = <String, dynamic>{'handle': rawHandle};
    if (params != null) payload['params'] = params;

    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'bindParams',
        'payload': payload,
      }.jsify());
    } catch (e) {
      _requests.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Binds a single [param] (positional `int` index or named `String`
  /// placeholder, e.g. `':name'` / `'@name'` / `r'$name'`) on the
  /// prepared [rawHandle]. One worker round-trip per bind, returning
  /// the actual SQLite rc so the statement layer's `_replayBinds` can
  /// honour the per-call rc contract — matching native FFI's
  /// `sqlite3_bind_*` semantics 1:1, including the
  /// `SQLITE_RANGE`-on-missing-named-parameter behaviour that the
  /// `bindNameParameters` docstring promises ("silently skipped …
  /// matching Microsoft.Data.Sqlite").
  ///
  /// This method ALWAYS throws on a non-OK outcome — both for worker /
  /// protocol failures (generic exceptions) and for SQLite-level non-OK
  /// rcs (a [DbasSqliteWebWorkerError] whose
  /// [DbasSqliteWebWorkerError.sqliteCode] carries the rc). The
  /// throw-vs-return rc translation lives one layer up in
  /// `DbasSqliteNativeWeb._bindOne`, which catches and converts so the
  /// platform-interface signature (`Future<int>` returning the rc)
  /// matches native.
  Future<void> bindParam(JSAny rawHandle, Object param, Object? value) {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer = Completer<void>();
    _requests[id] = completer;

    final payload = <String, dynamic>{
      'handle': rawHandle,
      'param': param,
      'value': value,
    };

    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'bindParam',
        'payload': payload,
      }.jsify());
    } catch (e) {
      _requests.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Steps the prepared [rawHandle] one row.
  ///
  /// Returns `(rc: 100, columns: [...])` on `SQLITE_ROW` (with one
  /// [ColumnData] per column in [columnNames] order), or
  /// `(rc: 101, columns: null)` on `SQLITE_DONE`. Throws on any other
  /// rc. The caller is responsible for [finalizeStmt] in both cases —
  /// the worker does not auto-finalize on `SQLITE_DONE`.
  ///
  /// Used by `DbasSqliteNativeWeb.readRowAndCache` for the **first**
  /// step of every prepared SELECT — so `executeScalar` issues
  /// exactly one round-trip and exits without fetching additional
  /// rows. Subsequent steps use [readRows] to bulk-fetch chunks.
  Future<({int rc, List<ColumnData>? columns})> readRow(
      JSAny rawHandle, List<String> columnNames) {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer =
        Completer<({int rc, List<ColumnData>? columns})>();

    _streamHandlers[id] = (_JSObj jsData) {
      _streamHandlers.remove(id);
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        final err = (errorProp as JSObject).dartify();
        completer.completeError(_workerErrorFromJsError(err));
        return;
      }
      final resultProp = jsData['result'];
      if (resultProp == null || resultProp.isUndefinedOrNull) {
        completer.completeError(
            DbasSqliteWebWorkerError('readRow: empty result'));
        return;
      }
      final result = resultProp as _JSObj;
      final rcRaw = result['rc'];
      final rc = (rcRaw as JSNumber).toDartDouble.toInt();
      if (rc == sqliteDone) {
        completer.complete((rc: rc, columns: null));
        return;
      }
      if (rc != sqliteRow) {
        // Worker returned a real envelope with a non-OK SQLite rc
        // (e.g. SQLITE_CONSTRAINT mid-iteration over an
        // INSERT…RETURNING that violates UNIQUE). Surface the rc on
        // the worker error so [DbasSqliteNativeWeb._captureError] can
        // populate the platform accessors and the public
        // [DbasSqliteException] carries the code.
        completer.completeError(DbasSqliteWebWorkerError(
            'readRow: unexpected rc=$rc from worker',
            sqliteCode: rc));
        return;
      }
      final rowProp = result['row'];
      if (rowProp == null || rowProp.isUndefinedOrNull) {
        completer.completeError(DbasSqliteWebWorkerError(
            'readRow: rc=100 but no row payload',
            sqliteCode: rc));
        return;
      }
      final rowObj = rowProp as _JSObj;
      final cols = <ColumnData>[];
      for (final name in columnNames) {
        cols.add(_classifyJsColumnValue(rowObj[name]));
      }
      completer.complete((rc: rc, columns: cols));
    };

    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'readRow',
        'payload': <String, dynamic>{'handle': rawHandle},
      }.jsify());
    } catch (e) {
      _streamHandlers.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Steps the prepared [rawHandle] up to [maxRows] times in a single
  /// worker round-trip and returns the rows fetched plus a flag
  /// indicating whether more rows might be available.
  ///
  ///  - `hasMore == true`: [maxRows] reached without `SQLITE_DONE` —
  ///    call [readRows] again to fetch the next chunk.
  ///  - `hasMore == false`: the worker observed `SQLITE_DONE`; the
  ///    statement is exhausted and any further [readRows] returns an
  ///    empty list with `hasMore == false`.
  ///
  /// Throws on SQLite step errors. The caller is responsible for
  /// [finalizeStmt] in both cases — the worker does not auto-finalize
  /// on `SQLITE_DONE`.
  ///
  /// The worker caps [maxRows] at 10000 to bound memory; values up to
  /// that cap are accepted. Typical chunk size is 50.
  Future<({List<List<ColumnData>> rows, bool hasMore})> readRows(
      JSAny rawHandle, List<String> columnNames, int maxRows) {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer =
        Completer<({List<List<ColumnData>> rows, bool hasMore})>();

    _streamHandlers[id] = (_JSObj jsData) {
      _streamHandlers.remove(id);
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        final err = (errorProp as JSObject).dartify();
        completer.completeError(_workerErrorFromJsError(err));
        return;
      }
      final resultProp = jsData['result'];
      if (resultProp == null || resultProp.isUndefinedOrNull) {
        completer.completeError(
            DbasSqliteWebWorkerError('readRows: empty result'));
        return;
      }
      final result = resultProp as _JSObj;
      final hasMoreRaw = result['hasMore'];
      final hasMore = hasMoreRaw != null &&
          !hasMoreRaw.isUndefinedOrNull &&
          (hasMoreRaw as JSBoolean).toDart;
      final rowsArr = result['rows'];
      final outRows = <List<ColumnData>>[];
      if (rowsArr != null && !rowsArr.isUndefinedOrNull) {
        // Inspect each row's raw JS values directly. Going through
        // `dartify()` would lose precision for SQLite INTEGER values
        // outside int32 range (the worker emits a JS BigInt for those,
        // which dartify converts to a Dart String in some Dart-on-web
        // build configurations — see the legacy comment in
        // `dbas_sqlite_row_cache.dart`). Reading the JS object directly
        // and classifying via `typeofEquals` / `isA<>` keeps the type
        // and the bit-pattern intact end-to-end.
        final rowList = (rowsArr as JSArray<JSAny?>).toDart;
        for (final r in rowList) {
          if (r == null || r.isUndefinedOrNull) {
            outRows.add(List.filled(
                columnNames.length, ColumnData(type: 5, isNull: true)));
            continue;
          }
          final rowObj = r as _JSObj;
          final cols = <ColumnData>[];
          for (final name in columnNames) {
            cols.add(_classifyJsColumnValue(rowObj[name]));
          }
          outRows.add(cols);
        }
      }
      completer.complete((rows: outRows, hasMore: hasMore));
    };

    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'readRows',
        'payload': <String, dynamic>{
          'handle': rawHandle,
          'maxRows': maxRows,
        },
      }.jsify());
    } catch (e) {
      _streamHandlers.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Finalises the prepared [rawHandle] on the worker. Idempotent and
  /// tolerant of unknown handles, so it is safe to call after a
  /// successful `SQLITE_DONE` step or after a bind failure.
  Future<void> finalizeStmt(JSAny rawHandle) {
    if (_closed) {
      // Join an in-flight close instead of claiming the worker is
      // already gone. `_closed` latches at the TOP of [close], before
      // the graceful worker close and `terminate()` it awaits, so the
      // old unconditional `Future.value()` was answering "implicitly
      // finalized" while the worker was still alive with the statement
      // still open — a close that has STARTED reported as one that has
      // FINISHED. Waiting on [_closing] (the same barrier [close]'s own
      // duplicate-caller path awaits, for the same reason) makes the
      // answer true when it is given: the barrier completes only after
      // the worker confirmed its OPFS drain and every pending request
      // was settled, so the handle really is reclaimed by then.
      //
      // Null barrier means no close is in flight for this dbName — the
      // close finished, the worker is gone, and the original immediate
      // return is correct.
      final closing = _closing[dbName];
      if (closing != null) return closing;
      return Future.value();
    }
    final id = _nextId++;
    if (_openStmtCount > 0) _openStmtCount--;
    final completer = Completer<void>();
    _requests[id] = completer;
    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'finalizeStmt',
        'payload': <String, dynamic>{'handle': rawHandle},
      }.jsify());
    } catch (e) {
      _requests.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Returns `sqlite3_changes()` for the connection scoped to the
  /// prepared [rawHandle]. The worker throws `STMT_NEVER_STEPPED` if
  /// the handle has never been successfully stepped via [readRow] or
  /// [readRows], so callers must read counters only after at least
  /// one successful step.
  Future<int> getStmtAffectedRows(JSAny rawHandle) async {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer = Completer<int>();
    _streamHandlers[id] = (_JSObj jsData) {
      _streamHandlers.remove(id);
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        final err = (errorProp as JSObject).dartify();
        completer.completeError(_workerErrorFromJsError(err));
        return;
      }
      final resultProp = jsData['result'];
      if (resultProp == null || resultProp.isUndefinedOrNull) {
        completer.completeError(
            Exception('getStmtAffectedRows: empty result'));
        return;
      }
      final result = resultProp as _JSObj;
      completer.complete(_jsToInt(result['value']));
    };
    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'getStmtAffectedRows',
        'payload': <String, dynamic>{'handle': rawHandle},
      }.jsify());
    } catch (e) {
      _streamHandlers.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Returns `sqlite3_last_insert_rowid()` for the connection scoped
  /// to the prepared [rawHandle]. Same `STMT_NEVER_STEPPED` invariant
  /// as [getStmtAffectedRows].
  Future<int> getStmtLastInsertedId(JSAny rawHandle) async {
    if (_closed) throw StateError('Pool is closed for "$dbName"');
    final id = _nextId++;
    final completer = Completer<int>();
    _streamHandlers[id] = (_JSObj jsData) {
      _streamHandlers.remove(id);
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        final err = (errorProp as JSObject).dartify();
        completer.completeError(_workerErrorFromJsError(err));
        return;
      }
      final resultProp = jsData['result'];
      if (resultProp == null || resultProp.isUndefinedOrNull) {
        completer.completeError(
            Exception('getStmtLastInsertedId: empty result'));
        return;
      }
      final result = resultProp as _JSObj;
      completer.complete(_jsToInt(result['value']));
    };
    try {
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'getStmtLastInsertedId',
        'payload': <String, dynamic>{'handle': rawHandle},
      }.jsify());
    } catch (e) {
      _streamHandlers.remove(id);
      completer.completeError(e);
    }
    return completer.future;
  }

  /// Classify a raw JS column value into a typed [ColumnData]. Thin
  /// delegator to the canonical implementation in
  /// `dbas_sqlite_web_columns.dart`, shared with the multi-worker
  /// `DbasSqliteWebReaderPool` so both pools produce identical row shapes.
  static ColumnData _classifyJsColumnValue(JSAny? raw) =>
      classifyJsColumnValue(raw);

  /// Drop the database (removes all OPFS files).
  Future<void> drop() async {
    await send('drop');
  }

  // ── Streaming attach (chunked protocol) ─────────────────────────────────

  /// Attach a database from a Dart [Stream] using the chunked protocol
  /// (`attachStreamBegin` / `attachStreamChunk` / `attachStreamEnd`).
  ///
  /// Each chunk is transferred as an ArrayBuffer for zero-copy handoff.
  /// The worker sends an ACK after each chunk, providing backpressure.
  Future<void> attachStreamChunked(Stream<List<int>> stream, {int? totalSize}) async {
    if (_closed) throw StateError('Pool is closed for "$dbName"');

    final id = _nextId++;
    final readyCompleter = Completer<void>();
    final endCompleter = Completer<void>();
    Completer<void>? ackCompleter;

    _streamHandlers[id] = (_JSObj jsData) {
      // Error
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        _streamHandlers.remove(id);
        final err = (errorProp as JSObject).dartify();
        final exception = _workerErrorFromJsError(err);
        if (!readyCompleter.isCompleted) readyCompleter.completeError(exception);
        ackCompleter?.completeError(exception);
        if (!endCompleter.isCompleted) endCompleter.completeError(exception);
        return;
      }

      // Action-based messages (ready / ack)
      final actionProp = jsData['action'];
      if (actionProp != null && !actionProp.isUndefinedOrNull) {
        final action = (actionProp as JSString).toDart;
        if (action == 'attachStreamReady') {
          if (!readyCompleter.isCompleted) readyCompleter.complete();
        } else if (action == 'attachStreamAck') {
          final ac = ackCompleter;
          if (ac != null && !ac.isCompleted) ac.complete();
        }
        return;
      }

      // Normal result (from attachStreamEnd response)
      final resultProp = jsData['result'];
      if (resultProp != null && !resultProp.isUndefinedOrNull) {
        _streamHandlers.remove(id);
        if (!endCompleter.isCompleted) endCompleter.complete();
      }
    };

    try {
      // Begin — worker closes DB and opens FS path for writing
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'attachStreamBegin',
        'payload': totalSize != null ? {'totalSize': totalSize} : <String, dynamic>{},
      }.jsify());
      await readyCompleter.future;

      // Stream chunks with backpressure (wait for ACK after each)
      await for (final chunk in stream) {
        ackCompleter = Completer<void>();
        final bytes = Uint8List.fromList(chunk);
        final jsBuffer = bytes.buffer.toJS;
        _worker.postMessage(<String, dynamic>{
          'id': id,
          'action': 'attachStreamChunk',
          'payload': {'chunk': jsBuffer},
        }.jsify(), [jsBuffer].toJS);
        await ackCompleter.future;
      }

      // End — worker closes FS, reopens DB
      _worker.postMessage(<String, dynamic>{
        'id': id,
        'action': 'attachStreamEnd',
        'payload': <String, dynamic>{},
      }.jsify());
      await endCompleter.future;
    } catch (e) {
      _streamHandlers.remove(id);
      // Fire-and-forget abort using the original session ID so the worker
      // can correlate it with the active attach session.
      if (!_closed) {
        try {
          _worker.postMessage(<String, dynamic>{
            'id': id,
            'action': 'attachStreamAbort',
            'payload': <String, dynamic>{},
          }.jsify());
        } catch (abortErr, abortStack) {
          // Best-effort signal — if posting fails the worker is
          // likely already gone. Log so the rare diagnostic case
          // (worker terminated unexpectedly) isn't completely silent.
          developer.log(
            'attachStreamAbort postMessage failed for "$dbName"',
            name: 'dbas_sqlite.DbasSqliteWebPool',
            error: abortErr,
            stackTrace: abortStack,
          );
        }
      }
      rethrow;
    }
  }

  // ── Streaming export ────────────────────────────────────────────────────

  /// Export the database content as bytes using the streaming protocol.
  ///
  /// **Eager output**: chunks stream from the worker to Dart, but
  /// they're aggregated into a single `List<int>` here before the
  /// future completes — the entire DB sits in Dart memory at the
  /// peak. This is the default `getContent` path. Memory-friendly
  /// alternatives exist at the application layer (`streamCopyDb` for
  /// file-to-file copy without ever entering Dart memory).
  ///
  /// Handles both the Transferable Streams path (Chrome/Firefox — the worker
  /// sends a [ReadableStream]) and the chunked postMessage fallback (Safari —
  /// `exportStreamChunk` messages with ACK-based backpressure).
  Future<List<int>> exportContentStream() async {
    if (_closed) throw StateError('Pool is closed for "$dbName"');

    final id = _nextId++;
    final completer = Completer<List<int>>();
    final chunks = <Uint8List>[];

    _streamHandlers[id] = (_JSObj jsData) {
      // Error
      final errorProp = jsData['error'];
      if (errorProp != null && !errorProp.isUndefinedOrNull) {
        _streamHandlers.remove(id);
        final err = (errorProp as JSObject).dartify();
        if (!completer.isCompleted) completer.completeError(_workerErrorFromJsError(err));
        return;
      }

      // Chunked path: exportStreamChunk action
      final actionProp = jsData['action'];
      if (actionProp != null && !actionProp.isUndefinedOrNull) {
        final action = (actionProp as JSString).toDart;
        if (action == 'exportStreamChunk') {
          final payloadObj = jsData['payload'] as _JSObj;
          final chunkProp = payloadObj['chunk'];
          if (chunkProp != null && !chunkProp.isUndefinedOrNull) {
            chunks.add((chunkProp as JSArrayBuffer).toDart.asUint8List());
          }
          // Send ACK for backpressure
          _worker.postMessage(<String, dynamic>{
            'id': id,
            'action': 'exportStreamAck',
          }.jsify());
        }
        return;
      }

      // Result — either a ReadableStream (transferable path) or final
      // success object from the chunked path.
      final resultProp = jsData['result'];
      if (resultProp != null && !resultProp.isUndefinedOrNull) {
        if (resultProp.isA<web.ReadableStream>()) {
          // Transferable Streams path — read chunks from the stream
          _readStreamToBytes(resultProp as web.ReadableStream).then((bytes) {
            _streamHandlers.remove(id);
            if (!completer.isCompleted) completer.complete(bytes);
          }).catchError((Object e) {
            _streamHandlers.remove(id);
            if (!completer.isCompleted) completer.completeError(e);
          });
        } else {
          // Final success from chunked path — assemble accumulated chunks
          _streamHandlers.remove(id);
          if (!completer.isCompleted) {
            final builder = BytesBuilder();
            for (final c in chunks) {
              builder.add(c);
            }
            completer.complete(builder.toBytes());
          }
        }
        return;
      }

      // Progress messages — ignore
    };

    _worker.postMessage(<String, dynamic>{
      'id': id,
      'action': 'exportStream',
      'payload': <String, dynamic>{},
    }.jsify());

    return completer.future.timeout(
      const Duration(seconds: 120),
      onTimeout: () {
        _streamHandlers.remove(id);
        throw TimeoutException('exportContentStream did not complete within 120 seconds');
      },
    );
  }

  /// Read all bytes from a [ReadableStream] (used for the Transferable
  /// Streams export path).
  Future<Uint8List> _readStreamToBytes(web.ReadableStream stream) async {
    final reader = stream.getReader() as web.ReadableStreamDefaultReader;
    final builder = BytesBuilder();
    try {
      while (true) {
        final result = await reader.read().toDart;
        final jsResult = result as _JSObj;
        final doneVal = jsResult['done'];
        if (doneVal != null && !doneVal.isUndefinedOrNull &&
            (doneVal as JSBoolean).toDart) {
          break;
        }
        final value = jsResult['value'];
        if (value != null && !value.isUndefinedOrNull) {
          if (value.isA<JSUint8Array>()) {
            builder.add((value as JSUint8Array).toDart);
          } else if (value.isA<JSArrayBuffer>()) {
            builder.add((value as JSArrayBuffer).toDart.asUint8List());
          } else {
            throw StateError(
              '_readStreamToBytes: unexpected chunk type from ReadableStream');
          }
        }
      }
    } finally {
      try {
        reader.releaseLock();
      } catch (e) {
        developer.log(
          'DbasSqliteWebPool: releaseLock failed',
          name: 'dbas_sqlite.DbasSqliteWebPool',
          error: e,
        );
      }
    }
    return builder.toBytes();
  }

  /// Convert a JS value (Number, BigInt, or null) to a Dart int. Thin
  /// delegator to the canonical [jsAnyToInt] in `dbas_sqlite_web_columns.dart`.
  static int _jsToInt(JSAny? v) => jsAnyToInt(v);

  /// Copy the database to a new OPFS file.
  Future<void> streamCopy(String destName) async {
    await send('streamCopy', {
      'destName': destName.endsWith('.db') ? destName : '$destName.db',
    });
  }

  /// Close the worker and release resources.
  ///
  /// The pool stays registered in [_pools] for the full duration of
  /// the close — including the worker-side `closeOpfsHandles()` drain
  /// — and is only removed in the `finally` after the worker has
  /// terminated. Until then, any concurrent [create] for the same
  /// `dbName` blocks on [_closing] so the OPFS sync access handle
  /// cannot be acquired by a fresh worker before the previous one
  /// has released it. See [_closing] for the rationale.
  ///
  /// A duplicate `close()` call (already in flight) awaits the same
  /// barrier so its caller's "teardown done" signal lines up with
  /// the actual OPFS-release timing, rather than racing ahead of it.
  Future<void> close() async {
    if (_closed) {
      // Already closing or closed. Wait for the in-flight drain so
      // duplicate callers (e.g. two `_teardownLivePool`s on sibling
      // `DbasSqliteNativeWeb` instances sharing this pool object)
      // observe consistent "OPFS released" timing.
      final closing = _closing[dbName];
      if (closing != null) await closing;
      return;
    }
    // Flip the flag synchronously BEFORE awaiting the graceful close so
    // any concurrent send() invocation rejects immediately instead of
    // queueing a request whose response will never arrive once we
    // terminate the worker below.
    _closed = true;

    // Register the closing barrier synchronously so any concurrent
    // [create] for the same dbName waits for full OPFS drain before
    // spawning a fresh worker. Note: `_pools.remove(dbName)` is
    // deliberately deferred to the `finally` block below — keeping
    // the entry in `_pools` during the drain prevents
    // [DbasSqliteNativeWeb._ensurePool] from observing a `null`
    // registry slot and concluding "no owner" while the worker still
    // holds the OPFS handle.
    final closeBarrier = Completer<void>();
    _closing[dbName] = closeBarrier.future;

    try {
      // Post the 'close' message directly (bypassing send() which now
      // throws on _closed) and await the response inline. The worker's
      // `handleClose` awaits `closeOpfsHandles()` before posting its
      // response, so a successful await here means the OPFS file is
      // fully released — that's what makes the barrier above honest.
      final id = _nextId++;
      final completer = Completer<dynamic>();
      _requests[id] = completer;
      try {
        _worker.postMessage(<String, dynamic>{
          'id': id,
          'action': 'close',
          'payload': <String, dynamic>{},
        }.jsify());
        await completer.future;
      } catch (e, st) {
        developer.log(
          'DbasSqliteWebPool: graceful close failed for "$dbName"',
          name: 'dbas_sqlite.DbasSqliteWebPool',
          error: e,
          stackTrace: st,
        );
      } finally {
        _requests.remove(id);
      }

      // Terminate is best-effort: the cleanup loops below MUST run
      // regardless so that in-flight `_requests` completers and
      // `_streamHandlers` are not left hanging. If `terminate()`
      // throws (worker proxy detached, browser exception in a
      // degraded context, …), we still need to reject every awaiter
      // — otherwise the static `_closing` barrier completes in the
      // outer `finally` advertising "OPFS released, safe to spawn"
      // while Dart-side callers from the dying pool stay wedged.
      try {
        _worker.terminate();
      } catch (e, st) {
        developer.log(
          'DbasSqliteWebPool: worker.terminate() threw for "$dbName"; '
          'continuing with completer cleanup',
          name: 'dbas_sqlite.DbasSqliteWebPool',
          error: e,
          stackTrace: st,
        );
      }

      // After terminate, any still-pending completers will never receive
      // a response. Reject them so callers don't hang forever.
      final closedErr = StateError('Pool closed for "$dbName"');
      for (final c in _requests.values) {
        if (!c.isCompleted) c.completeError(closedErr);
      }
      _requests.clear();

      // Stream handlers own their completers inside their closures (see
      // `prepareQueryStream`, `readRow`, `readRows`, `getStmtAffectedRows`,
      // `getStmtLastInsertedId`, `exec`, `attachStreamChunked`,
      // `exportContentStream`). Synthesise an `error`-shaped JS object
      // and dispatch it to each handler — they're already structured to
      // unwrap the error and reject their captured completer. Without
      // this, an in-flight stream-RPC caller (e.g. a `readRows` chunk
      // fetch driving `DbasSqliteReader.readRow` mid-iteration) would
      // hang forever after a graceful close.
      final handlersSnapshot = Map.of(_streamHandlers);
      _streamHandlers.clear();
      for (final entry in handlersSnapshot.entries) {
        try {
          final errMsg = <String, dynamic>{
            'id': entry.key,
            'error': {
              'code': 'POOL_CLOSED',
              'message': 'Pool closed for "$dbName"',
            },
          }.jsify() as _JSObj;
          entry.value(errMsg);
        } catch (e, st) {
          developer.log(
            'DbasSqliteWebPool: failed to wake stream handler ${entry.key} '
            'during close',
            name: 'dbas_sqlite.DbasSqliteWebPool',
            error: e,
            stackTrace: st,
          );
        }
      }
    } finally {
      // OPFS is released (worker confirmed close, or we logged the
      // failure and proceeded to terminate). NOW publish the
      // teardown to the static registry: drop the pool slot AND
      // release the barrier so subsequent [create] calls can spawn
      // a fresh worker without colliding with a still-open sync
      // access handle.
      _pools.remove(dbName);
      _closing.remove(dbName);
      closeBarrier.complete();
    }
  }

  static void removePool(String dbName) {
    _pools.remove(dbName);
  }
}
