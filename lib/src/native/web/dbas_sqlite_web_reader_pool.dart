import 'dart:async';
import 'dart:developer' as developer;
import 'dart:js_interop';
import 'dart:js_interop_unsafe';
import 'dart:typed_data';

import 'package:web/web.dart' as web;

import 'package:dbas_sqlite/src/dbas_sqlite.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_row_cache.dart';
import 'package:dbas_sqlite/src/stub/dbas_sqlite_db_stub.dart'
    show sqliteRow, sqliteDone;

import 'dbas_sqlite_web_columns.dart';
import 'dbas_sqlite_web_live_pool.dart';
import 'dbas_sqlite_web_pool.dart';

// ── Bundle assets (served by the Flutter plugin) ──────────────────────
const _libAsset = 'assets/packages/dbas_sqlite/web/libs/dbas_sqlite.js';
const _workerAsset =
    'assets/packages/dbas_sqlite/web/libs/dbas_sqlite_worker.js';

// ── globalThis bindings defined by dbas_sqlite.js once it loads ───────
// `createPool({dbName, workerUrl, libUrl, readerCount, onError})` →
// ConnectionPool. Resolved lazily at call time, after the script tag
// below has loaded, so the extern is safe to declare up-front.
@JS('createPool')
external _JsPool _jsCreatePool(JSObject options);

/// True when the page is cross-origin isolated, i.e. `SharedArrayBuffer`
/// is actually usable. `createPool` throws without it, so this gates the
/// multi-worker pool vs. the single-worker fallback.
@JS('crossOriginIsolated')
external bool get _crossOriginIsolated;

/// Raw keyed access to a JS result object (avoids `dartify()` losing
/// BigInt identity on INTEGER columns).
extension type _JsObj._(JSObject _) implements JSObject {
  external JSAny? operator [](String key);
}

/// The `ConnectionPool` object returned by `createPool`. Each method
/// returns a Promise; a rejection carries the worker's structured
/// `{code, message, rc, extendedRc}` envelope (or a pool-level Error
/// with `.code`), unwrapped by [_toWorkerError].
extension type _JsPool._(JSObject _) implements JSObject {
  external JSPromise<JSObject> exec(JSString sql, JSAny? params);
  external JSPromise<JSAny?> query(JSString sql, JSAny? params);
  external JSPromise<JSObject> streamPrepare(JSString role, JSString sql);
  external JSPromise<JSObject> streamBind(
      JSAny cursor, JSAny param, JSAny? value);
  external JSPromise<JSObject> streamReadRow(JSAny cursor);
  external JSPromise<JSObject> streamReadRows(JSAny cursor, JSNumber maxRows);
  external JSPromise<JSObject> streamAffectedRows(JSAny cursor);
  external JSPromise<JSObject> streamLastInsertedId(JSAny cursor);
  external JSPromise<JSObject> streamFinalize(JSAny cursor);
  // getWorkerStatus() is synchronous in the wrapper (returns an array).
  external JSArray<JSAny?> getWorkerStatus();
  external JSPromise<JSAny?> whenReady();
  external JSPromise<JSAny?> close();
}

// ── Main-thread lib loader (idempotent, once per page) ────────────────
Future<void>? _libLoad;

/// Ensures `dbas_sqlite.js` is loaded on the main thread so the
/// `createPool` global exists. Injecting it as a classic `<script>` runs
/// the bundle's function block (defining `globalThis.createPool` etc.)
/// WITHOUT instantiating the WASM module — the MODULARIZE factory is only
/// invoked when a worker calls it, never here.
Future<void> _ensureLibLoaded() {
  if (globalContext.has('createPool')) return Future<void>.value();
  return _libLoad ??= _injectScript(web.URL(_libAsset, web.window.location.href).href);
}

Future<void> _injectScript(String url) {
  final completer = Completer<void>();
  final script = web.HTMLScriptElement()
    ..src = url
    ..type = 'text/javascript'
    ..async = true;
  script.addEventListener(
      'load',
      (web.Event _) {
        if (!completer.isCompleted) completer.complete();
      }.toJS);
  script.addEventListener(
      'error',
      (web.Event _) {
        if (!completer.isCompleted) {
          completer.completeError(
              StateError('Failed to load dbas_sqlite.js from "$url"'));
        }
      }.toJS);
  web.document.head!.appendChild(script);
  return completer.future;
}

/// Converts a JS promise rejection into a [DbasSqliteWebWorkerError],
/// reading the worker's `{code, message, rc, extendedRc}` envelope (or a
/// pool-level `Error` carrying `.code`) when present. Defensive: any
/// non-object rejection falls back to its string form.
DbasSqliteWebWorkerError _toWorkerError(Object e) {
  // Already a typed worker error (an inner defensive throw from one of the
  // stream* methods) — return it as-is. Without this, the `e as JSObject`
  // read below would silently yield null fields on web (a Dart object is a
  // JS object at runtime but has no `code`/`rc` properties), stripping the
  // SQLite codes the public DbasSqliteException relies on.
  if (e is DbasSqliteWebWorkerError) return e;
  String? code;
  String? message;
  int? rc;
  int? extendedRc;
  try {
    final o = e as JSObject;
    final codeV = o.getProperty<JSAny?>('code'.toJS);
    if (codeV != null && codeV.isA<JSString>()) code = (codeV as JSString).toDart;
    final msgV = o.getProperty<JSAny?>('message'.toJS);
    if (msgV != null && msgV.isA<JSString>()) message = (msgV as JSString).toDart;
    final rcV = o.getProperty<JSAny?>('rc'.toJS);
    if (rcV != null && rcV.isA<JSNumber>()) rc = (rcV as JSNumber).toDartInt;
    final extV = o.getProperty<JSAny?>('extendedRc'.toJS);
    if (extV != null && extV.isA<JSNumber>()) {
      extendedRc = (extV as JSNumber).toDartInt;
    }
  } catch (_) {
    // Not a structured JS error object — fall through to toString below.
  }
  final text = '${code != null ? '[$code] ' : ''}${message ?? e.toString()}';
  return DbasSqliteWebWorkerError(text,
      sqliteCode: rc, sqliteUniqueCode: extendedRc);
}

/// True if [e] is a JS error object carrying the exact `.code`. Used to
/// let the STRUCTURAL `POOL_ALREADY_ACTIVE` error (a live pool already
/// exists for this dbName — a prior pool's `close()` was not awaited)
/// escape [bootWebLivePool]'s single-worker fallback instead of being
/// silently demoted to single-worker mode. Defensive: a non-JSObject
/// rejection cleanly returns false.
bool _jsErrorCodeIs(Object e, String code) {
  try {
    final c = (e as JSObject).getProperty<JSAny?>('code'.toJS);
    return c != null && c.isA<JSString>() && (c as JSString).toDart == code;
  } catch (_) {
    return false;
  }
}

/// Adapts a normalized Dart bind value (already passed through the shim's
/// `_jsifyBindValue`, so it is null / int / double / String / Uint8List)
/// into the JS value the worker's `bindParam` expects.
JSAny? _toJsBindValue(Object? v) {
  if (v == null) return null;
  if (v is int) return v.toJS;
  if (v is double) return v.toJS;
  if (v is String) return v.toJS;
  if (v is Uint8List) return v.toJS;
  if (v is bool) return (v ? 1 : 0).toJS;
  return v.toString().toJS;
}

/// Live pool backed by the native multi-worker `createPool` (1 writer +
/// N readers, SharedArrayBuffer-coordinated). Hosted on the main thread
/// (so the pool's workers are not nested workers), driven directly
/// through the `ConnectionPool` object via `js_interop`.
class DbasSqliteWebReaderPool implements WebLivePool {
  static int _idSeq = 0;
  @override
  final int poolId = ++_idSeq;

  final _JsPool _pool;

  /// Non-null from the instant [close] is first called, and the one close
  /// every later caller joins. Memoized rather than re-entered, so a
  /// duplicate [close] can never start a second worker teardown. See
  /// [close] for why joining (rather than returning early) is the point.
  Future<void>? _closeFuture;

  bool _closed = false;

  /// True once [close] has been CALLED — deliberately not "once the
  /// workers are gone".
  ///
  /// Its two consumers (`DbasSqliteNativeWeb._ensurePool`'s liveness
  /// probe and `databaseExists`'s live-pool short-circuit) both need a
  /// pool that is on its way out to read as unusable immediately, so
  /// "started" is the correct reading THERE. It is only wrong where the
  /// answer depends on the workers having actually gone — see
  /// [streamFinalize], which used to make exactly that mistake.
  @override
  bool get isClosed => _closed;

  DbasSqliteWebReaderPool._(this._pool);

  /// Loads the lib (once), creates the pool for [dbName] with
  /// [readerCount] readers, and waits for the init handshake. Throws if
  /// the writer worker failed to initialize (the caller falls back to a
  /// single worker).
  static Future<DbasSqliteWebReaderPool> create({
    required String dbName,
    required int readerCount,
  }) async {
    await _ensureLibLoaded();
    final base = web.window.location.href;
    final options = JSObject()
      ..setProperty('dbName'.toJS, dbName.toJS)
      ..setProperty('workerUrl'.toJS, web.URL(_workerAsset, base).href.toJS)
      ..setProperty('libUrl'.toJS, web.URL(_libAsset, base).href.toJS)
      ..setProperty('readerCount'.toJS, readerCount.toJS)
      ..setProperty(
          'onError'.toJS,
          ((JSObject ev) {
            // Best-effort observability — pool-internal events
            // (init/crash/respawn). Never throws back into the pool.
            developer.log(
              'web pool event: ${(_JsObj._(ev)['code'] as JSString?)?.toDart} '
              '${(_JsObj._(ev)['message'] as JSString?)?.toDart ?? ''}',
              name: 'dbas_sqlite.pool',
            );
          }).toJS);

    final pool = _jsCreatePool(options);
    await pool.whenReady().toDart;

    // Surface a dead writer as a creation failure so the caller can fall
    // back to the single-worker path instead of handing out a pool whose
    // every write rejects with WRITER_DEAD.
    if (_writerIsDead(pool.getWorkerStatus())) {
      try {
        await pool.close().toDart;
      } catch (_) {/* best-effort */}
      throw StateError(
          'createPool: writer worker failed to initialize for "$dbName"');
    }
    return DbasSqliteWebReaderPool._(pool);
  }

  /// `getWorkerStatus()` returns an array of `{role, dead, …}`; the writer
  /// is the entry with `role === 'writer'`.
  static bool _writerIsDead(JSArray<JSAny?> status) {
    final arr = status.toDart;
    for (final s in arr) {
      if (s == null) continue;
      final o = _JsObj._(s as JSObject);
      if ((o['role'] as JSString?)?.toDart == 'writer') {
        return (o['dead'] as JSBoolean?)?.toDart ?? false;
      }
    }
    return false;
  }

  void _ensureOpen() {
    if (_closed) throw StateError('Web reader pool is closed (id=$poolId)');
  }

  @override
  Future<Map<String, dynamic>> exec(String sql) async {
    _ensureOpen();
    try {
      final res = await _pool.exec(sql.toJS, null).toDart;
      final o = _JsObj._(res);
      return <String, dynamic>{
        'affectedRows': jsAnyToInt(o['affectedRows']),
        'lastInsertId': jsAnyToInt(o['lastInsertId']),
      };
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  @override
  Future<List<Map<String, dynamic>>> query(String sql) async {
    _ensureOpen();
    try {
      final res = await _pool.query(sql.toJS, null).toDart;
      if (res == null || res.isUndefinedOrNull || !res.isA<JSArray>()) {
        return const [];
      }
      final rows = (res as JSArray<JSAny?>).toDart;
      final out = <Map<String, dynamic>>[];
      for (final r in rows) {
        if (r == null || r.isUndefinedOrNull) continue;
        final dartified = (r as JSObject).dartify();
        if (dartified is Map) {
          out.add(Map<String, dynamic>.from(dartified));
        }
      }
      return out;
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  @override
  Future<({Object cursor, int columnCount, List<String> columnNames})>
      streamPrepare(bool writer, String sql) async {
    _ensureOpen();
    try {
      final res =
          await _pool.streamPrepare(writer ? 'writer'.toJS : 'reader'.toJS, sql.toJS).toDart;
      final o = _JsObj._(res);
      final cursor = o['cursor'];
      if (cursor == null || cursor.isUndefinedOrNull) {
        throw DbasSqliteWebWorkerError('streamPrepare: response missing cursor');
      }
      final cc = (o['columnCount'] as JSNumber?)?.toDartInt ?? 0;
      final names = <String>[];
      final namesRaw = o['columnNames'];
      if (namesRaw != null && namesRaw.isA<JSArray>()) {
        for (final n in (namesRaw as JSArray<JSAny?>).toDart) {
          names.add((n != null && n.isA<JSString>()) ? (n as JSString).toDart : '');
        }
      }
      return (cursor: cursor as Object, columnCount: cc, columnNames: names);
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  @override
  Future<void> streamBind(Object cursor, Object param, Object? value) async {
    _ensureOpen();
    final jsParam = param is int ? param.toJS : (param as String).toJS;
    try {
      await _pool
          .streamBind(cursor as JSAny, jsParam, _toJsBindValue(value))
          .toDart;
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  @override
  Future<({int rc, List<ColumnData>? columns})> streamReadRow(
      Object cursor, List<String> columnNames) async {
    _ensureOpen();
    try {
      final res = await _pool.streamReadRow(cursor as JSAny).toDart;
      final o = _JsObj._(res);
      final rc = (o['rc'] as JSNumber?)?.toDartInt ?? -1;
      if (rc == sqliteDone) return (rc: rc, columns: null);
      if (rc != sqliteRow) {
        throw DbasSqliteWebWorkerError('streamReadRow: unexpected rc=$rc',
            sqliteCode: rc);
      }
      final rowRaw = o['row'];
      if (rowRaw == null || rowRaw.isUndefinedOrNull) {
        throw DbasSqliteWebWorkerError('streamReadRow: rc=100 but no row',
            sqliteCode: rc);
      }
      return (rc: rc, columns: _row(rowRaw as JSObject, columnNames));
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  @override
  Future<({List<List<ColumnData>> rows, bool hasMore})> streamReadRows(
      Object cursor, List<String> columnNames, int maxRows) async {
    _ensureOpen();
    try {
      final res =
          await _pool.streamReadRows(cursor as JSAny, maxRows.toJS).toDart;
      final o = _JsObj._(res);
      final hasMore = (o['hasMore'] as JSBoolean?)?.toDart ?? false;
      final rows = <List<ColumnData>>[];
      final rowsRaw = o['rows'];
      if (rowsRaw != null && rowsRaw.isA<JSArray>()) {
        for (final r in (rowsRaw as JSArray<JSAny?>).toDart) {
          if (r == null || r.isUndefinedOrNull) {
            rows.add(List.filled(
                columnNames.length, ColumnData(type: 5, isNull: true)));
          } else {
            rows.add(_row(r as JSObject, columnNames));
          }
        }
      }
      return (rows: rows, hasMore: hasMore);
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  List<ColumnData> _row(JSObject rowObj, List<String> columnNames) {
    final o = _JsObj._(rowObj);
    return [for (final name in columnNames) classifyJsColumnValue(o[name])];
  }

  @override
  Future<int> streamAffectedRows(Object cursor) async {
    _ensureOpen();
    try {
      final res = await _pool.streamAffectedRows(cursor as JSAny).toDart;
      return jsAnyToInt(_JsObj._(res)['value']);
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  @override
  Future<int> streamLastInsertedId(Object cursor) async {
    _ensureOpen();
    try {
      final res = await _pool.streamLastInsertedId(cursor as JSAny).toDart;
      return jsAnyToInt(_JsObj._(res)['value']);
    } catch (e) {
      throw _toWorkerError(e);
    }
  }

  @override
  Future<void> streamFinalize(Object cursor) async {
    // Join an in-flight close rather than reading [isClosed] as "already
    // gone". This guard used to be `if (_closed) return;` with the claim
    // "workers gone; statements implicitly finalized" — false for the
    // whole duration of a close, because `_closed` latches at the TOP of
    // [close], before the worker teardown it awaits. In that window the
    // workers are alive, the cursor is still open, and the caller was
    // being told the finalize was complete.
    //
    // Joining makes the claim true by construction instead of asserting
    // it early: when the join returns, the pool's own teardown really
    // has reclaimed every straggler cursor — which is what the old
    // comment described, just at the wrong moment. It matters because
    // `DbasSqliteNativeWeb.finalizeStmt` drops its in-flight-finalize
    // count when this future completes, and a destructive op's drain
    // waits on that count; completing early hands the drain a false
    // all-clear, the same "started read as finished" error this fix
    // exists to remove.
    //
    // Cannot deadlock: [close] awaits only the JS pool's own teardown.
    // Nothing on the close path awaits a Dart-side finalize, so the join
    // is strictly one-directional.
    final closing = _closeFuture;
    if (closing != null) return await closing;
    try {
      await _pool.streamFinalize(cursor as JSAny).toDart;
    } catch (e) {
      // Finalize is a cleanup step; the pool's own close() reclaims any
      // straggler. Surface via log rather than throwing from teardown.
      developer.log('streamFinalize failed (cursor=$cursor)',
          name: 'dbas_sqlite.pool', error: _toWorkerError(e));
    }
  }

  /// Tears the pool (all workers) down. **Join-idempotent:** every caller
  /// after the first gets the SAME future, so it returns when the workers
  /// are actually gone rather than at the moment the close was requested.
  ///
  /// This replaced `if (_closed) return;`, which was the same
  /// started-vs-finished error as [streamFinalize]'s old guard: a second
  /// closer — `DbasSqliteNativeWeb._teardownLivePool` running on a
  /// sibling shim instance that shares this pool object — returned while
  /// the first close was still tearing workers down, and was then free to
  /// `bootWebLivePool` a replacement against OPFS files the outgoing
  /// workers had not yet released.
  ///
  /// The single-worker sibling ([DbasSqliteWebPool.close]) has behaved
  /// this way since it grew its static `_closing` barrier, and documents
  /// the same rationale; this brings the multi-worker pool in line rather
  /// than inventing a second discipline.
  @override
  Future<void> close() => _closeFuture ??= _doClose();

  Future<void> _doClose() async {
    // Latched before this method's first suspension (an `async` body runs
    // synchronously up to its first `await`, and [close] assigns the
    // memo synchronously too), so a concurrent [exec] / [query] /
    // [streamPrepare] trips [_ensureOpen] immediately instead of opening
    // new work against workers on their way out.
    _closed = true;
    try {
      await _pool.close().toDart;
    } catch (e, st) {
      // BOTH sinks, and the [DbasSqlite.onDiagnostic] one is what makes
      // this reachable at all: `developer.log` is dropped whenever no VM
      // service client is subscribed, i.e. every release build.
      //
      // Swallowing here is now MORE consequential than it was, because
      // [close] memoizes this future. A failed close is never retried,
      // and every later joiner — including
      // `DbasSqliteNativeWeb._teardownLivePool` on a sibling shim — is
      // handed this same completed future and resolves SUCCESSFULLY off
      // it, asserting the very claim that failed: that the workers are
      // gone and their OPFS files released. The next `bootWebLivePool`
      // then races files the outgoing workers may still own. Nothing
      // downstream can detect that, so this report is the only signal
      // there is.
      developer.log('web reader pool close failed (id=$poolId)',
          name: 'dbas_sqlite.pool', error: e, stackTrace: st);
      DbasSqlite.reportDiagnosticInternal(
        'web: tearing down reader pool $poolId failed ($e). The close is '
        'MEMOIZED, so it is never retried and every later caller joins this '
        'failed attempt and is told the pool closed cleanly. Its workers may '
        'still hold their OPFS files, and the next pool created for the same '
        'database will race them.',
        name: 'dbas_sqlite.pool',
      );
    }
  }
}

/// Single-worker fallback: adapts the existing [DbasSqliteWebPool] (one
/// connection, no SAB) to [WebLivePool]. Used when the page is not
/// cross-origin isolated (no `SharedArrayBuffer`) or if multi-worker pool
/// creation fails. The [writer] role is ignored — one connection serves
/// reads and writes — so it keeps the pre-pool behaviour (including the
/// write-while-read limitation), letting the app still run.
///
/// The cursor token is the worker's raw statement handle (a JS BigInt),
/// passed straight back to the underlying pool's rawHandle methods.
class _SingleWorkerLivePool implements WebLivePool {
  final DbasSqliteWebPool _pool;
  _SingleWorkerLivePool(this._pool);

  @override
  int get poolId => _pool.poolId;
  @override
  bool get isClosed => _pool.isClosed;

  @override
  Future<Map<String, dynamic>> exec(String sql) => _pool.exec(sql);

  @override
  Future<List<Map<String, dynamic>>> query(String sql) => _pool.query(sql);

  @override
  Future<({Object cursor, int columnCount, List<String> columnNames})>
      streamPrepare(bool writer, String sql) async {
    final prep = await _pool.prepareQueryStream(sql);
    return (
      cursor: prep.rawHandle as Object,
      columnCount: prep.columnCount,
      columnNames: prep.columnNames,
    );
  }

  @override
  Future<void> streamBind(Object cursor, Object param, Object? value) =>
      _pool.bindParam(cursor as JSAny, param, value);

  @override
  Future<({int rc, List<ColumnData>? columns})> streamReadRow(
          Object cursor, List<String> columnNames) =>
      _pool.readRow(cursor as JSAny, columnNames);

  @override
  Future<({List<List<ColumnData>> rows, bool hasMore})> streamReadRows(
          Object cursor, List<String> columnNames, int maxRows) =>
      _pool.readRows(cursor as JSAny, columnNames, maxRows);

  @override
  Future<int> streamAffectedRows(Object cursor) =>
      _pool.getStmtAffectedRows(cursor as JSAny);

  @override
  Future<int> streamLastInsertedId(Object cursor) =>
      _pool.getStmtLastInsertedId(cursor as JSAny);

  @override
  Future<void> streamFinalize(Object cursor) =>
      _pool.finalizeStmt(cursor as JSAny);

  @override
  Future<void> close() => _pool.close();
}

/// Boots the live read/write pool for [dbName]. Prefers the multi-worker
/// `createPool` pool (true read/write concurrency) when the page is
/// cross-origin isolated; falls back to a single worker otherwise, or if
/// multi-worker creation fails for any reason — so the app always comes up.
Future<WebLivePool> bootWebLivePool({
  required String dbName,
  required int readerCount,
}) async {
  if (_crossOriginIsolated) {
    try {
      return await DbasSqliteWebReaderPool.create(
          dbName: dbName, readerCount: readerCount);
    } catch (e, st) {
      // POOL_ALREADY_ACTIVE is a STRUCTURAL programming error (a live
      // pool for this dbName already exists — the prior pool's close()
      // wasn't awaited), NOT a recoverable multi-worker init failure.
      // Let it propagate loudly. Demoting it to the single-worker
      // fallback would silently lose read/write concurrency and bury the
      // exact lifecycle bug the native one-pool-per-dbName check exists
      // to surface.
      if (_jsErrorCodeIs(e, 'POOL_ALREADY_ACTIVE')) rethrow;
      developer.log(
        'multi-worker pool init FAILED for "$dbName" — falling back to a '
        'single worker (no read/write concurrency; the write-while-read '
        'SQLITE_BUSY can recur). Is the rebuilt dbas_sqlite.js bundle copied '
        'into web/libs/?',
        name: 'dbas_sqlite.lifecycle',
        error: e,
        stackTrace: st,
      );
    }
  } else {
    // The multi-worker pool needs SharedArrayBuffer, which needs the page
    // to be cross-origin isolated (COOP/COEP headers). `flutter run -d
    // chrome` does NOT set those by default — pass --web-header flags (see
    // README) or serve behind a proxy that does. Logged at the lifecycle
    // channel so it shows up next to the SQLITE_BUSY it explains.
    developer.log(
      'page is NOT cross-origin isolated (crossOriginIsolated=false) → '
      'SharedArrayBuffer unavailable → multi-worker read/write pool cannot '
      'run for "$dbName". Falling back to a SINGLE worker, which keeps the '
      'old write-while-read SQLITE_BUSY limitation. Serve with '
      'Cross-Origin-Opener-Policy=same-origin + '
      'Cross-Origin-Embedder-Policy=require-corp to enable the fix.',
      name: 'dbas_sqlite.lifecycle',
    );
  }
  return _SingleWorkerLivePool(await DbasSqliteWebPool.create(dbName: dbName));
}
