import 'dart:async';
import 'dart:typed_data';

import 'package:dbas_sqlite/src/dbas_sqlite.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_column_type.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_db.dart'
    if (dart.library.js_interop) 'package:dbas_sqlite/src/stub/dbas_sqlite_db_stub.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_platform.dart';
import 'package:dbas_sqlite/src/dbas_sqlite_row_cache.dart';
import 'package:dbas_sqlite/src/exceptions/dbas_sqlite_exception.dart';
import 'package:decimal/decimal.dart';
// `show`n rather than imported wholesale: a bare foundation import makes
// this file's `dart:typed_data` (Uint8List, for getColumnBlob) redundant.
import 'package:flutter/foundation.dart' show visibleForTesting;

/// Why a [DbasSqliteReader] was closed, as far as the reader itself can
/// actually know it.
///
/// Exists so [DbasSqliteErrorCode.readerClosedDuringScan] can describe
/// the cause instead of asserting one. A reader records *that* it was
/// closed; before this it also told every caller *who* had closed it
/// ("something tore it down mid-scan"), which sends a consumer who
/// deliberately closed after a partial scan hunting for a `closeDb()`
/// that never happened.
enum _ReaderCloseReason {
  /// Someone called [DbasSqliteReader.close] or
  /// [DbasSqliteStatement.close] directly. The commonest case, and the
  /// one the old message described worst.
  explicit,

  /// `closeDb`'s statement sweep — the only route that genuinely is a
  /// teardown, and the only one allowed to say so.
  teardown,

  /// `readRow` closed the reader itself after a step failed. The caller
  /// already received that failure; this only explains the state a later
  /// call finds.
  stepFailed,
}

/// An independent reader for a single prepared SELECT statement.
///
/// Each [DbasSqliteReader] is bound to one statement handle on one
/// connection (a pool reader, or the writer if inside a transaction).
/// Multiple readers can coexist simultaneously across different
/// statements.
///
/// Use [readRow] to iterate; the column accessors read from the
/// per-reader [RowData] cache populated on each step. Call [close]
/// when done — or let [readRow] auto-close when there are no more
/// rows.
///
/// ```dart
/// final stmt = await db.prepareQuery('SELECT * FROM users WHERE age > ?');
/// final reader = await stmt.executeReader(params: [18]);
/// while (await reader.readRow()) {
///   print(reader.getColumnText(0));
/// }
/// await reader.close();
/// ```
class DbasSqliteReader {
  /// Test-only rendezvous inside [readRow]'s step window — after the
  /// native step has been dispatched against this reader's live
  /// `sqlite3_stmt` and before [readRow] consumes its result. `null` in
  /// production; the awaited call is the only cost when it is set.
  ///
  /// Exists because that window is the one stretch of a reader's life no
  /// other seam can observe. `DbasSqliteStatement.debugBeforeReaderTransfer`
  /// stops one instruction too early — there is no reader yet — and once
  /// there is one, nothing this class exposes moves while a step is out:
  /// [isClosed] is still `false`, the row cache still holds the previous
  /// row, and the parent statement's `_activeReader` has pointed here
  /// since `executeReader` returned.
  ///
  /// Awaited as part of the step future itself rather than beside it (see
  /// [_stepAndCache]), so whatever a caller tracks as "this reader has a
  /// step outstanding" stays outstanding for exactly as long as the hook
  /// parks. A hook that ran alongside the dispatch instead would let the
  /// dispatch settle underneath it and observe nothing.
  ///
  /// Reset it to `null` in a `finally` / `addTearDown`; it is static, so
  /// a leaked hook would park the first row of every later test.
  ///
  /// **Never await a close of this reader from inside the hook** — a
  /// `db.closeDb()`, a `stmt.close()` or a `reader.close()`. The hook is
  /// part of the step future, [close] drains that future before it lets
  /// `onClose` finalize anything, and `closeDb`'s statement sweep runs
  /// through [close]: the hook would be waiting on a close that is
  /// waiting on the hook. That wait is unbounded by design (see
  /// [close]), so the deadlock produces no error at all — only the
  /// periodic stall report [close] logs. Start the close from the test
  /// body and let the hook do nothing but park, as the reader-teardown
  /// cases do.
  @visibleForTesting
  static Future<void> Function()? debugInsideReadRowStep;

  /// How long [close] waits for an in-flight step before it starts
  /// logging that it is still waiting, and the interval it repeats at.
  ///
  /// It does **not** bound the wait — see [close] for why the wait
  /// itself must stay unbounded. It exists so that "unbounded" does not
  /// also mean "silent": a teardown wedged behind a step that never
  /// hands back is otherwise indistinguishable from a slow one. Wire
  /// [DbasSqlite.onDiagnostic] to actually receive the report; the
  /// `dart:developer` copy reaches nobody in a release build or under
  /// `flutter test`.
  static const int kStepDrainStallReportMs = 5000;

  /// Test-only override for [kStepDrainStallReportMs], so the stall
  /// report can be exercised in milliseconds. Static, like the other
  /// debug seams here — reset it in a `finally` / `addTearDown`. Values
  /// below 1 ms are clamped to 1 ms.
  @visibleForTesting
  static int? debugStepDrainStallReportMs;

  final DbasSqliteDb _conn;
  final int _handle;
  final DbasSqlitePlatform _platform;
  final Future<void> Function() _onClose;
  final RowData _rowCache = RowData();

  bool _closed = false;
  Future<void>? _closeFuture;

  /// Why this reader closed — see [_ReaderCloseReason]. Meaningful only
  /// once [_closed] is `true`; the initial value is never read.
  _ReaderCloseReason _closeReason = _ReaderCloseReason.explicit;

  /// Every step [readRow] currently has dispatched against [_handle].
  /// **This reader's only signal that native code is touching its
  /// `sqlite3_stmt` right now**, and what [_doClose] drains before it
  /// lets `onClose` finalize that handle.
  ///
  /// **A set, not a single slot, because N steps can be outstanding at
  /// once.** Nothing rejects two un-awaited [readRow] calls on one
  /// reader, and a single slot would be *overwritten* by the second:
  /// the first step would still be inside `sqlite3_step` with nothing
  /// referencing it, so a [close] arriving in between would drain only
  /// the second and finalize the handle under the first — the exact
  /// `FinalizeStmt`-under-`sqlite3_step` corruption this drain exists to
  /// prevent, reached through the drain. `_dispatch` picks the
  /// least-loaded worker with no per-handle affinity, so two steps do
  /// not even share an OS thread and can settle in either order.
  ///
  /// Concurrent [readRow] on one reader is not a *useful* shape — the
  /// two calls race [_rowCache], so each may read the other's row — but
  /// nothing rejects it, so "not useful" must not be allowed to mean
  /// "corrupts memory".
  ///
  /// Nothing else in the wrapper can stand in for this. `readRow`
  /// registers nothing with [DbasSqlite]'s native-operation registry
  /// (see `_drainNativeOps`), so from teardown's point of view a reader
  /// parked mid-step looks exactly like an idle one: [isClosed] is
  /// `false`, the row cache still holds the previous row, and the parent
  /// statement's `_activeReader` has pointed here since `executeReader`
  /// returned.
  final Set<Future<int>> _inFlightSteps = <Future<int>>{};

  /// How many steps are currently published in [_inFlightSteps].
  /// Test-only seam, and the only NON-DESTRUCTIVE witness there is for
  /// "this step reached the set before anything could observe it": the
  /// alternative — starting a close from inside the step window and
  /// seeing whether it waits — is the corruption under test, and a
  /// SIGSEGV kills the runner instead of failing an assertion.
  @visibleForTesting
  int get debugInFlightStepCount => _inFlightSteps.length;

  /// How many times [close] has reported that it is still waiting for an
  /// in-flight step. Test-only seam: the stall report is a fire-and-
  /// forget side effect with no other observable, and an unbounded wait
  /// that stopped reporting would be silent again.
  @visibleForTesting
  int get debugStepDrainStallReports => _stepDrainStallReports;
  int _stepDrainStallReports = 0;

  /// `true` once a step returned `SQLITE_DONE` — the result set genuinely
  /// ran out. The **only** state in which a [readRow] on a closed reader
  /// may answer `false`; see [readRow] for why every other closed state
  /// throws instead.
  bool _exhausted = false;

  /// Internal constructor used by [DbasSqliteStatement.executeReader].
  /// Consumers should not call this directly.
  ///
  /// [initialColumnCount] / [initialColumnNames] populate the per-
  /// reader [RowData] cache with metadata captured at prepare time.
  /// This makes [getColumnCount] / [getColumnName] return correct
  /// values BEFORE the first [readRow] call.
  DbasSqliteReader.internal({
    required this._conn,
    required this._handle,
    required this._platform,
    required this._onClose,
    int initialColumnCount = 0,
    List<String> initialColumnNames = const [],
  }) {
    _rowCache.columnCount = initialColumnCount;
    _rowCache.columnNames = initialColumnNames;
  }

  /// Whether this reader has been closed.
  ///
  /// Latches the INSTANT a close starts, before anything has been waited
  /// for. Correct for "may I still use this reader?" — every consumer
  /// entry point must refuse from that moment — but **not** for "is
  /// native code done with this reader?". Nothing on this class answers
  /// that second question, deliberately: an `isFullyClosedInternal` flag
  /// set at the end of [_doClose] used to, and it was inert by
  /// construction — see [DbasSqliteStatement.hasOpenWriterReaderInternal],
  /// which is the only predicate that ever needed the distinction and
  /// which gets it from the statement's own slot instead.
  bool get isClosed => _closed;

  /// Advances to the next row of the current result set.
  ///
  /// Returns `true` if a row is available, `false` when all rows have
  /// been read. The reader is automatically closed when there are no
  /// more rows.
  ///
  /// Throws a [DbasSqliteException] with code
  /// [DbasSqliteErrorCode.readRowFailed] if the query execution fails.
  /// The exception carries the primary step rc on
  /// [DbasSqliteException.sqliteCode] and — when the platform resolved
  /// one — the extended rc on [DbasSqliteException.sqliteUniqueCode]
  /// (e.g. 2067 for `SQLITE_CONSTRAINT_UNIQUE`).
  ///
  /// **`false` means "no more rows", and nothing else.** Calling this on
  /// a reader that was closed for any other reason — an explicit
  /// [close], [DbasSqliteStatement.close], or `closeDb`'s statement
  /// sweep during teardown — throws
  /// [DbasSqliteErrorCode.readerClosedDuringScan] instead. Answering
  /// `false` there would make a scan cut short by teardown
  /// indistinguishable from one that ran out of rows, and the consumer
  /// shape this protects (a `while (await readRow())` loop building a
  /// list) would hand back a silently truncated result with no error of
  /// any kind. See [close] for what a mid-scan consumer observes.
  Future<bool> readRow() async {
    if (_closed) {
      if (_exhausted) return false;
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.readerClosedDuringScan,
        'This reader was closed before its result set was exhausted, so '
        'the scan is TRUNCATED rather than finished. ${_closedByDetail()} '
        'Returning false would be indistinguishable from a genuine end of '
        'rows and would silently drop the remaining rows, so the '
        'truncation is raised instead. Stop iterating: the statement '
        'behind this reader has already been finalized.',
      );
    }

    // Publish the step BEFORE suspending on it: [_doClose] reads
    // [_inFlightSteps] to decide whether native code is still touching
    // [_handle], and a step that only becomes visible after the first
    // `await` is a step teardown can walk past. [_stepAndCache] does the
    // publishing itself, in the same expression as the dispatch, so the
    // two cannot come apart.
    final step = _stepAndCache();
    final int readResult;
    try {
      readResult = await step;
    } finally {
      // Clear only OUR registration. A concurrent un-awaited readRow has
      // its own entry, and that one must stay visible to [_doClose].
      _inFlightSteps.remove(step);
    }

    if (!_isSuccessRc(readResult)) {
      // Every connection-scoped read happens HERE, before `close()`
      // hands the pool connection back through `releaseFn`. After that
      // release `_conn` may already be serving another reader's
      // statement, so a later `getErrorCode` would describe someone
      // else's failure — and both are SYNCHRONOUS main-isolate FFI while
      // `finalizeStmt` / `closePool` run on worker isolates, which makes
      // a read after the release a use-after-free in a teardown race
      // rather than merely a stale number.
      String? error = _platform.getLastStmtError(_conn, _handle);
      final errorCode = _platform.getErrorCode(_conn) ?? readResult;
      final uniqueErrorCode = _platform.getUniqueErrorCode(_conn);
      await _close(_ReaderCloseReason.stepFailed);
      if (error == null && readResult == sqliteMisuse) {
        error = 'Misuse: possibly missing or invalid bind.';
      }
      error ??= 'Unknown error ($readResult).';
      throw DbasSqliteException.sqlite(
        DbasSqliteErrorCode.readRowFailed,
        'It was not possible to run the query ($readResult): $error',
        sqliteCode: errorCode,
        sqliteUniqueCode: uniqueErrorCode,
      );
    }

    final hasRow = readResult == sqliteRow;
    if (!hasRow) {
      // Set BEFORE the close, so the closed reader is already tagged
      // "ran out of rows" by the time anything can observe it closed.
      _exhausted = true;
      await close();
    }
    return hasRow;
  }

  /// Reads up to [amount] rows by repeatedly calling [readRow],
  /// snapshotting each row as a `Map<String, ColumnData>` keyed by
  /// column name. Each [ColumnData] preserves the SQLite type, the
  /// raw value, and the null flag for the column.
  ///
  /// Returns a record with:
  ///   * `rows`: between 0 and [amount] entries;
  ///   * `hasMore`: the boolean result of the last [readRow] call —
  ///     `true` if [amount] rows were read and more may still follow,
  ///     `false` if the result set was exhausted before reaching
  ///     [amount].
  ///
  /// Returns an empty list with `hasMore: false` immediately when
  /// [amount] is non-positive.
  ///
  /// This is the library's own [readRow] loop, so it inherits both of
  /// that method's teardown properties rather than restating them: a
  /// [close] arriving mid-batch waits for the dispatched step (see
  /// [close]), and a reader torn down mid-batch makes the next
  /// [readRow] throw [DbasSqliteErrorCode.readerClosedDuringScan].
  /// **That throw propagates and the rows gathered so far are
  /// discarded** — deliberately, and for the reason [readRow] documents:
  /// returning them with `hasMore: false` would report a truncated batch
  /// as a completed one, and returning them with `hasMore: true` would
  /// invite a follow-up call on a finalized statement.
  Future<({List<Map<String, ColumnData>> rows, bool hasMore})> readRows(
      [int amount = 50]) async {
    final rows = <Map<String, ColumnData>>[];
    if (amount <= 0) return (rows: rows, hasMore: false);
    bool hasMore = false;
    for (int i = 0; i < amount; i++) {
      hasMore = await readRow();
      if (!hasMore) break;
      final cols = _rowCache.columns;
      if (cols == null) {
        // Raised, never skipped. `readRow()` answered `true`, so a row
        // exists; `continue`ing past it would drop that row and return
        // the list SHORT — the exact silent truncation this method's
        // contract promises cannot happen. No producer currently emits
        // `SQLITE_ROW` with a null column set, which is what makes this
        // defensive rather than reachable, but a defence that silently
        // loses a row is worse than none.
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.readRowFailed,
          'readRows: the step reported a row but the row cache holds no '
          'columns, so the row cannot be snapshotted. Returning the batch '
          'without it would silently drop a row that SQLite produced.',
        );
      }
      final row = <String, ColumnData>{};
      for (int c = 0; c < cols.length; c++) {
        row[getColumnName(c)] = cols[c];
      }
      rows.add(row);
    }
    return (rows: rows, hasMore: hasMore);
  }

  /// Dispatches one native step, registers it in [_inFlightSteps], and
  /// lets the platform populate [_rowCache] from its reply.
  ///
  /// Split out of [readRow] for two reasons. The future it returns — one
  /// of this reader's signals that a step is outstanding against
  /// [_handle] — spans [debugInsideReadRowStep] as well as the dispatch;
  /// and the dispatch and its registration happen in one expression with
  /// no `await` between them, so nothing can run in that gap and there is
  /// no path on which a step reaches native code without reaching
  /// [_inFlightSteps]. Such a step would be dispatched against a live
  /// handle, referenced by nothing and drained by nothing. ([_stepThroughHook]
  /// yields before it calls the hook for exactly this reason — see there.)
  ///
  /// Not `async`, so the production path (hook `null`) returns the
  /// platform future itself and adds no frame or microtask per row.
  Future<int> _stepAndCache() {
    final step = _platform.readRowAndCache(_conn, _handle, _rowCache);
    // Test-only rendezvous — see [debugInsideReadRowStep].
    final insideStep = debugInsideReadRowStep;
    if (insideStep == null) return _publishStep(step);
    return _publishStep(_stepThroughHook(step, insideStep));
  }

  /// Records [step] as outstanding against [_handle] and returns it
  /// unchanged.
  Future<int> _publishStep(Future<int> step) {
    _inFlightSteps.add(step);
    return step;
  }

  /// [debugInsideReadRowStep] composed **into** [step] rather than run
  /// beside it, so "this reader has a step outstanding" stays true for
  /// exactly as long as the hook parks. A hook running alongside the
  /// dispatch would let the dispatch settle underneath it and observe
  /// nothing.
  ///
  /// The hook is invoked in here rather than by [_stepAndCache] so that
  /// a hook throwing **synchronously** becomes this future's error
  /// instead of an exception escaping [_stepAndCache] before
  /// [_publishStep] runs — which would leave [step] dispatched against a
  /// live handle and tracked by nothing. The step is awaited on the
  /// failure path too, for the same reason: it is already out, and the
  /// hook failing does not recall it.
  ///
  /// Test-path only. Production never reaches this frame.
  Future<int> _stepThroughHook(
      Future<int> step, Future<void> Function() hook) async {
    // Yield BEFORE calling the hook. An `async` body runs synchronously
    // up to its first `await`, so without this the hook would run before
    // [_publishStep] had added this future to [_inFlightSteps] — and a
    // hook that synchronously starts a close would find an empty set and
    // finalize the statement under a live step. The docs forbid that
    // shape, but "there is no path on which a step reaches native code
    // without reaching [_inFlightSteps]" has to be true of this path too.
    await Future<void>.value();
    try {
      await hook();
    } catch (_) {
      await step.then<void>((_) {}, onError: (Object _) {});
      rethrow;
    }
    return step;
  }

  /// The cause clause of [DbasSqliteErrorCode.readerClosedDuringScan],
  /// branched on what actually closed this reader rather than listing
  /// every way it might have been closed and letting the reader guess.
  String _closedByDetail() {
    switch (_closeReason) {
      case _ReaderCloseReason.explicit:
        return 'It was closed by an explicit close() — either '
            'reader.close() or DbasSqliteStatement.close(). If that was '
            'deliberate, stop iterating after the close instead of '
            'probing the reader again; if it was not, the close is the '
            'bug, not this call.';
      case _ReaderCloseReason.teardown:
        return "It was torn down by closeDb()'s statement sweep, i.e. the "
            'database was closed while this scan was still running.';
      case _ReaderCloseReason.stepFailed:
        return 'An earlier readRow() failed and closed the reader as part '
            'of reporting that failure — see the readRowFailed exception '
            'that call threw for the underlying cause.';
    }
  }

  // sqlite3_step never returns SQLITE_OK per the C contract; the
  // success values are SQLITE_ROW (more rows) and SQLITE_DONE (end of
  // result set). Anything else is an error.
  bool _isSuccessRc(int rc) => rc == sqliteRow || rc == sqliteDone;

  // ── Column accessors ─────────────────────────────────────────────────

  bool isColumnNull(int idx) => _column(idx)?.isNull ?? true;

  String getColumnText(int idx) => _column(idx)?.value?.toString() ?? '';

  String? getColumnNullableText(int idx) =>
      isColumnNull(idx) ? null : getColumnText(idx);

  bool getColumnBool(int idx) => getColumnInt(idx) == 1;
  bool? getColumnNullableBool(int idx) =>
      isColumnNull(idx) ? null : getColumnBool(idx);

  int getColumnInt(int idx) => toIntSafe(_column(idx)?.value);
  int? getColumnNullableInt(int idx) =>
      isColumnNull(idx) ? null : getColumnInt(idx);

  Decimal getColumnDecimal(int idx) {
    if (isColumnNull(idx)) return Decimal.zero;
    final text = _column(idx)?.value?.toString() ?? '';
    final v = Decimal.tryParse(text);
    if (v == null) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.invalidDecimalFormat,
        'getColumnDecimal: cannot parse column $idx value as Decimal: "$text"',
      );
    }
    return v;
  }

  Decimal? getColumnNullableDecimal(int idx) =>
      isColumnNull(idx) ? null : getColumnDecimal(idx);

  double getColumnDouble(int idx) => toDoubleSafe(_column(idx)?.value);
  double? getColumnNullableDouble(int idx) =>
      isColumnNull(idx) ? null : getColumnDouble(idx);

  /// Reads a stored timestamp as a UTC [DateTime].
  ///
  /// SQLite stores timestamps as text. The convention is that every
  /// persisted timestamp is UTC, so a naive stored string (no offset /
  /// `Z`) is interpreted as UTC wall-clock and is **never shifted** by
  /// the device timezone; an explicit offset / `Z` is honored. The
  /// returned value always has `isUtc == true`, so equality and ordering
  /// comparisons never diverge between `…Z` and no-`Z` values.
  DateTime getColumnDateTime(int idx) {
    final parsed = DateTime.parse(_column(idx)?.value?.toString() ?? '');
    return parsed.isUtc
        ? parsed
        : DateTime.utc(parsed.year, parsed.month, parsed.day, parsed.hour,
            parsed.minute, parsed.second, parsed.millisecond,
            parsed.microsecond);
  }

  DateTime? getColumnNullableDateTime(int idx) =>
      isColumnNull(idx) ? null : getColumnDateTime(idx);

  Duration getColumnTime(int idx) {
    final raw = _column(idx)?.value?.toString() ?? '';
    final parts = raw.split(':');
    if (parts.length < 2) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.invalidTimeFormat,
        'getColumnTime: column $idx value "$raw" is not in HH:MM or HH:MM:SS[.mmm] format',
      );
    }

    int parsePart(String s, String label) {
      final v = int.tryParse(s);
      if (v == null) {
        throw DbasSqliteException.dart(
          DbasSqliteErrorCode.invalidTimeComponent,
          'getColumnTime: column $idx value "$raw" has invalid $label component "$s"',
        );
      }
      return v;
    }

    final hours = parsePart(parts[0], 'hours');
    final minutes = parsePart(parts[1], 'minutes');

    int seconds = 0;
    int milliseconds = 0;
    if (parts.length > 2) {
      final secParts = parts[2].split('.').where((s) => s.trim().isNotEmpty).toList();
      seconds = parsePart(secParts.first, 'seconds');
      if (secParts.length > 1) {
        milliseconds = parsePart(
          secParts.last.padRight(3, '0').substring(0, 3),
          'milliseconds',
        );
      }
    }

    return Duration(
      hours: hours,
      minutes: minutes,
      seconds: seconds,
      milliseconds: milliseconds,
    );
  }

  Duration? getColumnNullableTime(int idx) =>
      isColumnNull(idx) ? null : getColumnTime(idx);

  T getColumnEnum<T extends Enum>(int idx, List<T> values) {
    final intValue = getColumnInt(idx);
    if (intValue < 0 || intValue >= values.length) {
      throw DbasSqliteException.dart(
        DbasSqliteErrorCode.invalidEnumIndex,
        'No enum value found for index $intValue in ${T.toString()}',
      );
    }
    return values[intValue];
  }

  T? getColumnNullableEnum<T extends Enum>(int idx, List<T> values) =>
      isColumnNull(idx) ? null : getColumnEnum<T>(idx, values);

  Uint8List getColumnBlob(int idx) {
    final value = _column(idx)?.value;
    if (value is Uint8List) return value;
    if (value is List) return Uint8List.fromList(value.cast<int>());
    return Uint8List(0);
  }

  Uint8List? getColumnNullableBlob(int idx) =>
      isColumnNull(idx) ? null : getColumnBlob(idx);

  String getColumnName(int columnIndex) {
    final names = _columnNames();
    return columnIndex < names.length ? names[columnIndex] : '';
  }

  SqliteColumnType getColumnType(int idx) =>
      SqliteColumnType.fromInt(_column(idx)?.type ?? 5);

  int getColumnCount() => _rowCache.columnCount;

  /// Returns the typed value of the column at [idx] based on its
  /// SQLite type. Returns `null` for NULL columns.
  dynamic getColumnValue(int idx) {
    switch (getColumnType(idx)) {
      case SqliteColumnType.integer:
        return getColumnInt(idx);
      case SqliteColumnType.double:
        return getColumnDouble(idx);
      case SqliteColumnType.blob:
        return getColumnBlob(idx);
      case SqliteColumnType.nullType:
        return null;
      default:
        return getColumnText(idx);
    }
  }

  ColumnData? _column(int idx) {
    final cols = _rowCache.columns;
    if (cols == null || idx >= cols.length) return null;
    return cols[idx];
  }

  List<String> _columnNames() => _rowCache.columnNames;

  // ── Lifecycle ────────────────────────────────────────────────────────

  /// Closes this reader, releasing its connection back to the pool.
  ///
  /// Idempotent — concurrent calls all observe the same completion
  /// future, so a second caller waits for the first call's cleanup
  /// to finish rather than returning instantly while resources are
  /// still mid-tear-down. Auto-called when [readRow] returns `false`.
  ///
  /// The `_closed = true` flag is set synchronously before the first
  /// `await` so the active-reader guard on the parent statement
  /// observes the closing state immediately — and, since this release,
  /// so that no NEW step can be dispatched once teardown has begun,
  /// which is what makes the drain below terminate.
  ///
  /// **Waits for every in-flight [readRow] step before running
  /// `onClose`.** `onClose` finalizes this reader's `sqlite3_stmt`, and a
  /// step that is still dispatched is native code holding that exact
  /// handle on a different worker thread — `_dispatch` picks the
  /// least-loaded worker, so the step and the finalize genuinely do not
  /// share one. Neither side interlocks in C: `FinalizeStmt` has no busy
  /// check and no refcount, and `ReadRow` resolves the pointer, drops
  /// `db_stmts_lock`, then steps and writes through it unlocked.
  /// Serialising here covers every route by construction — an explicit
  /// [close], [DbasSqliteStatement.close], `executeScalar`'s `finally`,
  /// and `closeDb`'s statement sweep all funnel through this one method
  /// — and every step, since [_inFlightSteps] is a set rather than a
  /// single slot (see it for why that matters).
  ///
  /// The wait is deliberately **unbounded**. A timeout could only expire
  /// into finalizing the handle anyway (the corruption this exists to
  /// prevent), and throwing instead would leave `onClose` unrun — the
  /// statement never finalized and the pool reader never released, which
  /// wedges `ClosePool` just as hard with less information. What is
  /// waited for is already-dispatched `sqlite3_step`s, not a consumer
  /// -driven scan: they cannot fail to arrive unless the worker isolate
  /// is gone, in which case nothing is recoverable.
  ///
  /// Unbounded is not the same as silent, and this is the one wait in
  /// the library that no timeout will ever surface: every
  /// [kStepDrainStallReportMs] spent waiting reports how long it has
  /// waited and for how many steps, so a wedged teardown can be diagnosed
  /// from a log instead of inferred from a hang. That report goes to
  /// [DbasSqlite.onDiagnostic] **as well as** `dart:developer` —
  /// `developer.log` alone is discarded whenever no VM service client is
  /// subscribed, which is every release build on a device and every
  /// `flutter test` run, i.e. precisely where a production hang happens.
  /// Note that `closeDb`'s advertised `kNativeOpDrainTimeoutMs` bound
  /// does **not** cover this wait: the registry drain it bounds runs
  /// earlier, and this one sits downstream of it inside the statement
  /// sweep.
  ///
  /// **Consumer-visible consequence, intended:** a consumer suspended in
  /// `readRow()` when this runs still receives the row its step already
  /// produced — it was read before anything was torn down and the cache
  /// is pure Dart — and its NEXT `readRow()` throws
  /// [DbasSqliteErrorCode.readerClosedDuringScan]. A `while (await
  /// readRow())` loop therefore ends in an error rather than in a
  /// silently short list.
  Future<void> close() => _close(_ReaderCloseReason.explicit);

  /// [close] for `closeDb`'s statement sweep, which is the one caller
  /// that may truthfully describe itself as teardown. Everything else it
  /// does is identical — including the join-idempotency, so a sweep
  /// arriving on a reader whose consumer already started closing it joins
  /// that close and leaves its reason (and its message) alone.
  Future<void> closeForTeardownInternal() =>
      _close(_ReaderCloseReason.teardown);

  Future<void> _close(_ReaderCloseReason reason) =>
      _closeFuture ??= _doClose(reason);

  Future<void> _doClose(_ReaderCloseReason reason) async {
    // Recorded before the latch below, so a reader is never observable
    // as closed without also carrying why. [_closeFuture] means the FIRST
    // caller's reason is the one kept, which is the honest one: it is the
    // close that actually tore the reader down.
    _closeReason = reason;
    // Synchronous, and first — and that makes ONE pass enough.
    //
    // [readRow]'s guard and its publish sit in a single uninterrupted
    // stretch: there is no `await` between `if (_closed)` and the
    // `_stepAndCache()` below it, and `_stepAndCache` publishes before
    // it returns. So latching here, before this method's first
    // suspension, means no further step can ever be published against
    // [_handle] — every readRow that had already dispatched one is in
    // the set, and every readRow that had not yet will now throw at the
    // guard. The snapshot taken below is therefore COMPLETE, which is
    // why this drains once instead of looping. (Put an `await` between
    // that guard and that publish and this reasoning stops holding —
    // the snapshot would no longer be complete and this would have to
    // become a loop.)
    _closed = true;
    if (_inFlightSteps.isNotEmpty) {
      final steps = List.of(_inFlightSteps);
      _inFlightSteps.clear();
      await _awaitSteps(steps);
    }
    await _onClose();
  }

  /// Waits for every step in [steps] to hand back, logging a stall
  /// report every [kStepDrainStallReportMs] rather than ever giving up
  /// on one — see [close] for why the wait must stay unbounded and why
  /// it must not therefore be silent.
  ///
  /// Results and failures belong to the [readRow] calls that dispatched
  /// these steps; all this needs to know is that native code is done
  /// with [_handle]. Swallowing the errors here also keeps a failed step
  /// from surfacing as an unhandled asynchronous error on this path.
  Future<void> _awaitSteps(List<Future<int>> steps) async {
    final waited = Stopwatch()..start();
    final everyMs = debugStepDrainStallReportMs ?? kStepDrainStallReportMs;
    final reporter = Timer.periodic(
      Duration(milliseconds: everyMs < 1 ? 1 : everyMs),
      (_) {
        _stepDrainStallReports++;
        // Through [DbasSqlite.reportDiagnosticInternal], not `developer
        // .log` alone: this is the only report a wedged teardown ever
        // produces, and `developer.log` is dropped whenever no VM service
        // client is subscribed — which is every release build on a device
        // and every `flutter test` run. See [DbasSqlite.onDiagnostic].
        DbasSqlite.reportDiagnosticInternal(
          'reader close: waited ${waited.elapsedMilliseconds}ms so far for '
          '${steps.length} in-flight readRow step(s) on statement $_handle, '
          'and is still waiting. The wait is UNBOUNDED by design — expiring '
          'it could only expire into finalizing a statement a step is using, '
          'which is the corruption it exists to prevent — so this repeats '
          'until the step(s) hand back. A step that never hands back means '
          'the worker isolate serving it is gone or wedged, or something '
          'inside the step window is itself waiting on this close (a '
          'debugInsideReadRowStep hook that awaits closeDb does exactly '
          'that).',
          name: 'dbas_sqlite.DbasSqliteReader',
        );
      },
    );
    try {
      await Future.wait([
        for (final step in steps)
          step.then<void>((_) {}, onError: (Object _) {}),
      ]);
    } finally {
      reporter.cancel();
    }
  }
}
