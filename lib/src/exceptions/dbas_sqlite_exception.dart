/// Stable identifiers for every throw site in [DbasSqlite],
/// [DbasSqliteStatement], and [DbasSqliteReader]. Each value is used
/// by exactly one throw so callers can branch on a specific failure
/// without parsing message text. For coarse-grained branching prefer
/// the [DbasSqliteErrorCategory] returned by [DbasSqliteErrorCodeX.category].
enum DbasSqliteErrorCode {
  // DbasSqlite — lifecycle
  closeDbBusyWithStmtFinalizeFailures,
  closeDbBusyLeakedHandle,
  prepareQueryDatabaseNotOpened,
  openDbReopenWithDifferentPoolSize,

  // DbasSqlite — busy timeout
  setBusyTimeoutDatabaseNotOpened,
  setBusyTimeoutWriterFailed,
  setBusyTimeoutReaderBusy,
  setBusyTimeoutReaderFailed,

  // DbasSqlite — WAL
  enableWalDatabaseNotOpened,
  enableWalFailed,

  // DbasSqlite — transactions
  beginTransactionDatabaseNotOpened,
  beginTransactionDatabaseClosedWaitingLock,
  beginTransactionFailed,
  commitDatabaseNotOpened,
  commitBlockedByInFlightOperation,
  commitBlockedByActiveReader,
  commitFailed,
  commitRollbackAlsoFailed,
  rollbackFailed,
  transactionAlreadyActive,
  transactionRollbackAlsoFailed,

  // DbasSqlite — vacuum
  vacuumDatabaseNotOpened,
  vacuumInsideTransaction,
  vacuumDatabaseClosedWaitingLock,
  vacuumFailed,

  // DbasSqlite — locks / queues
  writerLockWaitCancelled,
  writerLockWaitTimeout,
  readerSlotWaitTimeout,
  readerSlotWaitCancelled,
  acquireReaderConnectionNoPool,

  // DbasSqliteStatement
  executeSqlPrepareFailed,
  executeSqlStepFailed,
  bindPositionalFailed,
  bindNamedParameterNotFound,
  bindNamedFailed,
  unsupportedPositionalBindType,
  unsupportedNamedBindType,
  readerAlreadyActive,
  executeReaderPoolAcquireTimeout,
  executeReaderPrepareFailed,
  statementClosed,
  statementDatabaseNotOpened,

  // DbasSqliteReader
  readRowFailed,
  invalidDecimalFormat,
  invalidTimeFormat,
  invalidTimeComponent,
  invalidEnumIndex,
}

/// High-level grouping of [DbasSqliteErrorCode] values. Use this for
/// `switch`-based recovery decisions; use the underlying [DbasSqliteErrorCode]
/// for test assertions and telemetry IDs.
enum DbasSqliteErrorCategory {
  /// Caller tried to use an unopened, closed, or closing database / statement.
  notOpened,

  /// A connection / writer-lock / reader-slot wait timed out or was cancelled.
  busyOrCancelled,

  /// `sqlite3_prepare_v2` (or equivalent) refused the SQL.
  prepareFailed,

  /// Step (DDL/DML/SELECT) returned a non-OK / non-row / non-done rc.
  executeFailed,

  /// A bind call rejected the value (range, type mismatch, or unknown name
  /// when `throwOnMissingNamedParams` is enabled).
  bindFailed,

  /// BEGIN/COMMIT/ROLLBACK/VACUUM failed at the SQL or wrapper layer.
  transactionFailed,

  /// A reader-lifecycle invariant was violated (e.g. two readers per stmt).
  readerStateFailed,

  /// Column accessor could not decode the raw value (decimal, time, enum).
  decodeFailed,

  /// Generic / wrapper-internal — should be rare.
  internal,
}

/// Maps each [DbasSqliteErrorCode] to its [DbasSqliteErrorCategory].
extension DbasSqliteErrorCodeX on DbasSqliteErrorCode {
  DbasSqliteErrorCategory get category {
    switch (this) {
      case DbasSqliteErrorCode.prepareQueryDatabaseNotOpened:
      case DbasSqliteErrorCode.setBusyTimeoutDatabaseNotOpened:
      case DbasSqliteErrorCode.enableWalDatabaseNotOpened:
      case DbasSqliteErrorCode.beginTransactionDatabaseNotOpened:
      case DbasSqliteErrorCode.beginTransactionDatabaseClosedWaitingLock:
      case DbasSqliteErrorCode.commitDatabaseNotOpened:
      case DbasSqliteErrorCode.vacuumDatabaseNotOpened:
      case DbasSqliteErrorCode.vacuumDatabaseClosedWaitingLock:
      case DbasSqliteErrorCode.statementClosed:
      case DbasSqliteErrorCode.statementDatabaseNotOpened:
      case DbasSqliteErrorCode.acquireReaderConnectionNoPool:
        return DbasSqliteErrorCategory.notOpened;

      case DbasSqliteErrorCode.closeDbBusyWithStmtFinalizeFailures:
      case DbasSqliteErrorCode.closeDbBusyLeakedHandle:
      case DbasSqliteErrorCode.setBusyTimeoutReaderBusy:
      case DbasSqliteErrorCode.readerSlotWaitTimeout:
      case DbasSqliteErrorCode.readerSlotWaitCancelled:
      case DbasSqliteErrorCode.writerLockWaitCancelled:
      case DbasSqliteErrorCode.writerLockWaitTimeout:
      case DbasSqliteErrorCode.executeReaderPoolAcquireTimeout:
        return DbasSqliteErrorCategory.busyOrCancelled;

      case DbasSqliteErrorCode.executeSqlPrepareFailed:
      case DbasSqliteErrorCode.executeReaderPrepareFailed:
        return DbasSqliteErrorCategory.prepareFailed;

      case DbasSqliteErrorCode.executeSqlStepFailed:
      case DbasSqliteErrorCode.readRowFailed:
        return DbasSqliteErrorCategory.executeFailed;

      case DbasSqliteErrorCode.bindPositionalFailed:
      case DbasSqliteErrorCode.bindNamedParameterNotFound:
      case DbasSqliteErrorCode.bindNamedFailed:
      case DbasSqliteErrorCode.unsupportedPositionalBindType:
      case DbasSqliteErrorCode.unsupportedNamedBindType:
        return DbasSqliteErrorCategory.bindFailed;

      case DbasSqliteErrorCode.setBusyTimeoutWriterFailed:
      case DbasSqliteErrorCode.setBusyTimeoutReaderFailed:
      case DbasSqliteErrorCode.enableWalFailed:
      case DbasSqliteErrorCode.beginTransactionFailed:
      case DbasSqliteErrorCode.commitFailed:
      case DbasSqliteErrorCode.commitBlockedByInFlightOperation:
      case DbasSqliteErrorCode.commitBlockedByActiveReader:
      case DbasSqliteErrorCode.commitRollbackAlsoFailed:
      case DbasSqliteErrorCode.rollbackFailed:
      case DbasSqliteErrorCode.transactionAlreadyActive:
      case DbasSqliteErrorCode.transactionRollbackAlsoFailed:
      case DbasSqliteErrorCode.vacuumInsideTransaction:
      case DbasSqliteErrorCode.vacuumFailed:
        return DbasSqliteErrorCategory.transactionFailed;

      case DbasSqliteErrorCode.readerAlreadyActive:
        return DbasSqliteErrorCategory.readerStateFailed;

      case DbasSqliteErrorCode.invalidDecimalFormat:
      case DbasSqliteErrorCode.invalidTimeFormat:
      case DbasSqliteErrorCode.invalidTimeComponent:
      case DbasSqliteErrorCode.invalidEnumIndex:
        return DbasSqliteErrorCategory.decodeFailed;

      case DbasSqliteErrorCode.openDbReopenWithDifferentPoolSize:
        return DbasSqliteErrorCategory.internal;
    }
  }
}

/// Fine-grained semantic interpretation of the underlying SQLite
/// result code. Derived from [DbasSqliteException.sqliteUniqueCode]
/// (the extended rc) when present, falling back to
/// [DbasSqliteException.sqliteCode] (the primary rc) otherwise.
/// Returns [DbasSqliteSubCategory.notApplicable] when both are `null`.
///
/// Both primary result codes (e.g. `SQLITE_BUSY=5`, `SQLITE_CONSTRAINT=19`)
/// and extended result codes (e.g. `SQLITE_CONSTRAINT_UNIQUE=2067`)
/// are mapped — extended codes take precedence over their primary
/// counterpart so a UNIQUE-index violation surfaces as [duplicatedData]
/// rather than the coarser [constraintViolation].
///
/// Use this to write `switch`-style recovery code that doesn't depend
/// on raw integer rc literals at the call site.
enum DbasSqliteSubCategory {
  /// The exception has neither `sqliteCode` nor `sqliteUniqueCode`
  /// (Dart-side failure) — the semantic interpretation does not apply.
  notApplicable,

  /// `SQLITE_ERROR` (1) — generic catch-all from the SQLite layer.
  genericError,

  /// `SQLITE_INTERNAL` (2) — an internal SQLite invariant was violated.
  internalError,

  /// `SQLITE_PERM` (3) — requested access mode not allowed.
  permissionDenied,

  /// `SQLITE_ABORT` (4) — operation aborted by callback.
  aborted,

  /// `SQLITE_BUSY` (5) — the database file is locked by another
  /// connection. Typically resolvable by retrying after a backoff.
  databaseBusy,

  /// `SQLITE_LOCKED` (6) — a table or shared-cache resource is locked
  /// by another statement on the same database connection. Distinct
  /// from `SQLITE_BUSY` ([databaseBusy]), which signals contention
  /// across separate connections.
  tableLocked,

  /// `SQLITE_NOMEM` (7) — malloc() failure inside SQLite.
  outOfMemory,

  /// `SQLITE_READONLY` (8) — write attempted on a read-only DB.
  readOnlyDatabase,

  /// `SQLITE_INTERRUPT` (9) — `sqlite3_interrupt()` cancelled the call.
  interrupted,

  /// `SQLITE_IOERR` (10) and any of its extended codes — disk I/O
  /// failed.
  ioError,

  /// `SQLITE_CORRUPT` (11) — the database file is malformed.
  corruptDatabase,

  /// `SQLITE_NOTFOUND` (12) — internal opcode/parameter not found.
  notFound,

  /// `SQLITE_FULL` (13) — disk is full (or a temp-store limit was hit).
  diskFull,

  /// `SQLITE_CANTOPEN` (14) and extended codes — could not open the
  /// DB file (path missing, permission denied, etc).
  cannotOpen,

  /// `SQLITE_PROTOCOL` (15) — WAL protocol violation; typically a
  /// retryable transient.
  protocolError,

  /// `SQLITE_EMPTY` (16) — historical; unused by modern SQLite.
  emptyDatabase,

  /// `SQLITE_SCHEMA` (17) — the database schema changed underneath a
  /// prepared statement.
  schemaChanged,

  /// `SQLITE_TOOBIG` (18) — a string/BLOB exceeded `SQLITE_MAX_LENGTH`.
  valueTooLarge,

  /// `SQLITE_CONSTRAINT` (19) — a constraint failed but the extended
  /// code didn't narrow it further (or wasn't reported). Use the
  /// other `constraint*`/`*Violation` values for specific cases.
  constraintViolation,

  /// `SQLITE_CONSTRAINT_UNIQUE` (2067), `_PRIMARYKEY` (1555), or
  /// `_ROWID` (2579) — a row with the same key already exists. This
  /// covers UNIQUE column constraints, explicit UNIQUE indexes, and
  /// PRIMARY KEY duplicates.
  duplicatedData,

  /// `SQLITE_CONSTRAINT_FOREIGNKEY` (787) — a FOREIGN KEY constraint
  /// would be violated.
  foreignKeyViolation,

  /// `SQLITE_CONSTRAINT_NOTNULL` (1299) — NULL written to a NOT NULL
  /// column.
  notNullViolation,

  /// `SQLITE_CONSTRAINT_CHECK` (275) — a CHECK constraint failed.
  checkViolation,

  /// `SQLITE_CONSTRAINT_TRIGGER` (1811) — a trigger raised RAISE(ABORT).
  triggerAborted,

  /// `SQLITE_CONSTRAINT_DATATYPE` (3091) — STRICT-table type check failed.
  dataTypeViolation,

  /// Other `SQLITE_CONSTRAINT_*` extended codes (`_FUNCTION` 1043,
  /// `_COMMITHOOK` 531, `_VTAB` 2323, `_PINNED` 2835).
  otherConstraintViolation,

  /// `SQLITE_MISMATCH` (20) — datatype mismatch (e.g. text→integer
  /// affinity coercion failed).
  typeMismatch,

  /// `SQLITE_MISUSE` (21) — wrapper-level API misuse (e.g. bind on a
  /// finalized statement). Indicates a bug, not a transient.
  misuse,

  /// `SQLITE_NOLFS` (22) — large-file support missing at OS level.
  noLargeFileSupport,

  /// `SQLITE_AUTH` (23) — authorization callback denied access.
  authorizationDenied,

  /// `SQLITE_FORMAT` (24) — historical; unused.
  formatError,

  /// `SQLITE_RANGE` (25) — bind index/name was out of range.
  rangeError,

  /// `SQLITE_NOTADB` (26) — file is not a SQLite database (bad header).
  notADatabase,

  /// `SQLITE_ROW` (100) / `SQLITE_DONE` (101) — these are success codes
  /// and should not normally surface on a `DbasSqliteException`, but if
  /// they ever do, they map here.
  stepStatus,

  /// `sqliteCode` is non-null but didn't match any known SQLite rc.
  other,
}

/// Internal: maps a SQLite primary or extended result code to a
/// [DbasSqliteSubCategory]. Pure function — exposed only via
/// [DbasSqliteException.subCategory].
DbasSqliteSubCategory _subCategoryFromRc(int rc) {
  // Extended-code mapping (always check first — extended codes share
  // their low byte with the primary code).
  switch (rc) {
    case 275: return DbasSqliteSubCategory.checkViolation;
    case 531: return DbasSqliteSubCategory.otherConstraintViolation;
    case 787: return DbasSqliteSubCategory.foreignKeyViolation;
    case 1043: return DbasSqliteSubCategory.otherConstraintViolation;
    case 1299: return DbasSqliteSubCategory.notNullViolation;
    case 1555: return DbasSqliteSubCategory.duplicatedData;
    case 1811: return DbasSqliteSubCategory.triggerAborted;
    case 2067: return DbasSqliteSubCategory.duplicatedData;
    case 2323: return DbasSqliteSubCategory.otherConstraintViolation;
    case 2579: return DbasSqliteSubCategory.duplicatedData;
    case 2835: return DbasSqliteSubCategory.otherConstraintViolation;
    case 3091: return DbasSqliteSubCategory.dataTypeViolation;
  }
  // Primary code mapping (low byte of any extended code is the primary
  // code; extended codes for SQLITE_IOERR / SQLITE_CANTOPEN fall through
  // to their primary categories below).
  final primary = rc & 0xFF;
  switch (primary) {
    case 1: return DbasSqliteSubCategory.genericError;
    case 2: return DbasSqliteSubCategory.internalError;
    case 3: return DbasSqliteSubCategory.permissionDenied;
    case 4: return DbasSqliteSubCategory.aborted;
    case 5: return DbasSqliteSubCategory.databaseBusy;
    case 6: return DbasSqliteSubCategory.tableLocked;
    case 7: return DbasSqliteSubCategory.outOfMemory;
    case 8: return DbasSqliteSubCategory.readOnlyDatabase;
    case 9: return DbasSqliteSubCategory.interrupted;
    case 10: return DbasSqliteSubCategory.ioError;
    case 11: return DbasSqliteSubCategory.corruptDatabase;
    case 12: return DbasSqliteSubCategory.notFound;
    case 13: return DbasSqliteSubCategory.diskFull;
    case 14: return DbasSqliteSubCategory.cannotOpen;
    case 15: return DbasSqliteSubCategory.protocolError;
    case 16: return DbasSqliteSubCategory.emptyDatabase;
    case 17: return DbasSqliteSubCategory.schemaChanged;
    case 18: return DbasSqliteSubCategory.valueTooLarge;
    case 19: return DbasSqliteSubCategory.constraintViolation;
    case 20: return DbasSqliteSubCategory.typeMismatch;
    case 21: return DbasSqliteSubCategory.misuse;
    case 22: return DbasSqliteSubCategory.noLargeFileSupport;
    case 23: return DbasSqliteSubCategory.authorizationDenied;
    case 24: return DbasSqliteSubCategory.formatError;
    case 25: return DbasSqliteSubCategory.rangeError;
    case 26: return DbasSqliteSubCategory.notADatabase;
    case 100: case 101: return DbasSqliteSubCategory.stepStatus;
  }
  return DbasSqliteSubCategory.other;
}

/// Single exception type thrown by the public API of [DbasSqlite],
/// [DbasSqliteStatement], and [DbasSqliteReader].
///
/// Choose the factory at the call site to encode the invariant:
///   * [DbasSqliteException.dart] — Dart-side failure with no native
///     SQLite result code (closed-state guards, format errors, timeouts,
///     queue cancellations). Both [sqliteCode] and [sqliteUniqueCode]
///     are `null`.
///   * [DbasSqliteException.sqlite] — native SQLite layer returned a
///     non-OK rc. [sqliteCode] carries the **primary** rc (e.g. `19`
///     for `SQLITE_CONSTRAINT`); [sqliteUniqueCode] is the **extended**
///     rc (e.g. `2067` for `SQLITE_CONSTRAINT_UNIQUE`) when the
///     platform exposes it, otherwise `null`.
///
/// [cause] / [causeStackTrace] are non-null when this exception wraps
/// an underlying failure (the rollback-after-failed-transaction path
/// and the rollback's own catch branch). Callers can inspect [cause]
/// to recover the original error's type and structured fields.
class DbasSqliteException implements Exception {
  final DbasSqliteErrorCode code;

  /// SQLite **primary** result code — the coarse category from
  /// `sqlite3_errcode` (e.g. `5` = `SQLITE_BUSY`, `19` =
  /// `SQLITE_CONSTRAINT`). `null` for `.dart` factory throws.
  final int? sqliteCode;

  /// SQLite **extended** result code — the unique discriminator
  /// within a primary code (e.g. `2067` = `SQLITE_CONSTRAINT_UNIQUE`,
  /// `787` = `SQLITE_CONSTRAINT_FOREIGNKEY`). `null` when the
  /// platform doesn't expose extended rcs (web today) or for `.dart`
  /// factory throws.
  final int? sqliteUniqueCode;

  final String message;
  final Object? cause;
  final StackTrace? causeStackTrace;

  const DbasSqliteException._(
    this.code,
    this.sqliteCode,
    this.sqliteUniqueCode,
    this.message, {
    this.cause,
    this.causeStackTrace,
  }) : assert(sqliteUniqueCode == null || sqliteCode != null,
            'sqliteUniqueCode is only meaningful when sqliteCode is also set');

  /// Dart-side failure: no native rc available.
  factory DbasSqliteException.dart(
    DbasSqliteErrorCode code,
    String message, {
    Object? cause,
    StackTrace? causeStackTrace,
  }) =>
      DbasSqliteException._(
        code,
        null,
        null,
        message,
        cause: cause,
        causeStackTrace: causeStackTrace,
      );

  /// Native SQLite failure. [sqliteCode] is the primary rc (required).
  /// [sqliteUniqueCode] is the extended rc when the platform resolved
  /// one; pass `null` when only the primary rc is known.
  ///
  /// Both native and web populate [sqliteUniqueCode] when the SQLite
  /// layer set it: native via `sqlite3_extended_errcode`, web via the
  /// worker's `extendedRc` envelope field. It is `null` when the
  /// failing call didn't update the connection's extended-rc slot
  /// (e.g. `sqlite3_bind_*` returning `SQLITE_RANGE` without queuing
  /// it on the connection).
  factory DbasSqliteException.sqlite(
    DbasSqliteErrorCode code,
    String message, {
    required int sqliteCode,
    int? sqliteUniqueCode,
    Object? cause,
    StackTrace? causeStackTrace,
  }) =>
      DbasSqliteException._(
        code,
        sqliteCode,
        sqliteUniqueCode,
        message,
        cause: cause,
        causeStackTrace: causeStackTrace,
      );

  /// Coarse-grained category for `switch`-based recovery branching.
  /// Equivalent to `code.category`.
  DbasSqliteErrorCategory get category => code.category;

  /// Fine-grained semantic interpretation. Prefers the extended rc
  /// ([sqliteUniqueCode]) when available — that's what carries the
  /// specific constraint kind (UNIQUE vs FOREIGNKEY vs NOTNULL etc.).
  /// Falls back to [sqliteCode] (primary) when no extended rc was
  /// resolved. Returns [DbasSqliteSubCategory.notApplicable] when
  /// neither is available.
  DbasSqliteSubCategory get subCategory {
    final rc = sqliteUniqueCode ?? sqliteCode;
    return rc == null
        ? DbasSqliteSubCategory.notApplicable
        : _subCategoryFromRc(rc);
  }

  @override
  String toString() {
    final buf = StringBuffer('DbasSqliteException(${code.name})');
    if (sqliteCode != null || sqliteUniqueCode != null) {
      buf.write(' [sqliteCode=$sqliteCode');
      if (sqliteUniqueCode != null) {
        buf.write(', sqliteUniqueCode=$sqliteUniqueCode');
      }
      buf.write(', ${subCategory.name}]');
    }
    buf.write(': $message');
    if (cause != null) {
      buf.write(' (cause: $cause)');
    }
    return buf.toString();
  }
}
