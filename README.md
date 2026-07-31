# dbas_sqlite

Flutter plugin that provides access to SQLite databases for Android, iOS, macOS, Linux, Windows and Web platforms.

> Previously published as `dbas_sqlite_flutter` (≤ 1.x) under the repository name `DBAS.SQLite.Flutter`. Both the pub package and GitHub repo now use `dbas_sqlite`.

## Features

### Cross-Platform Support
- **Android** - Native library integration
- **iOS** - xcframework with optimized performance  
- **macOS** - xcframework for both development and production
- **Linux** - Native shared libraries
- **Windows** - DLL integration
- **Web** - WASM SQLite via dedicated Web Worker with OPFS persistence

### Connection Pool (WAL Mode)
- `openDb()` automatically creates a C-level connection pool with 1 writer + 4 readers (configurable)
- WAL mode enables concurrent reads and writes
- Pool is fully transparent -- no API changes needed
- Reads automatically use pool readers; writes use the dedicated writer
- Falls back to single connection if pool creation fails or `readerPoolSize = 0`
- Native pool has mutex-protected reader acquire/release for thread safety
- The writer is pinned to `PRAGMA synchronous=FULL` and `PRAGMA wal_autocheckpoint=1` when it enters WAL, so **every commit folds the WAL back into the main `.db` file** — committed data is always present in the file a copy, a backup or `streamCopyDb` reads, instead of sitting in the `-wal`. Measured cost: about **+2.2 ms per commit** (~3.5× on a 2000-commit write loop). The cost is per *commit*, not per row, so batch bulk writes into a single transaction — N writes inside one transaction pay for one checkpoint. `closeDb()` and `streamCopyDb()` checkpoint on their own; `checkpoint()` is public for anything that reads the `.db` file by other means
- **Web** uses an equivalent multi-worker pool (1 writer + N reader Web Workers, each its own SQLite connection, coordinated via a `SharedArrayBuffer`-backed WAL SHM) so reads and writes run on separate connections just like native. **Requires the page to be cross-origin isolated** — see [Web Setup](#web-setup); without it the web side falls back to a single connection (no read/write concurrency)

### Thread Safety & Parallel Readers
- **Writer lock**: serializes all write operations (`executeSql`, transactions) on both web and native
- **Independent readers**: each `executeReader` call returns a `DbasSqliteReader` with its own pool connection -- multiple readers can be active simultaneously
- Pool readers are acquired non-blocking; if all are busy, the reader falls back to the writer connection
- Transactions hold the writer lock for their full duration
- Reads within a transaction use the writer connection to see uncommitted data
- Readers must be closed explicitly via `reader.close()` (or auto-closed when `readRow()` returns `false`) before the connection can be reused
- `closeDb()` automatically closes all active readers, then releases all locks and unblocks any pending operations -- no risk of use-after-free on lingering readers
- Closing a reader **waits for every in-flight `readRow()` step** before finalizing the statement, so `closeDb()` can never finalize a `sqlite3_stmt` a step is still running against (the C layer neither refuses nor blocks that -- it corrupts). A consumer mid-scan when this happens keeps the row its step already produced, and its next `readRow()` throws `readerClosedDuringScan` rather than reporting a truncated list as a complete one. This wait is **unbounded** -- a timeout could only expire into the corruption it prevents -- so it is not covered by the `kNativeOpDrainTimeoutMs` ceiling below; the reader emits a stall report every `DbasSqliteReader.kStepDrainStallReportMs` (5 s) instead
- **Wire `DbasSqlite.onDiagnostic` to receive that stall report.** It is the only diagnostic a wedged teardown ever produces, and the report's other sink (`dart:developer`) is dropped whenever no VM service client is subscribed -- i.e. in every release build on a device and every `flutter test` run, which is exactly where a production hang happens. `DbasSqlite.onDiagnostic = (message) => myLogger.warn(message);` once at app start is enough. The sink **must be synchronous** (`void Function(String)` accepts an `async` body, but its failure then escapes as an unhandled async error); a synchronous throw is caught rather than allowed to break the close, and the original message is re-emitted through `Zone.current.print` so a consumer whose logger died first still sees it
- `closeDb()` is **single-flight**: a second call issued while a teardown is in flight joins it and returns with its outcome, rather than reporting success while the pool is still being destroyed
- `closeDb()` **joins a close that is already in progress** rather than skipping it. `reader.close()` / `stmt.close()` latch `isClosed` synchronously and then suspend, so an un-awaited close (`unawaited(reader.close()); await db.closeDb();`) leaves a reader that reports itself closed while it still holds a pool connection. Both closes are join-idempotent and the statement sweep waits for them
- `closeDb()` also **waits** for any operation that has already crossed into native code, since neither the closing flag nor the wait-queue cancellations can recall one. Callers parked in Dart are still rejected immediately. In practice: closing a database with an un-awaited call still in flight takes as long as that call does. The wait is bounded by `kNativeOpDrainTimeoutMs` (30 s), after which `closeDb()` throws `closeDbNativeOpDrainTimeout` and leaves the connection **open** rather than destroying it under live native work
- **A caught `closeDbNativeOpDrainTimeout` does not hand back a usable connection.** It stays open but stays *marked closing*: every writer-lock acquire then throws `writerLockWaitCancelled` (`executeSql` outside a transaction, `executeScript`, `beginTransaction`, `checkpoint`, `vacuum`, `enableWal`), every reader-slot acquire throws `readerSlotWaitCancelled` (pooled `executeReader`), and `setBusyTimeout` throws too. `openDb()` will **not** clear it -- the connection is still open, so `openDb` returns early on its `isOpened()` guard. Any open transaction is gone as well, since `closeDb()` rolls back as its first step. The only supported next step is to await the outstanding work and call `closeDb()` again

### Background FFI Worker (Native)
- All heavy FFI operations (`executeSql`, `prepareQuery`, `readRow`, `openDb`, `closeDb`) run on a dedicated background isolate
- Column data is pre-fetched into a Dart-side cache after each `readRow` -- `getColumn*` calls are synchronous reads from Dart memory, not FFI round-trips
- Bind operations remain synchronous on the main isolate for performance

### True Streaming I/O (Web)
- **`attachStreamDb(stream)`**: Streams a database to OPFS chunk by chunk via `attachStreamBegin`/`attachStreamChunk`/`attachStreamEnd` with ACK-based backpressure. The complete file is never buffered in Dart memory -- critical for large databases (500 MB+)
- **`getContent()`**: Streams the database from OPFS chunk by chunk. Supports both Transferable Streams (Chrome/Firefox) and chunked postMessage fallback (Safari)
- **`streamCopyDb(destDbName)`**: Copies between OPFS files chunk by chunk

### Database Operations
- **Lifecycle**
  - `getInstance(dbName:)` - Get singleton instance for a database
  - `openDb({readerPoolSize})` - Open database with connection pool
  - `closeDb()` - Close database connection (rolls back any open transaction, waits for work already inside native code, closes all active readers, then folds the WAL into the main `.db` file)
  - `isOpened()` - Check connection status
  - `getAppDatabasePath()` - Get platform-specific database path
  - `databaseExists()` - Check if the database file exists
  - `dropDb()` - Delete the database file (including WAL and SHM)

- **Database Content**
  - `attachDb(bytes)` - Attach a database from raw bytes
  - `attachStreamDb(stream)` - Attach a database from a byte stream
  - `streamCopyDb(destDbName)` - Stream-copy database to a new name (checkpoints first, so the copy is self-contained — only the main `.db` file is copied)
  - `getContent()` - Get the raw bytes of the database file
  - `checkpoint()` - Fold committed WAL frames into the main `.db` file. Returns a `DbasSqliteCheckpointResult` (`busy`, `log`, `checkpointed`, `isComplete`). Read `isComplete` (`checkpointed == log`) — `busy` is **not** a success signal: a PASSIVE checkpoint that folds nothing because a reader pins the WAL still reports `busy: 0` and `SQLITE_OK`. An incomplete fold is recoverable, not an error: the pinned frames fold at the next opportunity

- **SQL Execution**
  - `prepareQuery(sql)` - Prepare **one** statement, returns a `DbasSqliteStatement`. Everything after the first `;` is silently discarded — no rc, no exception, no log — so a multi-statement string must go to `executeScript` instead
  - `executeSql(sql, {params, nameParams})` - Execute DDL/DML statements with optional positional or named parameters. One statement per call, same limit as `prepareQuery`
  - `executeReader(sql, {params, nameParams})` - Prepare a SELECT query, returns an independent `DbasSqliteReader`
  - `executeScript(sql)` - Run a whole multi-statement script via `sqlite3_exec`. The only call that executes more than one statement. No bindings (the text must be complete — never interpolate untrusted values), result rows are discarded, execution stops at the first failing statement with everything before it already applied, and there is **no atomicity across the script** unless you wrap it: `await db.transaction((tx) => tx.executeScript(sql))`. Returns the **last** row-changing statement's change count, not a total

- **Transactions**
  - `beginTransaction({strict = false})` - Begin a new transaction. Idempotent by default: if one is already active this joins it (no second hold on the writer lock). `strict: true` never joins — it parks on the writer-lock queue until the active transaction ends, then issues its own `BEGIN`. A `strict: true` call from the flow that already owns the transaction is a self-deadlock and fails with `writerLockWaitTimeout`
  - `startedCurrentTransaction` - Whether the last `beginTransaction()` issued a real `BEGIN` or took the join path. Read it right after the `await`, and use it to decide whether ending the transaction is your job: `if (db.startedCurrentTransaction) await db.commit();`
  - `commit()` - Commit the current transaction (idempotent). Pre-flights the writer connection first and throws `commitBlockedByInFlightOperation` / `commitBlockedByActiveReader` rather than committing out from under an unfinished write or an open writer-routed reader. There is no reference counting — a joiner's `commit()` ends the transaction for everyone
  - `rollback()` - Rollback the current transaction (idempotent). Drains in-flight writes first, then issues `ROLLBACK`
  - `transaction(action)` - Execute an action within a transaction with automatic commit/rollback
  - `isInTransaction` - Check if a transaction is currently active

### Data Retrieval (`DbasSqliteReader`)
- `readRow()` - Advance to next row (`true` if available, `false` and auto-closes when done). **`false` means "no more rows" and nothing else**: a reader closed for any other reason -- an explicit `close()`, a `DbasSqliteStatement.close()`, `closeDb()`'s statement sweep, or an earlier failed step -- throws `readerClosedDuringScan` instead, so a scan cut short by teardown can never be mistaken for one that ran out of rows
- `readRows([amount = 50])` - Read up to `amount` rows in one call. Returns a record `({rows, hasMore})` where `rows` is `List<Map<String, ColumnData>>` (column name → typed `ColumnData`) and `hasMore` is the result of the last `readRow` (`true` = more rows may follow, `false` = result set exhausted). It is a plain loop over `readRow()`, so it inherits the throw above: a batch cut short by a close raises `readerClosedDuringScan` and **the rows gathered so far are discarded**, rather than being reported as a completed batch
- `close()` - Manually close the reader and release its connection
- **Column Access** (with nullable variants)
  - `getColumnText(index)` / `getColumnNullableText(index)` - Get string value
  - `getColumnInt(index)` / `getColumnNullableInt(index)` - Get integer value
  - `getColumnBool(index)` / `getColumnNullableBool(index)` - Get boolean value
  - `getColumnDouble(index)` / `getColumnNullableDouble(index)` - Get double value
  - `getColumnDecimal(index)` / `getColumnNullableDecimal(index)` - Get decimal value
  - `getColumnDateTime(index)` / `getColumnNullableDateTime(index)` - Get DateTime value
  - `getColumnTime(index)` / `getColumnNullableTime(index)` - Get Duration value
  - `getColumnEnum<T>(index, values)` / `getColumnNullableEnum<T>(index, values)` - Get enum value
  - `getColumnBlob(index)` / `getColumnNullableBlob(index)` - Get binary data
  - `getColumnValue(index)` - Get typed value based on SQLite column type
  - `isColumnNull(index)` - Check if column is NULL
  - `getColumnType(index)` - Get column data type
  - `getColumnName(index)` - Get column name
  - `getColumnCount()` - Get number of columns

### Query Information
- `getLastInsertedId()` - Get last inserted row ID

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  dbas_sqlite: ^2.9.0
```

Or install with the Dart CLI:

```bash
flutter pub add dbas_sqlite
```

## Setup

### Mobile & Desktop
No additional setup required. Native libraries are automatically bundled.

### Web Setup
The WASM module and Web Workers are bundled as Flutter assets and loaded automatically by the plugin. Requires a modern browser with OPFS persistence (Chrome 86+, Firefox 111+, Safari 15.2+) served over HTTPS or localhost.

**Cross-origin isolation is required for the connection pool.** The web pool runs multiple Web Workers that share WAL state through a `SharedArrayBuffer`, which the browser only exposes when the document is *cross-origin isolated* (`crossOriginIsolated === true`). Enable it one of two ways:

1. **Server headers (recommended for production):** serve the app with
   `Cross-Origin-Opener-Policy: same-origin` and
   `Cross-Origin-Embedder-Policy: require-corp` on the document response.

2. **`coi-serviceworker.js` (works on any host, including `flutter run` — no header config):**
   the plugin ships this file at `web/libs/coi-serviceworker.js`, and
   `scripts/sqlite/sync_sqlite_lib.(ps1|sh)` copies it to the example's web
   root. For your own app, copy `coi-serviceworker.js` into your `web/`
   folder and load it **first** in `web/index.html`, as a plain
   (non-`async`) script before the Flutter bootstrap:

   ```html
   <head>
     <base href="$FLUTTER_BASE_HREF">
     <meta charset="UTF-8">
     <script src="coi-serviceworker.js"></script>
     <!-- ...rest of head... -->
   </head>
   ```

   It registers a service worker that injects the COOP/COEP headers and
   reloads the page once. For **release** builds, build with
   `flutter build web --pwa-strategy=none` so Flutter's own service worker
   doesn't take the root scope from it.

> **Why the web root?** A service worker can only control pages within its
> own URL path, and isolation must apply to the root document — so
> `coi-serviceworker.js` must be served from `/coi-serviceworker.js`, not
> as a deep `/assets/packages/...` package asset.

If the page is **not** cross-origin isolated, the plugin logs the reason (channel `dbas_sqlite.lifecycle`) and falls back to a single Web Worker (one SQLite connection). The app still runs, but loses read/write concurrency — a write blocked behind an open read cursor can fail with `SQLITE_BUSY`.

## Usage

### Basic Example

```dart
import 'package:dbas_sqlite/dbas_sqlite.dart';

// Get database instance
final db = await DbasSqlite.getInstance(dbName: 'myapp.db');

// Open database (creates pool with WAL mode)
await db.openDb();

// Create table
final createStmt = await db.prepareQuery('''
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER
  )
''');
try {
  await createStmt.executeSql();
} finally {
  await createStmt.close();
}

// Insert with positional parameters
final insertStmt = await db.prepareQuery(
  'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
);
try {
  await insertStmt.executeSql(params: ['John Doe', 'john@example.com', 30]);
  print('Inserted user with id: ${insertStmt.getLastInsertedId()}');
} finally {
  await insertStmt.close();
}

// Query data
final selectStmt = await db.prepareQuery('SELECT * FROM users WHERE age > ?');
try {
  final reader = await selectStmt.executeReader(params: [25]);
  try {
    while (await reader.readRow()) {
      final name = reader.getColumnText(0);
      final email = reader.getColumnNullableText(1);
      final age = reader.getColumnInt(2);
      print('$name ($email) - age: $age');
    }
  } finally {
    await reader.close();
  }
} finally {
  await selectStmt.close();
}

await db.closeDb();
```

### Statement Reuse

A prepared statement can be executed many times with different params —
the bind buffer is replayed against a fresh native handle on each call:

```dart
final stmt = await db.prepareQuery('INSERT INTO users (name) VALUES (?)');
try {
  for (final name in ['Alice', 'Bob', 'Carol']) {
    await stmt.executeSql(params: [name]);
  }
} finally {
  await stmt.close();
}
```

### Named Parameters

```dart
final stmt = await db.prepareQuery(
  'INSERT INTO users (name, email, age) VALUES (:name, :email, :age)',
);
try {
  await stmt.executeSql(nameParams: {
    'name': 'Jane Smith',
    'email': 'jane@example.com',
    'age': 28,
  });
} finally {
  await stmt.close();
}

// By default, extra named parameters not present in the SQL are silently
// ignored (C#/SQLite behavior). To throw instead:
db.throwOnMissingNamedParams = true;
```

### Rich Types

```dart
import 'package:decimal/decimal.dart';

// Supports Decimal, bool, DateTime, Duration, Enum and Blob
final insStmt = await db.prepareQuery(
  'INSERT INTO products (name, price, active) VALUES (?, ?, ?)',
);
try {
  await insStmt.executeSql(
    params: ['Widget', Decimal.parse('19.99'), true],
  );
} finally {
  await insStmt.close();
}

final selStmt = await db.prepareQuery('SELECT name, price, active FROM products');
try {
  final reader = await selStmt.executeReader();
  try {
    while (await reader.readRow()) {
      final name = reader.getColumnText(0);
      final price = reader.getColumnDecimal(1);
      final active = reader.getColumnBool(2);
      print('$name - \$$price (active: $active)');
    }
  } finally {
    await reader.close();
  }
} finally {
  await selStmt.close();
}
```

### Transactions

```dart
// Automatic commit/rollback
await db.transaction((tx) async {
  final stmt = await tx.prepareQuery('INSERT INTO users (name) VALUES (?)');
  try {
    await stmt.executeSql(params: ['Alice']);
    await stmt.executeSql(params: ['Bob']);
  } finally {
    await stmt.close();
  }
});

// Manual control
await db.beginTransaction();
try {
  final stmt = await db.prepareQuery('INSERT INTO users (name) VALUES (?)');
  try {
    await stmt.executeSql(params: ['Alice']);
  } finally {
    await stmt.close();
  }
  await db.commit();
} catch (_) {
  await db.rollback();
  rethrow;
}

// A helper that may or may not be called inside someone else's
// transaction: begin, then only end what you actually started.
await db.beginTransaction();
final owned = db.startedCurrentTransaction; // read before the next await
try {
  // ...writes...
  if (owned) await db.commit();
} catch (_) {
  if (owned) await db.rollback();
  rethrow;
}
```

`commit()` pre-flights the writer connection: every write started inside
the transaction must be awaited, and every reader opened inside it (once
the transaction has written, reads route to the writer connection for
read-your-writes) must be closed, before `commit()` is called. Otherwise
it throws `commitBlockedByInFlightOperation` / `commitBlockedByActiveReader`
and leaves the transaction untouched — committing there would end the
transaction and hand the writer lock to the next waiter while that work
is still running on the connection. Neither error is transient: finish
the work, then commit again. `rollback()` does not throw for this — it
drains in-flight writes and then rolls back, because it is also the
cleanup path used by `closeDb()`.

### Multi-Statement Scripts

`prepareQuery` / `executeSql` run exactly **one** statement — everything
after the first `;` is dropped at prepare time, silently and with a
success return. Use `executeScript` for anything that holds more than
one statement (runtime DDL plus its indexes, a migration step, a pragma
block):

```dart
// Not atomic on its own — wrap it when it must be all-or-nothing.
await db.transaction((tx) => tx.executeScript('''
  CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id),
    total TEXT NOT NULL
  );
  CREATE UNIQUE INDEX IF NOT EXISTS ix_orders_user ON orders (user_id, id);
  PRAGMA foreign_keys = ON;
'''));
```

The script takes no bindings (`sqlite3_exec` has no bind surface), so
build parameterised SQL with `prepareQuery` instead and never
interpolate untrusted values into a script. Result rows are discarded —
a `SELECT` in a script runs and yields nothing. Execution stops at the
first statement that fails, and every statement before it has already
run; outside a transaction those are already committed and nothing can
take them back. The return value is the change count of the **last**
row-changing statement, not a total for the script.

### WAL & Checkpoints

Every commit folds the WAL into the main `.db` file, and `closeDb()` /
`streamCopyDb()` fold on their own, so committed data is in the file
that a copy or backup reads. When you read, copy or ship the `.db` file
by other means, checkpoint explicitly and check the result:

```dart
final result = await db.checkpoint();
if (!result.isComplete) {
  // A reader is pinning a WAL snapshot: `result.log - result.checkpointed`
  // frames stay in the -wal and fold at the next opportunity. Nothing
  // committed is lost, but a copy of the main .db file alone would be
  // missing them right now — close in-flight readers and try again.
}
```

`isComplete` (`checkpointed == log`) is the only honest test.
`result.busy` is not: a PASSIVE checkpoint that folds nothing still
reports `busy: 0` and success. `checkpoint()` throws
`checkpointInsideTransaction` if a transaction is open — SQLite refuses
to checkpoint a connection holding one, so it would fold nothing.

### Connection Pool

```dart
// Default: 4 readers (WAL mode)
await db.openDb();

// Custom pool size
await db.openDb(readerPoolSize: 8);

// Single connection (no pool)
await db.openDb(readerPoolSize: 0);
```

### Stream Copy

```dart
// Copy database to a backup
await db.streamCopyDb('myapp_backup.db');
```

### Error Handling

All public API calls on `DbasSqlite`, `DbasSqliteStatement`, and
`DbasSqliteReader` throw a single `DbasSqliteException` on failure
(implements `Exception`). It carries:

- `DbasSqliteErrorCode code` — stable, one unique value per throw
  site (e.g. `bindPositionalFailed`, `statementClosed`,
  `transactionAlreadyActive`). Useful for telemetry IDs and test
  assertions.
- `int? sqliteCode` — SQLite **primary** result code (e.g. `19` for
  `SQLITE_CONSTRAINT`, `5` for `SQLITE_BUSY`). `null` for Dart-side
  failures.
- `int? sqliteUniqueCode` — SQLite **extended** result code, the
  unique discriminator within a primary code (e.g. `2067` for
  `SQLITE_CONSTRAINT_UNIQUE`, `787` for `SQLITE_CONSTRAINT_FOREIGNKEY`).
  `null` when the platform didn't queue an extended rc.
- `String message` — human-readable description.
- `Object? cause` / `StackTrace? causeStackTrace` — non-`null` when
  the exception wraps an underlying failure (the rollback-failed
  paths of `rollback()` / `commit()` / `transaction()`).

Two derived enums make recovery branching ergonomic:

- **`DbasSqliteErrorCategory`** (coarse) — `notOpened`,
  `busyOrCancelled`, `prepareFailed`, `executeFailed`,
  `bindFailed`, `transactionFailed`, `readerStateFailed`,
  `decodeFailed`, `internal`.
- **`DbasSqliteSubCategory`** (fine, SQLite-aware) — derived from
  `sqliteUniqueCode ?? sqliteCode`, so the extended rc wins over the
  primary. Maps to semantic values: `databaseBusy`, `tableLocked`,
  `duplicatedData` (UNIQUE/PRIMARY KEY/ROWID violations),
  `foreignKeyViolation`, `notNullViolation`, `checkViolation`,
  `corruptDatabase`, `diskFull`, `readOnlyDatabase`, `rangeError`,
  `valueTooLarge`, and ~20 more.

```dart
try {
  await stmt.executeSql(params: ['alice@example.com']);
} on DbasSqliteException catch (e) {
  switch (e.subCategory) {
    case DbasSqliteSubCategory.duplicatedData:
      // Email already exists — show "use a different address".
      break;
    case DbasSqliteSubCategory.foreignKeyViolation:
      // Referenced row missing — abort or repair.
      break;
    case DbasSqliteSubCategory.databaseBusy:
      // Transient — retry after a backoff.
      break;
    default:
      // Inspect e.code, e.sqliteCode, e.sqliteUniqueCode, e.message,
      // and e.cause for the rest. For wrapped failures
      // (transactionRollbackAlsoFailed), e.cause holds the original
      // exception with its own code/subCategory.
      rethrow;
  }
}
```

`DbasSqliteStatement` also exposes `getLastErrorCode()` and
`getLastUniqueErrorCode()` — `int?` accessors parallel to the
existing `getLastError()` — populated after every `executeSql` /
`executeReader`. Useful when an exception is routed through a
generic handler and the statement is later inspected for telemetry
without rethrowing.

`openDb()` is idempotent — a second call on an already-open
instance is a no-op. Calling it with a different `readerPoolSize`
than the original throws `DbasSqliteException` with code
`openDbReopenWithDifferentPoolSize`.

## Minimum Platform Versions

| Platform | Minimum Version |
|----------|----------------|
| Android  | API 35         |
| iOS      | 16.0           |
| macOS    | 13.0 (Ventura) |
| Linux    | x86_64         |
| Windows  | x86_64         |
| Web      | Modern browsers with OPFS support |

## Platform Notes

- **iOS/macOS**: Uses xcframework for optimal performance
- **Android**: Native library automatically included (NDK r29)
- **Windows/Linux**: Dynamic libraries bundled with app
- **Web**: WASM module runs in a dedicated Web Worker with OPFS persistence. All heavy operations are serialized through the worker — column data is cached in Dart memory after each `readRow` for synchronous access

## License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.
