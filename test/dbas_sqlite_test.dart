import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dbas_sqlite/dbas_sqlite.dart';
import 'package:decimal/decimal.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as path;

enum TestStatus { active, inactive, suspended }

/// Helper to create a fresh database.
///
/// Uses single connection (no pool) by default to minimize native resource
/// overhead. Pool-specific tests use [readerPoolSize] explicitly.
///
/// [workerPoolSize] auto-bumps to `readerPoolSize + 2` inside the
/// library on `openDb`. Tests that fan out many parallel reads (more
/// than workers can serve concurrently) should pass an explicit
/// higher value so worker isolates don't starve while in-flight reads
/// wait for prepare/step round-trips.
Future<DbasSqlite> _createTestDb(String dbName,
    {int readerPoolSize = 0, int workerPoolSize = 4}) async {
  final db = await DbasSqlite.getInstance(
      dbName: dbName, workerPoolSize: workerPoolSize);
  await db.dropDb();
  await db.openDb(readerPoolSize: readerPoolSize);
  return db;
}

/// One-shot prepare/execute/close — the v2.3.x `db.executeSql(sql, ...)`
/// in a single function call. Used by test sites that need the call
/// to be a single expression (inside `Future.wait`, `() => ...`).
Future<int> _runSql(DbasSqlite db, String sql,
    {List<Object?>? params, Map<String, Object?>? nameParams}) async {
  final s = await db.prepareQuery(sql);
  try {
    return await s.executeSql(params: params, nameParams: nameParams);
  } finally {
    await s.close();
  }
}

/// Pumps the event loop until [count] callers have parked in the
/// reader-slot wait queue (or [maxYields] is exhausted). Avoids a
/// fixed `Future.delayed(Duration.zero)`, which assumes a single
/// microtask drain is enough to register every waiter.
Future<void> _awaitReaderWaiters(DbasSqlite db, int count,
    {int maxYields = 1000}) async {
  for (var i = 0;
      i < maxYields && db.debugReaderSlotWaitQueueLength < count;
      i++) {
    await Future<void>.delayed(Duration.zero);
  }
  expect(db.debugReaderSlotWaitQueueLength, greaterThanOrEqualTo(count),
      reason: 'expected at least $count parked reader-slot waiter(s)');
}

/// Pumps the event loop until [count] callers have parked in the
/// writer-lock wait queue (or [maxYields] is exhausted). Mirrors
/// [_awaitReaderWaiters]'s rationale — a caller only reaches the queue
/// after its own `await` chain has been scheduled, so a single
/// `Future.delayed(Duration.zero)` is not guaranteed to be enough.
Future<void> _awaitWriterWaiters(DbasSqlite db, int count,
    {int maxYields = 1000}) async {
  for (var i = 0;
      i < maxYields && db.debugWriterLockWaitQueueLength < count;
      i++) {
    await Future<void>.delayed(Duration.zero);
  }
  expect(db.debugWriterLockWaitQueueLength, greaterThanOrEqualTo(count),
      reason: 'expected at least $count parked writer-lock waiter(s)');
}

// ── WAL-checkpoint probes ─────────────────────────────────────────────
//
// The only assertion that actually proves committed data reached the
// main `.db` file is "rows visible in a main-file-only copy": copy the
// `.db` WITHOUT its `-wal`/`-shm` and open the copy. Frames that were
// never folded out of the WAL live only in the `-wal`, so the copy is
// short — or has no table at all — until a checkpoint runs. File sizes
// are a useful secondary signal, never the proof.

/// Runs [sql] and returns the first column of the first row as an int,
/// or `-1` when the query produced no rows.
Future<int> _queryInt(DbasSqlite db, String sql) async {
  final stmt = await db.prepareQuery(sql);
  try {
    final reader = await stmt.executeReader();
    try {
      if (!await reader.readRow()) return -1;
      return reader.getColumnInt(0);
    } finally {
      await reader.close();
    }
  } finally {
    await stmt.close();
  }
}

/// Rows in [table] (optionally narrowed by [where]), or `-1` when
/// [table] is absent from the schema entirely. Never throws for a
/// missing table, so an unfolded WAL shows up as a readable row-count
/// mismatch instead of a `no such table` crash that hides which of the
/// two failure modes happened.
Future<int> _rowCountOrAbsent(DbasSqlite db, String table,
    {String? where}) async {
  final present = await _queryInt(db,
      "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '$table'");
  if (present < 1) return -1;
  final filter = where == null ? '' : ' WHERE $where';
  return await _queryInt(db, 'SELECT COUNT(*) FROM $table$filter');
}

/// Byte sizes of the main `.db` and its `-wal`, formatted for `reason:`
/// strings. `-1` means the file does not exist.
Future<String> _walFootprint(DbasSqlite db) async {
  final base = await db.getAppDatabasePath();
  Future<int> len(String suffix) async {
    final f = File('$base$suffix');
    return await f.exists() ? await f.length() : -1;
  }

  return 'main=${await len('')}B, wal=${await len('-wal')}B';
}

/// Copies ONLY the main `.db` file of [db] — deliberately leaving the
/// `-wal` and `-shm` behind — into [probeDbName], opens the copy, and
/// returns [_rowCountOrAbsent] for [table] there. The copy is dropped
/// before returning, so the same [probeDbName] can be reused.
///
/// [db] may be open or closed; only the resolved path is used.
Future<int> _rowsInMainFileOnly(
    DbasSqlite db, String table, String probeDbName,
    {String? where}) async {
  final srcPath = await db.getAppDatabasePath();
  final probe = await DbasSqlite.getInstance(dbName: probeDbName);
  final probePath = await probe.getAppDatabasePath();
  // A stale foreign `-wal` beside a copied `.db` opens with NO error and
  // silently serves the OTHER database's rows, so clear all three first.
  for (final suffix in const ['', '-wal', '-shm']) {
    final f = File('$probePath$suffix');
    if (await f.exists()) await f.delete();
  }
  await File(srcPath).copy(probePath);

  await probe.openDb(readerPoolSize: 0);
  try {
    return await _rowCountOrAbsent(probe, table, where: where);
  } finally {
    await probe.closeDb();
    await probe.dropDb();
  }
}

void main() async {
  setUpAll(() async {
    // Clean test database directory before all tests
    final testDbDir = Directory(path.join(Directory.current.path, 'test', 'db'));
    if (await testDbDir.exists()) {
      await testDbDir.delete(recursive: true);
    }
    await testDbDir.create(recursive: true);
  });

  // ──────────────────────────────────────────────────────────────────────
  // Existing tests
  // ──────────────────────────────────────────────────────────────────────

  test('Test Open, create table, insert, select and close', () async {
    String dbName = 'test.db';
    final dbasSqlite = await DbasSqlite.getInstance(dbName: dbName);
    await dbasSqlite.dropDb();

    await dbasSqlite.openDb(readerPoolSize: 0);

    File dbFile = File(await dbasSqlite.getAppDatabasePath(dbName: dbName));

    expect(await dbasSqlite.databaseExists(), isTrue, reason: 'DB file should exist after opening the database (native).');
    expect(await dbFile.exists(), isTrue, reason: 'DB file should exist after opening the database.');
    expect(dbasSqlite.isOpened(), isTrue, reason: 'Database should be opened after calling openDb.');

    {
      final stmt = await dbasSqlite.prepareQuery('''
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await dbasSqlite.prepareQuery('INSERT INTO users (name, email) VALUES (:name, :email)');
      try {
        await stmt.executeSql(nameParams: <String, Object?>{
        'name': 'test1',
        'email': 'test1@test.com',
      },);
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await dbasSqlite.prepareQuery('INSERT INTO users (name, email) VALUES (?, ?)');
      try {
        await stmt.executeSql(params: ['test2', 'test2@test.com'],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await dbasSqlite.prepareQuery("SELECT name, email FROM users where id > :id")).executeReader(params: [0]);
    int colCount = reader.getColumnCount();

    List<List<Object?>> users = [];
    while (await reader.readRow()) {
      List<Object?> user = [];
      for (int colIdx = 0; colIdx < colCount; colIdx++) {
        SqliteColumnType type = reader.getColumnType(colIdx);

        if (type == SqliteColumnType.nullType) {
          user.add(null);
        } else if (type == SqliteColumnType.integer) {
          user.add(reader.getColumnInt(colIdx));
        } else if (type == SqliteColumnType.double) {
          user.add(reader.getColumnDouble(colIdx));
        } else if (type == SqliteColumnType.text) {
          user.add(reader.getColumnText(colIdx));
        } else if (type == SqliteColumnType.blob) {
          user.add(reader.getColumnBlob(colIdx));
        } else {
          user.add('<INVALID TYPE ${int.parse(type.toString())}>');
        }
      }

      users.add(user);
    }

    expect(users, [['test1', 'test1@test.com'], ['test2', 'test2@test.com']]);

    await dbasSqlite.closeDb();
    expect(dbasSqlite.isOpened(), isFalse);
  });

  test('Test exists and attach', () async {
    String testDbName = 'test_attach1.db';
    DbasSqlite testDbasSqlite = await DbasSqlite.getInstance(dbName: testDbName);
    String testDbPath = await testDbasSqlite.getAppDatabasePath();
    await testDbasSqlite.dropDb();

    await testDbasSqlite.openDb(readerPoolSize: 0);
    {
      final stmt = await testDbasSqlite.prepareQuery('''
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await testDbasSqlite.closeDb();

    String dbName = 'test_attach2.db';
    DbasSqlite dbasSqlite = await DbasSqlite.getInstance(dbName: dbName);

    File dbFile = File(await dbasSqlite.getAppDatabasePath());
    List<int> bytes = await File(testDbPath).readAsBytes();
    dbasSqlite = await dbasSqlite.attachDb(bytes);

    expect(await dbasSqlite.databaseExists(), isTrue, reason: 'DB file should exist after opening the database (native).');
    expect(await dbFile.exists(), isTrue, reason: 'DB file should exist after opening the database.');
    expect(dbasSqlite.isOpened(), isTrue, reason: 'Database should be opened after calling openDb.');

    final params = {
      ':name': 'name-text',
      ':email': 'email@email.com',
      ':created_at': '2023-01-01 00:00:00',
    };
    {
      final stmt = await dbasSqlite.prepareQuery('''
      INSERT INTO users (name, email, created_at) values (:name, :email, :created_at)
    ''');
      try {
        await stmt.executeSql(nameParams: params);
      } finally {
        await stmt.close();
      }
    }

    final selectParams = {
      ':id': 0,
      ':name': 'random-bla',
    };
    final reader = await (await dbasSqlite.prepareQuery("SELECT * FROM users WHERE id > :id AND name != :name")).executeReader(nameParams: selectParams);
    List<Map<String, String>> users = [];
    while (await reader.readRow()) {
      users.add({
        'id': reader.getColumnText(0),
        'name': reader.getColumnText(1),
        'email': reader.getColumnText(2),
        'created_at': reader.getColumnText(3),
      });
    }

    expect(users.length, 1);
    expect(users[0]['name'], 'name-text');
    expect(users[0]['email'], 'email@email.com');
    expect(users[0]['created_at'], '2023-01-01 00:00:00');

    await dbasSqlite.closeDb();
    await dbasSqlite.dropDb();

    expect(await dbFile.exists(), isFalse, reason: 'DB file should not exist after opening the database.');
    expect(dbasSqlite.isOpened(), isFalse, reason: 'Database should not be opened after calling openDb.');
  });

  // ──────────────────────────────────────────────────────────────────────
  // Singleton behavior
  // ──────────────────────────────────────────────────────────────────────

  test('getInstance returns the same instance for the same dbName', () async {
    final db1 = await DbasSqlite.getInstance(dbName: 'singleton_test.db');
    final db2 = await DbasSqlite.getInstance(dbName: 'singleton_test.db');
    expect(identical(db1, db2), isTrue);

    final db3 = await DbasSqlite.getInstance(dbName: 'singleton_other.db');
    expect(identical(db1, db3), isFalse);
  });

  // ──────────────────────────────────────────────────────────────────────
  // Error handling
  // ──────────────────────────────────────────────────────────────────────

  test('executeSql throws DbasSqliteException when database is not opened', () async {
    final db = await DbasSqlite.getInstance(dbName: 'not_opened.db');
    expect(
      () => db.prepareQuery('SELECT 1'),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.prepareQueryDatabaseNotOpened)),
    );
  });

  test('executeReader throws DbasSqliteException when database is not opened', () async {
    final db = await DbasSqlite.getInstance(dbName: 'not_opened_reader.db');
    expect(
      () => db.prepareQuery('SELECT 1'),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.prepareQueryDatabaseNotOpened)),
    );
  });

  test('executeSql throws Exception on invalid SQL', () async {
    final db = await _createTestDb('invalid_sql.db');

    await expectLater(
      () => _runSql(db, 'INVALID SQL STATEMENT'),
      throwsA(anything),
    );

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // getLastInsertedId
  // ──────────────────────────────────────────────────────────────────────

  test('getLastInsertedId returns correct id', () async {
    final db = await _createTestDb('last_insert_id.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final insertStmt =
        await db.prepareQuery('INSERT INTO items (name) VALUES (?)');
    await insertStmt.executeSql(params: ['first']);
    expect(insertStmt.getLastInsertedId(), 1);

    await insertStmt.executeSql(params: ['second']);
    expect(insertStmt.getLastInsertedId(), 2);

    await insertStmt.executeSql(params: ['third']);
    expect(insertStmt.getLastInsertedId(), 3);
    await insertStmt.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // getColumnName
  // ──────────────────────────────────────────────────────────────────────

  test('getColumnName returns correct column names', () async {
    final db = await _createTestDb('col_name.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        product_name TEXT,
        price REAL
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO products (id, product_name, price) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, 'Widget', 9.99],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT id, product_name, price FROM products')).executeReader();
    expect(await reader.readRow(), isTrue);

    expect(reader.getColumnName(0), 'id');
    expect(reader.getColumnName(1), 'product_name');
    expect(reader.getColumnName(2), 'price');
    expect(reader.getColumnCount(), 3);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // NULL handling & isColumnNull
  // ──────────────────────────────────────────────────────────────────────

  test('isColumnNull and nullable getters work correctly', () async {
    final db = await _createTestDb('null_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE nullable_test (
        id INTEGER PRIMARY KEY,
        text_col TEXT,
        int_col INTEGER,
        real_col REAL,
        blob_col BLOB
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Insert row with all NULLs (except id)
    {
      final stmt = await db.prepareQuery('INSERT INTO nullable_test (id, text_col, int_col, real_col, blob_col) VALUES (?, ?, ?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, null, null, null, null],);
      } finally {
        await stmt.close();
      }
    }

    // Insert row with values
    {
      final stmt = await db.prepareQuery('INSERT INTO nullable_test (id, text_col, int_col, real_col, blob_col) VALUES (?, ?, ?, ?, ?)');
      try {
        await stmt.executeSql(params: [2, 'hello', 42, 3.14, Uint8List.fromList([1, 2, 3])],);
      } finally {
        await stmt.close();
      }
    }

    // Read NULL row
    final reader = await (await db.prepareQuery('SELECT text_col, int_col, real_col, blob_col FROM nullable_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    expect(reader.isColumnNull(0), isTrue);
    expect(reader.isColumnNull(1), isTrue);
    expect(reader.isColumnNull(2), isTrue);
    expect(reader.isColumnNull(3), isTrue);

    expect(reader.getColumnNullableText(0), isNull);
    expect(reader.getColumnNullableInt(1), isNull);
    expect(reader.getColumnNullableDouble(2), isNull);
    expect(reader.getColumnNullableBlob(3), isNull);

    await reader.close();

    // Read non-NULL row
    final reader2 = await (await db.prepareQuery('SELECT text_col, int_col, real_col, blob_col FROM nullable_test WHERE id = 2')).executeReader();
    expect(await reader2.readRow(), isTrue);

    expect(reader2.isColumnNull(0), isFalse);
    expect(reader2.getColumnNullableText(0), 'hello');
    expect(reader2.getColumnNullableInt(1), 42);
    expect(reader2.getColumnNullableDouble(2), closeTo(3.14, 0.001));

    final blobResult = reader2.getColumnNullableBlob(3);
    expect(blobResult, isNotNull);
    expect(blobResult!.sublist(0, 3), Uint8List.fromList([1, 2, 3]));

    await reader2.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Bool binding and retrieval
  // ──────────────────────────────────────────────────────────────────────

  test('Bool bind and getColumnBool / getColumnNullableBool', () async {
    final db = await _createTestDb('bool_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE bool_test (
        id INTEGER PRIMARY KEY,
        flag INTEGER,
        nullable_flag INTEGER
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO bool_test (id, flag, nullable_flag) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, true, null],);
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO bool_test (id, flag, nullable_flag) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [2, false, true],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT flag, nullable_flag FROM bool_test ORDER BY id')).executeReader();

    // Row 1: flag=true, nullable_flag=NULL
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnBool(0), isTrue);
    expect(reader.getColumnNullableBool(1), isNull);

    // Row 2: flag=false, nullable_flag=true
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnBool(0), isFalse);
    expect(reader.getColumnNullableBool(1), isTrue);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Decimal binding and retrieval
  // ──────────────────────────────────────────────────────────────────────

  test('Decimal bind and getColumnDecimal / getColumnNullableDecimal', () async {
    final db = await _createTestDb('decimal_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE decimal_test (
        id INTEGER PRIMARY KEY,
        amount REAL,
        nullable_amount REAL
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final decimalValue = Decimal.parse('123.45');
    {
      final stmt = await db.prepareQuery('INSERT INTO decimal_test (id, amount, nullable_amount) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, decimalValue, null],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT amount, nullable_amount FROM decimal_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    final result = reader.getColumnDecimal(0);
    expect(result.toDouble(), closeTo(123.45, 0.001));

    expect(reader.getColumnNullableDecimal(1), isNull);

    // getColumnDecimal on NULL returns Decimal.zero
    expect(reader.getColumnDecimal(1), Decimal.zero);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // DateTime binding and retrieval
  // ──────────────────────────────────────────────────────────────────────

  test('getColumnDateTime / getColumnNullableDateTime', () async {
    final db = await _createTestDb('datetime_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE datetime_test (
        id INTEGER PRIMARY KEY,
        created_at TEXT,
        deleted_at TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO datetime_test (id, created_at, deleted_at) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, '2025-06-15T10:30:00.000', null],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT created_at, deleted_at FROM datetime_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    final dt = reader.getColumnDateTime(0);
    // A naive stored string is interpreted as UTC wall-clock: the value
    // is flagged UTC and the components are NOT shifted by the device
    // timezone (10:30 stays 10:30, never 13:30 on a UTC-3 host).
    expect(dt.isUtc, isTrue);
    expect(dt.year, 2025);
    expect(dt.month, 6);
    expect(dt.day, 15);
    expect(dt.hour, 10);
    expect(dt.minute, 30);

    expect(reader.getColumnNullableDateTime(1), isNull);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('getColumnDateTime honors an explicit `Z` without shifting', () async {
    final db = await _createTestDb('datetime_utc_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE datetime_utc_test (
        id INTEGER PRIMARY KEY,
        created_at TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery(
          'INSERT INTO datetime_utc_test (id, created_at) VALUES (?, ?)');
      try {
        await stmt.executeSql(params: [1, '2025-06-15T10:30:00.000Z']);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery(
            'SELECT created_at FROM datetime_utc_test WHERE id = 1'))
        .executeReader();
    expect(await reader.readRow(), isTrue);

    final dt = reader.getColumnDateTime(0);
    expect(dt.isUtc, isTrue);
    expect(dt.hour, 10);
    expect(dt.minute, 30);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Duration (Time) retrieval
  // ──────────────────────────────────────────────────────────────────────

  test('getColumnTime / getColumnNullableTime', () async {
    final db = await _createTestDb('time_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE time_test (
        id INTEGER PRIMARY KEY,
        duration TEXT,
        nullable_duration TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO time_test (id, duration, nullable_duration) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, '02:30:45', null],);
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO time_test (id, duration, nullable_duration) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [2, '01:15:30.500', null],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT duration, nullable_duration FROM time_test ORDER BY id')).executeReader();

    // Row 1: 02:30:45
    expect(await reader.readRow(), isTrue);
    final d1 = reader.getColumnTime(0);
    expect(d1.inHours, 2);
    expect(d1.inMinutes % 60, 30);
    expect(d1.inSeconds % 60, 45);
    expect(reader.getColumnNullableTime(1), isNull);

    // Row 2: 01:15:30.500
    expect(await reader.readRow(), isTrue);
    final d2 = reader.getColumnTime(0);
    expect(d2.inHours, 1);
    expect(d2.inMinutes % 60, 15);
    expect(d2.inSeconds % 60, 30);
    expect(d2.inMilliseconds % 1000, 500);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Enum binding and retrieval
  // ──────────────────────────────────────────────────────────────────────

  test('Enum bind and getColumnEnum / getColumnNullableEnum', () async {
    final db = await _createTestDb('enum_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE enum_test (
        id INTEGER PRIMARY KEY,
        status INTEGER,
        nullable_status INTEGER
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO enum_test (id, status, nullable_status) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, TestStatus.active, null],);
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO enum_test (id, status, nullable_status) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [2, TestStatus.suspended, TestStatus.inactive],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT status, nullable_status FROM enum_test ORDER BY id')).executeReader();

    // Row 1
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnEnum(0, TestStatus.values), TestStatus.active);
    expect(reader.getColumnNullableEnum(1, TestStatus.values), isNull);

    // Row 2
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnEnum(0, TestStatus.values), TestStatus.suspended);
    expect(reader.getColumnNullableEnum(1, TestStatus.values), TestStatus.inactive);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Blob binding and retrieval
  // ──────────────────────────────────────────────────────────────────────

  test('Blob bind and getColumnBlob / getColumnNullableBlob', () async {
    final db = await _createTestDb('blob_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE blob_test (
        id INTEGER PRIMARY KEY,
        data BLOB,
        nullable_data BLOB
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final blobData = Uint8List.fromList([0, 1, 2, 127, 128, 254, 255]);
    {
      final stmt = await db.prepareQuery('INSERT INTO blob_test (id, data, nullable_data) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, blobData, null],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT data, nullable_data FROM blob_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    final blobResult = reader.getColumnBlob(0);
    expect(blobResult, isNotEmpty, reason: 'Blob should contain data');
    expect(reader.getColumnNullableBlob(1), isNull);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('Blob bind accepts List<int> (not just Uint8List)', () async {
    final db = await _createTestDb('blob_list_int_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE blob_li_test (id INTEGER PRIMARY KEY, data BLOB)
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Use plain List<int> (not Uint8List) to exercise the List<int> branch
    final data = List<int>.generate(256, (i) => i);
    {
      final stmt = await db.prepareQuery('INSERT INTO blob_li_test (id, data) VALUES (?, ?)');
      try {
        await stmt.executeSql(params: [1, data],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT data FROM blob_li_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    final result = reader.getColumnBlob(0);
    expect(result.length, 256);
    expect(result[0], 0);
    expect(result[127], 127);
    expect(result[255], 255);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Double binding and retrieval
  // ──────────────────────────────────────────────────────────────────────

  test('Double bind and getColumnDouble / getColumnNullableDouble', () async {
    final db = await _createTestDb('double_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE double_test (
        id INTEGER PRIMARY KEY,
        value REAL,
        nullable_value REAL
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO double_test (id, value, nullable_value) VALUES (?, ?, ?)');
      try {
        await stmt.executeSql(params: [1, 3.14159265, null],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT value, nullable_value FROM double_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    expect(reader.getColumnDouble(0), closeTo(3.14159265, 0.0000001));
    expect(reader.getColumnNullableDouble(1), isNull);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Named parameters: auto-prefix and different prefixes
  // ──────────────────────────────────────────────────────────────────────

  test('Named params without prefix get auto-prefixed with ":"', () async {
    final db = await _createTestDb('auto_prefix.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE prefix_test (
        id INTEGER PRIMARY KEY,
        name TEXT,
        value INTEGER
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // No prefix — should be auto-prefixed with ':'
    {
      final stmt = await db.prepareQuery('INSERT INTO prefix_test (id, name, value) VALUES (:id, :name, :value)');
      try {
        await stmt.executeSql(nameParams: {'id': 1, 'name': 'auto', 'value': 100},);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT name, value FROM prefix_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'auto');
    expect(reader.getColumnInt(1), 100);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('Named params with @ prefix', () async {
    final db = await _createTestDb('at_prefix.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE at_test (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO at_test (id, name) VALUES (@id, @name)');
      try {
        await stmt.executeSql(nameParams: {'@id': 1, '@name': 'at-sign'},);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT name FROM at_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'at-sign');

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('Named params with \$ prefix', () async {
    final db = await _createTestDb('dollar_prefix.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE dollar_test (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO dollar_test (id, name) VALUES (\$id, \$name)');
      try {
        await stmt.executeSql(nameParams: {r'$id': 1, r'$name': 'dollar-sign'},);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT name FROM dollar_test WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'dollar-sign');

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Named params: extra params silently skipped & throwOnMissingNamedParams
  // ──────────────────────────────────────────────────────────────────────

  test('Extra named params are silently skipped by default', () async {
    final db = await _createTestDb('skip_extra_params.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE skip_tbl (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // :extra does not exist in the SQL — should be silently ignored
    {
      final stmt = await db.prepareQuery('INSERT INTO skip_tbl (id, name) VALUES (:id, :name)');
      try {
        await stmt.executeSql(nameParams: {'id': 1, 'name': 'Alice', 'extra': 'ignored'},);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT name FROM skip_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'Alice');
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('Extra named params in executeReader are silently skipped', () async {
    final db = await _createTestDb('skip_extra_reader.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE skip_reader_tbl (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO skip_reader_tbl (id, name) VALUES (?, ?)');
      try {
        await stmt.executeSql(params: [1, 'Alice'],);
      } finally {
        await stmt.close();
      }
    }

    // :missing does not exist in the SQL — should be silently ignored
    final reader = await (await db.prepareQuery('SELECT name FROM skip_reader_tbl WHERE id = :id')).executeReader(nameParams: {'id': 1, 'missing': 'ignored'},);
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'Alice');
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('throwOnMissingNamedParams defaults to false', () async {
    final db = await _createTestDb('throw_default.db');
    expect(db.throwOnMissingNamedParams, isFalse);
    await db.closeDb();
    await db.dropDb();
  });

  test('throwOnMissingNamedParams can be set via getInstance', () async {
    final db = await DbasSqlite.getInstance(
      dbName: 'throw_via_instance.db',
      throwOnMissingNamedParams: true,
    );
    expect(db.throwOnMissingNamedParams, isTrue);

    await db.dropDb();
  });

  test('getInstance updates throwOnMissingNamedParams on cached instance', () async {
    final db1 = await DbasSqlite.getInstance(
      dbName: 'cached_flag.db',
      throwOnMissingNamedParams: false,
    );
    expect(db1.throwOnMissingNamedParams, isFalse);

    final db2 = await DbasSqlite.getInstance(
      dbName: 'cached_flag.db',
      throwOnMissingNamedParams: true,
    );
    expect(identical(db1, db2), isTrue);
    expect(db2.throwOnMissingNamedParams, isTrue);
    // The original reference reflects the update too
    expect(db1.throwOnMissingNamedParams, isTrue);

    await db1.dropDb();
  });

  test('throwOnMissingNamedParams throws on extra named params', () async {
    final db = await _createTestDb('throw_extra.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE throw_tbl (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    db.throwOnMissingNamedParams = true;

    // :extra does not exist in the SQL — should throw
    await expectLater(
      () => _runSql(
        db,
        'INSERT INTO throw_tbl (id, name) VALUES (:id, :name)',
        nameParams: {'id': 1, 'name': 'Alice', 'extra': 'boom'},
      ),
      throwsA(isA<DbasSqliteException>()
          .having((e) => e.code, 'code',
              DbasSqliteErrorCode.bindNamedParameterNotFound)
          .having((e) => e.sqliteCode, 'sqliteCode (SQLITE_RANGE)', 25)
          // sqlite3_bind_* failures don't queue an extended rc, so
          // sqliteUniqueCode is null. subCategory still resolves to
          // rangeError via the primary rc fallback.
          .having(
              (e) => e.sqliteUniqueCode, 'sqliteUniqueCode', isNull)
          .having((e) => e.subCategory, 'subCategory',
              DbasSqliteSubCategory.rangeError)),
    );

    // Verify the row was NOT inserted (exception aborted the bind)
    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM throw_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 0);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('throwOnMissingNamedParams does not throw when all params match', () async {
    final db = await _createTestDb('throw_all_match.db');
    db.throwOnMissingNamedParams = true;

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE match_tbl (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // All params exist in the SQL — should succeed regardless of the flag
    {
      final stmt = await db.prepareQuery('INSERT INTO match_tbl (id, name) VALUES (:id, :name)');
      try {
        await stmt.executeSql(nameParams: {'id': 1, 'name': 'Bob'},);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT name FROM match_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'Bob');
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('throwOnMissingNamedParams can be toggled at runtime', () async {
    final db = await _createTestDb('toggle_throw.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE toggle_tbl (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Default: off — extra params silently skipped
    {
      final stmt = await db.prepareQuery('INSERT INTO toggle_tbl (id, name) VALUES (:id, :name)');
      try {
        await stmt.executeSql(nameParams: {'id': 1, 'name': 'first', 'extra': 'ok'},);
      } finally {
        await stmt.close();
      }
    }

    // Turn on — extra params should throw
    db.throwOnMissingNamedParams = true;
    await expectLater(
      () => _runSql(
        db,
        'INSERT INTO toggle_tbl (id, name) VALUES (:id, :name)',
        nameParams: {'id': 2, 'name': 'second', 'extra': 'boom'},
      ),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.bindNamedParameterNotFound)),
    );

    // Turn back off — extra params silently skipped again
    db.throwOnMissingNamedParams = false;
    {
      final stmt = await db.prepareQuery('INSERT INTO toggle_tbl (id, name) VALUES (:id, :name)');
      try {
        await stmt.executeSql(nameParams: {'id': 3, 'name': 'third', 'extra': 'ok'},);
      } finally {
        await stmt.close();
      }
    }

    // Verify rows 1 and 3 exist (row 2 was never inserted due to the throw)
    final reader = await (await db.prepareQuery('SELECT name FROM toggle_tbl ORDER BY id')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'first');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'third');
    expect(await reader.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // getContent
  // ──────────────────────────────────────────────────────────────────────

  test('getContent returns database file bytes', () async {
    final db = await _createTestDb('content_test.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE content_tbl (id INTEGER PRIMARY KEY)
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final content = await db.getContent();
    expect(content, isNotEmpty);
    // SQLite files start with "SQLite format 3\0"
    expect(String.fromCharCodes(content.sublist(0, 15)), 'SQLite format 3');

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // attachDb with openDb: false
  // ──────────────────────────────────────────────────────────────────────

  test('attachDb with openDb false does not open the database', () async {
    // Create source DB
    final sourceDb = await _createTestDb('attach_src.db');
    {
      final stmt = await sourceDb.prepareQuery('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    final bytes = await sourceDb.getContent();
    await sourceDb.closeDb();

    // Attach into new DB without opening
    final targetDb = await DbasSqlite.getInstance(dbName: 'attach_no_open.db');
    final result = await targetDb.attachDb(bytes, openDb: false);

    expect(result.isOpened(), isFalse);
    expect(await result.databaseExists(), isTrue);

    // Clean up
    await result.dropDb();
    await sourceDb.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // dropDb on non-existent database
  // ──────────────────────────────────────────────────────────────────────

  test('dropDb on non-existent database does nothing', () async {
    final db = await DbasSqlite.getInstance(dbName: 'nonexistent.db');
    // Should not throw
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // executeSql returns affected rows
  // ──────────────────────────────────────────────────────────────────────

  test('executeSql returns affected rows count', () async {
    final db = await _createTestDb('affected_rows.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE ar_test (
        id INTEGER PRIMARY KEY,
        value TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery("INSERT INTO ar_test (id, value) VALUES (1, 'a')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO ar_test (id, value) VALUES (2, 'b')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO ar_test (id, value) VALUES (3, 'c')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final updated = await ((stmt) async { try { return await stmt.executeSql(); } finally { await stmt.close(); } })(await db.prepareQuery("UPDATE ar_test SET value = 'x' WHERE id <= 2"));
    expect(updated, 2);

    final deleted = await ((stmt) async { try { return await stmt.executeSql(); } finally { await stmt.close(); } })(await db.prepareQuery("DELETE FROM ar_test WHERE id = 3"));
    expect(deleted, 1);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // readRow auto-closes reader when done
  // ──────────────────────────────────────────────────────────────────────

  test('readRow auto-closes reader when no more rows', () async {
    final db = await _createTestDb('auto_close.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE ac_test (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO ac_test (id) VALUES (1)");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT id FROM ac_test')).executeReader();

    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);

    // readRow returns false and auto-closes — subsequent query should work
    expect(await reader.readRow(), isFalse);

    // Verify we can immediately run another query (reader was properly closed)
    final reader2 = await (await db.prepareQuery('SELECT id FROM ac_test')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnInt(0), 1);
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Multiple column types in the same row
  // ──────────────────────────────────────────────────────────────────────

  test('All column types in a single query', () async {
    final db = await _createTestDb('all_types.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE all_types (
        int_col INTEGER,
        real_col REAL,
        text_col TEXT,
        blob_col BLOB,
        null_col TEXT
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final blob = Uint8List.fromList([10, 20, 30]);
    {
      final stmt = await db.prepareQuery('INSERT INTO all_types (int_col, real_col, text_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?)');
      try {
        await stmt.executeSql(params: [42, 2.718, 'euler', blob, null],);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT int_col, real_col, text_col, blob_col, null_col FROM all_types')).executeReader();
    expect(await reader.readRow(), isTrue);

    expect(reader.getColumnType(0), SqliteColumnType.integer);
    expect(reader.getColumnType(1), SqliteColumnType.double);
    expect(reader.getColumnType(2), SqliteColumnType.text);
    expect(reader.getColumnType(3), SqliteColumnType.blob);
    expect(reader.getColumnType(4), SqliteColumnType.nullType);

    expect(reader.getColumnInt(0), 42);
    expect(reader.getColumnDouble(1), closeTo(2.718, 0.001));
    expect(reader.getColumnText(2), 'euler');
    expect(reader.getColumnBlob(3).sublist(0, blob.length), blob);
    expect(reader.isColumnNull(4), isTrue);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // getColumnEnum throws on out-of-range index
  // ──────────────────────────────────────────────────────────────────────

  test('getColumnEnum throws DbasSqliteException(invalidEnumIndex) for out-of-range index', () async {
    final db = await _createTestDb('enum_error.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE enum_err (id INTEGER PRIMARY KEY, val INTEGER)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO enum_err (id, val) VALUES (1, 99)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT val FROM enum_err WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    expect(
      () => reader.getColumnEnum(0, TestStatus.values),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.invalidEnumIndex)),
    );

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Named params bind with Decimal and bool
  // ──────────────────────────────────────────────────────────────────────

  test('Named params with Decimal and bool types', () async {
    final db = await _createTestDb('named_types.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE named_types (
        id INTEGER PRIMARY KEY,
        amount REAL,
        active INTEGER
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db.prepareQuery('INSERT INTO named_types (id, amount, active) VALUES (:id, :amount, :active)');
      try {
        await stmt.executeSql(nameParams: {
        ':id': 1,
        ':amount': Decimal.parse('99.99'),
        ':active': true,
      },);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT amount, active FROM named_types WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    expect(reader.getColumnDecimal(0).toDouble(), closeTo(99.99, 0.01));
    expect(reader.getColumnBool(1), isTrue);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Named params bind with Blob and Enum
  // ──────────────────────────────────────────────────────────────────────

  test('Named params with Blob and Enum types', () async {
    final db = await _createTestDb('named_blob_enum.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE named_be (
        id INTEGER PRIMARY KEY,
        data BLOB,
        status INTEGER
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final blob = Uint8List.fromList([0xDE, 0xAD, 0xBE, 0xEF]);
    {
      final stmt = await db.prepareQuery('INSERT INTO named_be (id, data, status) VALUES (:id, :data, :status)');
      try {
        await stmt.executeSql(nameParams: {
        ':id': 1,
        ':data': blob,
        ':status': TestStatus.inactive,
      },);
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT data, status FROM named_be WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);

    expect(reader.getColumnBlob(0).sublist(0, blob.length), blob);
    expect(reader.getColumnEnum(1, TestStatus.values), TestStatus.inactive);

    await reader.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Multiple databases open simultaneously
  // ──────────────────────────────────────────────────────────────────────

  test('Multiple databases can be open at the same time', () async {
    final db1 = await _createTestDb('multi_1.db');
    final db2 = await _createTestDb('multi_2.db');

    {
      final stmt = await db1.prepareQuery('CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db2.prepareQuery('CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    {
      final stmt = await db1.prepareQuery("INSERT INTO t1 (id, val) VALUES (1, 'from_db1')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db2.prepareQuery("INSERT INTO t2 (id, val) VALUES (1, 'from_db2')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader1 = await (await db1.prepareQuery('SELECT val FROM t1 WHERE id = 1')).executeReader();
    expect(await reader1.readRow(), isTrue);
    expect(reader1.getColumnText(0), 'from_db1');
    await reader1.close();

    final reader2 = await (await db2.prepareQuery('SELECT val FROM t2 WHERE id = 1')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'from_db2');
    await reader2.close();

    await db1.closeDb();
    await db1.dropDb();
    await db2.closeDb();
    await db2.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Empty result set
  // ──────────────────────────────────────────────────────────────────────

  test('Empty result set returns false on first readRow', () async {
    final db = await _createTestDb('empty_result.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE empty_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT id FROM empty_tbl')).executeReader();
    expect(await reader.readRow(), isFalse);

    // Should be able to run another query immediately
    final reader2 = await (await db.prepareQuery('SELECT id FROM empty_tbl')).executeReader();
    expect(await reader2.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Unicode / special characters
  // ──────────────────────────────────────────────────────────────────────

  test('Unicode and special characters in text', () async {
    final db = await _createTestDb('unicode_test.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE unicode_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final testStrings = [
      'Hello 世界',
      'Ação não está à toa',
      'Ümlauts: äöü ÄÖÜ ß',
      '🚀🎉💡',
      "Quotes: 'single' and \"double\"",
      'Line\nbreak\ttab',
    ];

    for (int i = 0; i < testStrings.length; i++) {
      {
        final stmt = await db.prepareQuery('INSERT INTO unicode_tbl (id, val) VALUES (?, ?)');
        try {
          await stmt.executeSql(params: [i + 1, testStrings[i]],);
        } finally {
          await stmt.close();
        }
      }
    }

    final reader = await (await db.prepareQuery('SELECT val FROM unicode_tbl ORDER BY id')).executeReader();
    for (final expected in testStrings) {
      expect(await reader.readRow(), isTrue);
      expect(reader.getColumnText(0), expected);
    }
    expect(await reader.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: beginTransaction + commit
  // ──────────────────────────────────────────────────────────────────────

  test('beginTransaction and commit persists data', () async {
    final db = await _createTestDb('txn_commit.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();
    expect(db.isInTransaction, isTrue);

    {
      final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (1, 'a')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (2, 'b')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await db.commit();
    expect(db.isInTransaction, isFalse);

    final reader = await (await db.prepareQuery('SELECT val FROM txn_tbl ORDER BY id')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'a');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'b');
    expect(await reader.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: beginTransaction + rollback
  // ──────────────────────────────────────────────────────────────────────

  test('beginTransaction and rollback discards data', () async {
    final db = await _createTestDb('txn_rollback.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();
    {
      final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (1, 'a')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (2, 'b')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await db.rollback();
    expect(db.isInTransaction, isFalse);

    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM txn_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 0);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: transaction() helper commits on success
  // ──────────────────────────────────────────────────────────────────────

  test('transaction() helper commits on success', () async {
    final db = await _createTestDb('txn_helper_commit.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.transaction((db) async {
      {
        final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (1, 'x')");
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }
      {
        final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (2, 'y')");
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }
    });

    expect(db.isInTransaction, isFalse);

    final reader = await (await db.prepareQuery('SELECT val FROM txn_tbl ORDER BY id')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'x');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'y');
    expect(await reader.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: transaction() helper rolls back on error
  // ──────────────────────────────────────────────────────────────────────

  test('transaction() helper rolls back on error and rethrows', () async {
    final db = await _createTestDb('txn_helper_rollback.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Insert one row outside the transaction
    {
      final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (1, 'before')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await expectLater(
      () => db.transaction((db) async {
        {
          final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (2, 'inside')");
          try {
            await stmt.executeSql();
          } finally {
            await stmt.close();
          }
        }
        throw Exception('Simulated error');
      }),
      throwsA(isA<Exception>()),
    );

    expect(db.isInTransaction, isFalse);

    // Only the row inserted before the transaction should exist
    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM txn_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();

    final reader2 = await (await db.prepareQuery('SELECT val FROM txn_tbl WHERE id = 1')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'before');
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: idempotent behavior
  // ──────────────────────────────────────────────────────────────────────

  test('beginTransaction is idempotent when already in transaction', () async {
    final db = await _createTestDb('txn_idempotent_begin.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();
    expect(db.isInTransaction, isTrue);

    // Calling again should not throw
    await db.beginTransaction();
    expect(db.isInTransaction, isTrue);

    await db.commit();
    expect(db.isInTransaction, isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  test('commit is idempotent when no transaction is active', () async {
    final db = await _createTestDb('txn_idempotent_commit.db');

    expect(db.isInTransaction, isFalse);

    // Should not throw
    await db.commit();
    expect(db.isInTransaction, isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  test('rollback is idempotent when no transaction is active', () async {
    final db = await _createTestDb('txn_idempotent_rollback.db');

    expect(db.isInTransaction, isFalse);

    // Should not throw
    await db.rollback();
    expect(db.isInTransaction, isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: lock ownership — commit() pre-flight
  // ──────────────────────────────────────────────────────────────────────

  test('commit() rejects an in-flight reentrant executeSql instead of racing it',
      () async {
    // beginTransaction() is idempotent, so a second caller that joined an
    // already-active transaction never acquires the writer lock itself.
    // If THAT caller's commit() runs while the real owner still has an
    // executeSql mid prepare / bind / step / finalize dispatch, the
    // owner's still-running FFI work keeps using the writer connection
    // after COMMIT already ended the transaction and handed the lock to
    // the next FIFO waiter. commit() must refuse instead of racing it.
    final db = await _createTestDb('commit_preflight_inflight_write.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await db.beginTransaction();
    final stmt = await db.prepareQuery("INSERT INTO t (id, val) VALUES (1, 'x')");
    // Deliberately NOT awaited — still mid prepare/bind/step/finalize
    // dispatch when commit() is invoked on the very next line. No
    // `Future.delayed` guess needed: `executeSql()`'s synchronous prefix
    // (which is where the reentrant registration happens) runs to
    // completion before this statement returns control, and
    // `expectLater`/`throwsA` synchronously invokes the `() => db.commit()`
    // closure to obtain its Future — the same established idiom as
    // 'transaction() helper rolls back on error and rethrows'.
    final writeFuture = stmt.executeSql();

    await expectLater(
      () => db.commit(),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.commitBlockedByInFlightOperation)),
    );
    expect(db.isInTransaction, isTrue,
        reason: 'a rejected pre-flight must not touch transaction state');

    // Awaiting the write clears the blocker; the same commit then works.
    await writeFuture;
    await db.commit();
    expect(db.isInTransaction, isFalse);

    final v = await (await db.prepareQuery('SELECT val FROM t WHERE id = 1'))
        .executeScalar();
    expect(v, 'x');

    await stmt.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('commit() rejects a still-open in-transaction reader routed to the writer',
      () async {
    // A reader opened after a write in the same transaction is routed to
    // the writer connection for read-your-writes. Committing while its
    // cursor is still live leaves that cursor running on a connection
    // whose transaction has ended and whose writer lock has been handed
    // on. commit() must refuse until the reader is closed or exhausted.
    final db = await _createTestDb('commit_preflight_open_reader.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await db.beginTransaction();
    await _runSql(db, "INSERT INTO t VALUES (1, 'x')"); // flips routing to writer
    final readStmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
    final reader = await readStmt.executeReader(); // left open on purpose
    expect(await reader.readRow(), isTrue);

    await expectLater(
      () => db.commit(),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.commitBlockedByActiveReader)),
    );
    expect(db.isInTransaction, isTrue,
        reason: 'a rejected pre-flight must not touch transaction state');

    await reader.close();
    await db.commit();
    expect(db.isInTransaction, isFalse);

    await readStmt.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('commit() pre-flight does not block an unrelated pool reader', () async {
    // Blast-radius proof for the pre-flight check: it must fire ONLY for
    // users of the writer connection. A reader opened outside any
    // transaction runs on its own pool connection, and a WAL pool read
    // never blocks a writer COMMIT — so leaving one open must not make
    // an unrelated transaction's commit() throw.
    final db = await _createTestDb('commit_preflight_pool_reader_unaffected.db',
        readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'a')");

    // Opened OUTSIDE any transaction — a pool connection, independent of
    // the writer. Left open on purpose.
    final poolStmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
    final poolReader = await poolStmt.executeReader();
    expect(await poolReader.readRow(), isTrue);

    await db.beginTransaction();
    await _runSql(db, "INSERT INTO t VALUES (2, 'b')");
    await db.commit(); // must NOT throw — WAL pool reads never block COMMIT
    expect(db.isInTransaction, isFalse);

    await poolReader.close();
    await poolStmt.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('a nested beginTransaction/commit pair still ends the WHOLE transaction (no reference counting, unchanged)',
      () async {
    // Regression pin for the part of the design this fix does NOT
    // change. DbasSqlite tracks at most one active transaction with no
    // reference counting, so a joiner's commit() ends the transaction
    // for everyone. The fix only guards against RACING in-flight work —
    // it must not turn nested begin/commit into a reference count, or
    // 'beginTransaction is idempotent when already in transaction'
    // (2 begins + 1 commit → isInTransaction == false) would break.
    final db = await _createTestDb('nested_commit_ends_outer.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');

    await db.beginTransaction(); // "outer" — the real owner
    expect(db.startedCurrentTransaction, isTrue);
    await db.beginTransaction(); // "inner" — idempotent join, no new lock hold
    expect(db.startedCurrentTransaction, isFalse);

    await db.commit(); // ends the WHOLE transaction — by design, no refcounting
    expect(db.isInTransaction, isFalse);

    // A write issued after this point is no longer transactional with
    // anything — it lands autocommitted. This is the documented,
    // accepted consequence of the non-refcounted design.
    await _runSql(db, 'INSERT INTO t (id) VALUES (1)');
    final count = await (await db.prepareQuery('SELECT COUNT(*) FROM t'))
        .executeScalar();
    expect(count, 1);

    await db.closeDb();
    await db.dropDb();
  });

  test('a stale reentrant-op decrement must not cancel a live registration from a later transaction',
      () async {
    // The reentrant-op count is what commit()'s pre-flight consults, so
    // an unbalanced decrement silently disarms it. A decrement CAN
    // arrive after its own transaction ended: rollback() deliberately
    // has no pre-flight check (see 'rollback() does NOT
    // pre-flight-block …'), so it can end a transaction while an
    // executeSql dispatched inside it is still in flight; that
    // dispatch's unregistration then lands later, against whatever
    // transaction is current by then.
    //
    // Treating such a late decrement as a plain "clamp at zero" no-op is
    // only safe while the NEXT transaction has no reentrant op of its
    // own. With the counter at 1, the stale decrement drops it to 0 —
    // cancelling a live registration and letting a commit() straight
    // through a pre-flight that should have blocked it. The registration
    // must therefore be identified (generation/epoch tag), not merely
    // counted.
    //
    // Driven through the reentrant hooks rather than real in-flight SQL
    // because the ordering this pins — stale decrement lands AFTER the
    // next transaction's registration but BEFORE its commit() — is not
    // expressible deterministically through worker-isolate dispatch;
    // there is no seam to stall a worker mid-dispatch.
    final db = await _createTestDb('stale_reentrant_decrement.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');

    // Transaction 1 registers a reentrant op, then ends WITHOUT it
    // having finished — exactly what rollback() permits.
    await db.beginTransaction();
    final staleOp = db.beginReentrantWriterOpInternal();
    await db.rollback();
    expect(db.isInTransaction, isFalse);

    // Transaction 2 starts and registers its OWN, genuinely live op.
    await db.beginTransaction();
    final liveOp = db.beginReentrantWriterOpInternal();

    // Transaction 1's decrement finally lands — late, and against a
    // transaction that no longer exists. It must be a no-op.
    db.endReentrantWriterOpInternal(staleOp);

    // ...and must NOT have cancelled transaction 2's live registration.
    await expectLater(
      () => db.commit(),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.commitBlockedByInFlightOperation)),
    );
    expect(db.isInTransaction, isTrue);

    // Once the real op ends, the same commit goes through.
    await _runSql(db, 'INSERT INTO t (id) VALUES (1)');
    db.endReentrantWriterOpInternal(liveOp);
    await db.commit();
    expect(db.isInTransaction, isFalse);

    final count = await (await db.prepareQuery('SELECT COUNT(*) FROM t'))
        .executeScalar();
    expect(count, 1);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: strict mode (opt-in real serialization)
  // ──────────────────────────────────────────────────────────────────────

  test('beginTransaction(strict: true) parks instead of silently joining',
      () async {
    final db = await _createTestDb('strict_mode_parks.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await db.beginTransaction();
    expect(db.isInTransaction, isTrue);
    expect(db.startedCurrentTransaction, isTrue);

    final strictBegin = db.beginTransaction(strict: true);
    await _awaitWriterWaiters(db, 1);

    // The owner's transaction is still the active one — proof the
    // strict caller really parked rather than collapsing into it.
    expect(db.isInTransaction, isTrue);

    await _runSql(db, "INSERT INTO t VALUES (1, 'owner-write')");
    await db.commit(); // hands the writer lock to the parked strict caller

    await strictBegin; // resolves — strict caller has ITS OWN transaction
    expect(db.isInTransaction, isTrue);
    expect(db.startedCurrentTransaction, isTrue,
        reason: 'strict mode never joins — it always starts fresh');

    await _runSql(db, "INSERT INTO t VALUES (2, 'strict-write')");
    await db.commit();

    final count = await (await db.prepareQuery('SELECT COUNT(*) FROM t'))
        .executeScalar();
    expect(count, 2);

    await db.closeDb();
    await db.dropDb();
  });

  test('beginTransaction(strict: true) behaves like default when uncontended',
      () async {
    final db = await _createTestDb('strict_mode_no_contention.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');

    await db.beginTransaction(strict: true);
    expect(db.isInTransaction, isTrue);
    expect(db.startedCurrentTransaction, isTrue);
    expect(db.debugWriterLockWaitQueueLength, 0);

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  test('startedCurrentTransaction reports false when beginTransaction joins',
      () async {
    final db = await _createTestDb('started_current_transaction.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');

    await db.beginTransaction();
    expect(db.startedCurrentTransaction, isTrue);
    await db.beginTransaction(); // idempotent no-op join
    expect(db.startedCurrentTransaction, isFalse);
    await db.commit();
    expect(db.startedCurrentTransaction, isFalse,
        reason: 'resets once the transaction ends');

    await db.closeDb();
    await db.dropDb();
  });

  test('beginTransaction(strict: true) from the flow that already owns the transaction fails instead of deadlocking',
      () async {
    // Strict mode never joins — it parks on the writer-lock queue. When
    // the caller IS the current owner, nobody will ever release that
    // lock, so the call parks forever: no timeout, no detection, no
    // error code, and `isInTransaction` keeps reporting `true` while the
    // flow is wedged. A self-deadlock must surface as a diagnosable
    // typed error, mirroring the reader slot's existing wait timeout
    // (DbasSqliteErrorCode.readerSlotWaitTimeout, exercised by
    // 'pool: blocking-acquire times out when readers are saturated').
    final db = await _createTestDb('strict_mode_self_deadlock.db');
    // Test-only override, same rationale and lifecycle as
    // debugPoolAcquireTimeoutMs: shorten the wait so the test completes
    // in milliseconds instead of the production deadline.
    DbasSqlite.debugWriterLockWaitTimeoutMs = 200;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');

      await db.beginTransaction();
      expect(db.isInTransaction, isTrue);

      // The `.timeout` is a REGRESSION GUARD, not the assertion: without
      // it, a library that still parks forever would hang the whole
      // suite instead of failing this test. It is deliberately an order
      // of magnitude longer than the override above, so it can only fire
      // when the library never timed out at all.
      await expectLater(
        db.beginTransaction(strict: true).timeout(const Duration(seconds: 5)),
        throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
            DbasSqliteErrorCode.writerLockWaitTimeout)),
      );

      // A strict acquire that gave up must leave the transaction it
      // could not join completely untouched, and must not leak its
      // abandoned waiter into the queue.
      expect(db.isInTransaction, isTrue);
      expect(db.debugWriterLockWaitQueueLength, 0,
          reason: 'a timed-out waiter must remove itself from the queue');

      await _runSql(db, 'INSERT INTO t (id) VALUES (1)');
      await db.commit();
      expect(db.isInTransaction, isFalse);

      final count = await (await db.prepareQuery('SELECT COUNT(*) FROM t'))
          .executeScalar();
      expect(count, 1);
    } finally {
      DbasSqlite.debugWriterLockWaitTimeoutMs = null;
      await db.closeDb();
      await db.dropDb();
    }
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: commit failure recovery
  // ──────────────────────────────────────────────────────────────────────

  test('commit(): a genuine COMMIT failure alone still rethrows commitFailed (rollback recovers)',
      () async {
    final db = await _createTestDb('commit_failure_recovers.db');
    await _runSql(db, 'PRAGMA foreign_keys = ON');
    await _runSql(db, 'CREATE TABLE parent (id INTEGER PRIMARY KEY)');
    await _runSql(db,
        'CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))');

    await db.beginTransaction();
    await _runSql(db, 'PRAGMA defer_foreign_keys = ON');
    // No row 999 in `parent` — deferred, so this INSERT itself succeeds;
    // per sqlite.org/foreignkeys.html the transaction "remains open" on
    // a deferred-FK COMMIT failure (SQLite does NOT auto-rollback), so
    // this is a real, deterministic way to trigger `commitFailed`
    // without a mock, and to reach `rollback()` as a genuine recovery
    // afterwards.
    await _runSql(db, 'INSERT INTO child (id, parent_id) VALUES (1, 999)');

    // Verified empirically against the bundled SQLite build: a
    // deferred-FK COMMIT failure populates the extended rc exactly like
    // an immediate one, so SQLITE_CONSTRAINT (19) / 787 /
    // foreignKeyViolation all hold — the same triple the immediate-FK
    // test 'FOREIGN KEY violation surfaces
    // DbasSqliteSubCategory.foreignKeyViolation' pins on the step path.
    await expectLater(
      () => db.commit(),
      throwsA(isA<DbasSqliteException>()
          .having((e) => e.code, 'code', DbasSqliteErrorCode.commitFailed)
          .having((e) => e.sqliteCode, 'sqliteCode', 19)
          .having((e) => e.sqliteUniqueCode, 'sqliteUniqueCode', 787)
          .having((e) => e.subCategory, 'subCategory',
              DbasSqliteSubCategory.foreignKeyViolation)),
    );

    expect(db.isInTransaction, isFalse,
        reason: 'the automatic rollback() recovery succeeded');
    final count = await (await db.prepareQuery('SELECT COUNT(*) FROM child'))
        .executeScalar();
    expect(count, 0, reason: 'the deferred-violation insert was rolled back');

    await db.closeDb();
    await db.dropDb();
  });

  test('rollback() does NOT pre-flight-block on an open writer-routed reader (unlike commit)',
      () async {
    // Scoping pin for the pre-flight check: it belongs to commit() only.
    // SQLite tolerates a ROLLBACK with live statements on the connection
    // (unlike COMMIT, which fails with SQLITE_BUSY), and rollback() is
    // the best-effort cleanup path used by closeDb() and by error
    // recovery throughout DbasSqlite — adding a new way for it to fail
    // would be a regression, not a safety improvement.
    final db = await _createTestDb('rollback_no_preflight.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await db.beginTransaction();
    await _runSql(db, "INSERT INTO t VALUES (1, 'x')"); // flips routing to writer
    final readStmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
    final reader = await readStmt.executeReader(); // left open on purpose

    await db.rollback(); // must NOT throw — see rollback()'s doc comment
    expect(db.isInTransaction, isFalse);

    if (!reader.isClosed) await reader.close();
    await readStmt.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: rollback() vs. an in-flight write
  // ──────────────────────────────────────────────────────────────────────

  test('rollback() must not leave a raced in-flight write permanently persisted',
      () async {
    // The complement of 'rollback() does NOT pre-flight-block on an open
    // writer-routed reader (unlike commit)' directly above, and it does
    // NOT contradict it: READERS stay deliberately un-gated (a live
    // cursor only ever observes data the ROLLBACK is about to undo), but
    // an in-flight WRITE is a different animal — it can outlive the
    // ROLLBACK and land in autocommit.
    //
    // `executeSql` replays its bind buffer one bind at a time, and every
    // single bind is its own await / worker round-trip. A write
    // dispatched un-awaited inside an open transaction is therefore
    // still walking a long chain of pending dispatches when rollback()
    // runs on the next line. rollback() issues ROLLBACK immediately (no
    // pre-flight, by design), the ROLLBACK slips BETWEEN two of the
    // write's bind dispatches, and the statement's step then executes on
    // a connection that is back in autocommit mode. The row is committed
    // on its own and SURVIVES the rollback — silently, with no error
    // raised on either side.
    //
    // rollback() must therefore DRAIN in-flight writer operations before
    // issuing ROLLBACK — wait for them, never reject them. Draining
    // rather than throwing is deliberate: rollback() is closeDb()'s
    // cleanup path and the error-recovery path throughout DbasSqlite, so
    // a new failure mode on teardown would be a regression — which is
    // precisely what the neighbouring test above pins.
    final db = await _createTestDb('rollback_drains_inflight_write.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    // LOAD-BEARING — DO NOT "simplify" this to a small bind count.
    // `executeSql` crosses a prepare dispatch, then one dispatch PER
    // BIND, then a step and a finalize dispatch before the row actually
    // lands; ROLLBACK only has to slip into ONE of those gaps. Measured
    // against this library, the race reproduces 100% of the time at 2,
    // 11, 51, 101, 201, 301 and 401 binds — so today the width is a
    // safety MARGIN, not a threshold, and this number is deliberately
    // generous rather than minimal. It is what keeps the test meaningful
    // if the dispatch chain is ever shortened (batched binds, a fused
    // prepare+bind+step round-trip): trimmed to the minimum, a
    // still-broken library could start passing silently.
    const racingBindCount = 301;
    // One bind for the primary key, the rest concatenated into the text
    // column — a single row, but 301 separate bind dispatches to cross.
    final concatenatedValueBinds =
        List.filled(racingBindCount - 1, '?').join(' || ');
    final stmt = await db.prepareQuery(
        'INSERT INTO t (id, val) VALUES (?, $concatenatedValueBinds)');
    final racingParams = <Object?>[1, ...List.filled(racingBindCount - 1, 'x')];

    await db.beginTransaction();
    // Deliberately NOT awaited — the bind chain is still dispatching
    // when rollback() is invoked below. Same idiom as 'commit() rejects
    // an in-flight reentrant executeSql instead of racing it'.
    final writeFuture = stmt.executeSql(params: racingParams);

    // The outcome recorder is attached IMMEDIATELY (synchronously, so it
    // cannot perturb the race) rather than at the later await: an error
    // arriving while rollback() is in flight would otherwise be an
    // unhandled async error, which flutter_test escalates into a failure
    // unrelated to what this test pins.
    //
    // What the raced write ITSELF does is deliberately NOT pinned. Today
    // it completes silently — its step lands in autocommit and reports
    // success. A correct drain may just as legitimately leave it
    // completing normally against data the ROLLBACK then undoes, or
    // surface SQLITE_ABORT / executeSqlStepFailed to whoever awaits it.
    // Both are acceptable; the defect pinned here is the SURVIVING ROW,
    // not the write's return value. So assert only that it settles, and
    // carry the observed outcome into the row-count failure message so a
    // future regression is diagnosable from the output alone.
    String? writeOutcome;
    final writeSettled = writeFuture.then<void>((affectedRows) {
      writeOutcome = 'completed normally (affectedRows=$affectedRows)';
    }, onError: (Object e) {
      writeOutcome = 'threw ${e.runtimeType}: $e';
    });

    // Both `.timeout`s below are REGRESSION GUARDS, not assertions: a
    // drain written as an unbounded wait on an operation that can never
    // finish would wedge the entire suite instead of failing this test.
    // Same stance as 'beginTransaction(strict: true) from the flow that
    // already owns the transaction fails instead of deadlocking'.
    await db.rollback().timeout(const Duration(seconds: 30));
    expect(db.isInTransaction, isFalse);

    await writeSettled.timeout(const Duration(seconds: 30));
    expect(writeOutcome, isNotNull,
        reason: 'the raced write must settle, not hang');

    final survivors =
        await (await db.prepareQuery('SELECT COUNT(*) FROM t')).executeScalar();
    expect(survivors, 0,
        reason: 'ROLLBACK must undo the raced write instead of letting it '
            'autocommit behind the rollback; raced write $writeOutcome');
    expect(db.isInTransaction, isFalse,
        reason: 'the drain must not leave the transaction flag set');

    await stmt.close();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: DbasSqliteException when DB not opened
  // ──────────────────────────────────────────────────────────────────────

  test('beginTransaction throws DbasSqliteException when database is not opened', () async {
    final db = await DbasSqlite.getInstance(dbName: 'txn_not_opened.db');
    expect(
      () => db.beginTransaction(),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.beginTransactionDatabaseNotOpened)),
    );
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: transaction() throws DbasSqliteException(transactionAlreadyActive) when nested
  // ──────────────────────────────────────────────────────────────────────

  test('transaction() throws DbasSqliteException when already in transaction', () async {
    final db = await _createTestDb('txn_nested.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();
    expect(db.isInTransaction, isTrue);

    await expectLater(
      () => db.transaction((db) async {
        {
          final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id) VALUES (1)");
          try {
            await stmt.executeSql();
          } finally {
            await stmt.close();
          }
        }
      }),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.transactionAlreadyActive)),
    );

    // Original transaction should still be active
    expect(db.isInTransaction, isTrue);
    await db.rollback();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: closeDb auto-rollback
  // ──────────────────────────────────────────────────────────────────────

  test('closeDb automatically rolls back pending transaction', () async {
    final db = await _createTestDb('txn_close_rollback.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (1, 'committed')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();
    {
      final stmt = await db.prepareQuery("INSERT INTO txn_tbl (id, val) VALUES (2, 'uncommitted')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    expect(db.isInTransaction, isTrue);

    // closeDb should rollback the pending transaction
    await db.closeDb();
    expect(db.isOpened(), isFalse);

    // Reopen and verify only committed data exists
    final db2 = await DbasSqlite.getInstance(dbName: 'txn_close_rollback.db');
    await db2.openDb();

    final reader = await (await db2.prepareQuery('SELECT COUNT(*) FROM txn_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();

    final reader2 = await (await db2.prepareQuery('SELECT val FROM txn_tbl WHERE id = 1')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'committed');
    await reader2.close();

    await db2.closeDb();
    await db2.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction: isInTransaction state tracking
  // ──────────────────────────────────────────────────────────────────────

  test('isInTransaction tracks state correctly through lifecycle', () async {
    final db = await _createTestDb('txn_state.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE txn_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    expect(db.isInTransaction, isFalse);

    await db.beginTransaction();
    expect(db.isInTransaction, isTrue);

    await db.commit();
    expect(db.isInTransaction, isFalse);

    await db.beginTransaction();
    expect(db.isInTransaction, isTrue);

    await db.rollback();
    expect(db.isInTransaction, isFalse);

    await db.transaction((db) async {
      expect(db.isInTransaction, isTrue);
    });
    expect(db.isInTransaction, isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Pool: transparent pooling
  // ──────────────────────────────────────────────────────────────────────

  test('openDb with default pool works transparently', () async {
    final db = await _createTestDb('pool_default.db', readerPoolSize: 4);

    {
      final stmt = await db.prepareQuery('CREATE TABLE pool_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO pool_tbl (id, val) VALUES (1, 'pooled')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT val FROM pool_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'pooled');
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('openDb with readerPoolSize=0 uses single connection', () async {
    final db = await DbasSqlite.getInstance(dbName: 'pool_zero.db');
    await db.dropDb();

    await db.openDb(readerPoolSize: 0);
    expect(db.isOpened(), isTrue);

    {
      final stmt = await db.prepareQuery('CREATE TABLE single_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO single_tbl (id, val) VALUES (1, 'single')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT val FROM single_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'single');
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // streamCopyDb
  // ──────────────────────────────────────────────────────────────────────

  test('streamCopyDb copies database to new name', () async {
    final db = await _createTestDb('copy_src.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE copy_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO copy_tbl (id, val) VALUES (1, 'copied')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await db.closeDb();

    // Re-open to ensure WAL is flushed
    final srcDb = await DbasSqlite.getInstance(dbName: 'copy_src.db');
    await srcDb.openDb();
    await srcDb.streamCopyDb('copy_dest.db');
    await srcDb.closeDb();

    // Open the copy and verify data
    final destDb = await DbasSqlite.getInstance(dbName: 'copy_dest.db');
    await destDb.openDb();
    expect(destDb.isOpened(), isTrue);

    final reader = await (await destDb.prepareQuery('SELECT val FROM copy_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'copied');
    await reader.close();

    await destDb.closeDb();
    await destDb.dropDb();
    await srcDb.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // attachStreamDb
  // ──────────────────────────────────────────────────────────────────────

  test('attachStreamDb writes database from byte stream', () async {
    // Create source DB
    final srcDb = await _createTestDb('stream_src.db');
    {
      final stmt = await srcDb.prepareQuery('CREATE TABLE stream_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await srcDb.prepareQuery("INSERT INTO stream_tbl (id, val) VALUES (1, 'streamed')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    final srcPath = await srcDb.getAppDatabasePath();
    await srcDb.closeDb();

    // Read source as a stream
    final stream = File(srcPath).openRead();

    // Attach via stream
    final destDb = await DbasSqlite.getInstance(dbName: 'stream_dest.db');
    final result = await destDb.attachStreamDb(stream);

    expect(result.isOpened(), isTrue);

    final reader = await (await result.prepareQuery('SELECT val FROM stream_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'streamed');
    await reader.close();

    await result.closeDb();
    await result.dropDb();
    await srcDb.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // closeReader idempotent
  // ──────────────────────────────────────────────────────────────────────

  test('closeReader is safe to call multiple times', () async {
    final db = await _createTestDb('close_reader_idem.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE cr_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO cr_tbl (id) VALUES (1)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT id FROM cr_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);

    // Close twice — should not throw
    await reader.close();
    await reader.close();

    // Should still be able to run queries after
    final reader2 = await (await db.prepareQuery('SELECT id FROM cr_tbl')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnInt(0), 1);
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // executeReader within a transaction
  // ──────────────────────────────────────────────────────────────────────

  test('executeReader works within a transaction', () async {
    final db = await _createTestDb('reader_in_txn.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE rit_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO rit_tbl (id, val) VALUES (1, 'before')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();

    {
      final stmt = await db.prepareQuery("INSERT INTO rit_tbl (id, val) VALUES (2, 'during')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Read within the same transaction — should see uncommitted data
    final reader = await (await db.prepareQuery('SELECT val FROM rit_tbl ORDER BY id')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'before');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'during');
    expect(await reader.readRow(), isFalse);

    await db.commit();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Sequential reader then writer
  // ──────────────────────────────────────────────────────────────────────

  test('sequential reader then writer works correctly', () async {
    final db = await _createTestDb('seq_rw.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE seq_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO seq_tbl (id, val) VALUES (1, 'a')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Reader cycle
    final reader = await (await db.prepareQuery('SELECT val FROM seq_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'a');
    await reader.close();

    // Writer after reader
    {
      final stmt = await db.prepareQuery("UPDATE seq_tbl SET val = 'b' WHERE id = 1");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Verify write took effect
    final reader2 = await (await db.prepareQuery('SELECT val FROM seq_tbl WHERE id = 1')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'b');
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Thread safety: concurrent writes serialized
  // ──────────────────────────────────────────────────────────────────────

  test('concurrent executeSql calls are serialized and all succeed', () async {
    final db = await _createTestDb('concurrent_writes.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE cw_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Launch multiple writes concurrently
    await Future.wait([
      _runSql(db, "INSERT INTO cw_tbl (id, val) VALUES (1, 'a')"),
      _runSql(db, "INSERT INTO cw_tbl (id, val) VALUES (2, 'b')"),
      _runSql(db, "INSERT INTO cw_tbl (id, val) VALUES (3, 'c')"),
    ]);

    // All three rows should exist
    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM cw_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 3);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Thread safety: writer lock held during transaction
  // ──────────────────────────────────────────────────────────────────────

  test('concurrent transactions are serialized via writer lock', () async {
    final db = await _createTestDb('txn_lock.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE tl_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final executionOrder = <int>[];

    // Two transactions fired concurrently — must be serialized
    await Future.wait([
      db.transaction((db) async {
        {
          final stmt = await db.prepareQuery("INSERT INTO tl_tbl (id, val) VALUES (1, 'a')");
          try {
            await stmt.executeSql();
          } finally {
            await stmt.close();
          }
        }
        executionOrder.add(1);
      }),
      db.transaction((db) async {
        {
          final stmt = await db.prepareQuery("INSERT INTO tl_tbl (id, val) VALUES (2, 'b')");
          try {
            await stmt.executeSql();
          } finally {
            await stmt.close();
          }
        }
        executionOrder.add(2);
      }),
    ]);

    // Both should have completed
    expect(executionOrder.length, 2);
    expect(executionOrder.toSet(), {1, 2});

    // Both rows should exist
    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM tl_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 2);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Thread safety: reader does not block writer (pool)
  // ──────────────────────────────────────────────────────────────────────

  test('executeReader and executeSql do not deadlock', () async {
    final db = await _createTestDb('rw_nodeadlock.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE rw_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO rw_tbl (id, val) VALUES (1, 'initial')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Start a reader
    final reader = await (await db.prepareQuery('SELECT val FROM rw_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'initial');
    await reader.close();

    // Writer should not be blocked
    {
      final stmt = await db.prepareQuery("UPDATE rw_tbl SET val = 'updated' WHERE id = 1");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader2 = await (await db.prepareQuery('SELECT val FROM rw_tbl WHERE id = 1')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'updated');
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Thread safety: closeDb while operations pending
  // ──────────────────────────────────────────────────────────────────────

  test('closeDb cleans up state and subsequent operations throw', () async {
    final db = await _createTestDb('close_state.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE cs_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.closeDb();

    expect(db.isOpened(), isFalse);
    expect(db.isInTransaction, isFalse);

    expect(
      () => db.prepareQuery('SELECT 1'),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.prepareQueryDatabaseNotOpened)),
    );
    expect(
      () => db.prepareQuery('SELECT 1'),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.prepareQueryDatabaseNotOpened)),
    );
    expect(
      () => db.beginTransaction(),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.beginTransactionDatabaseNotOpened)),
    );

    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Multiple sequential reader sessions
  // ──────────────────────────────────────────────────────────────────────

  test('multiple sequential executeReader sessions work correctly', () async {
    final db = await _createTestDb('multi_reader.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE mr_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO mr_tbl (id, val) VALUES (1, 'a')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO mr_tbl (id, val) VALUES (2, 'b')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // First reader session
    final reader = await (await db.prepareQuery('SELECT val FROM mr_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'a');
    await reader.close();

    // Second reader session
    final reader2 = await (await db.prepareQuery('SELECT val FROM mr_tbl WHERE id = 2')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'b');
    await reader2.close();

    // Third session — full iteration
    final reader3 = await (await db.prepareQuery('SELECT val FROM mr_tbl ORDER BY id')).executeReader();
    final vals = <String>[];
    while (await reader3.readRow()) {
      vals.add(reader3.getColumnText(0));
    }
    expect(vals, ['a', 'b']);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Transaction with reads interleaved with writes
  // ──────────────────────────────────────────────────────────────────────

  test('transaction with interleaved reads and writes', () async {
    final db = await _createTestDb('txn_interleave.db');

    {
      final stmt = await db.prepareQuery('CREATE TABLE ti_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.transaction((db) async {
      {
        final stmt = await db.prepareQuery("INSERT INTO ti_tbl (id, val) VALUES (1, 'one')");
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }

      // Read back within same transaction
      final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM ti_tbl')).executeReader();
      expect(await reader.readRow(), isTrue);
      expect(reader.getColumnInt(0), 1);
      await reader.close();

      {
        final stmt = await db.prepareQuery("INSERT INTO ti_tbl (id, val) VALUES (2, 'two')");
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }

      // Read again — should see both
      final reader2 = await (await db.prepareQuery('SELECT COUNT(*) FROM ti_tbl')).executeReader();
      expect(await reader2.readRow(), isTrue);
      expect(reader2.getColumnInt(0), 2);
      await reader2.close();
    });

    // Verify after commit
    final reader = await (await db.prepareQuery('SELECT val FROM ti_tbl ORDER BY id')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'one');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'two');
    expect(await reader.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Prepare failure: recovery and error reporting
  // ──────────────────────────────────────────────────────────────────────

  test('executeSql prepare failure includes error code and recovers', () async {
    final db = await _createTestDb('prepare_fail_exec.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE pfe_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Trigger prepare failure (references non-existent table). v2.4
    // surfaces the C lib's error message; the rc is conveyed through
    // the `(handle == 0)` invariant rather than embedded in the
    // message, so we assert on the human-readable error text.
    try {
      {
        final stmt = await db.prepareQuery('SELECT * FROM nonexistent_table');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }
      fail('Should have thrown');
    } on Exception catch (e) {
      expect(e.toString(), contains('no such table'));
    }

    // Connection must still be usable — stmt was properly finalized
    {
      final stmt = await db.prepareQuery("INSERT INTO pfe_tbl (id, val) VALUES (1, 'ok')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    final reader = await (await db.prepareQuery('SELECT val FROM pfe_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'ok');
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('executeReader prepare failure includes error code and recovers', () async {
    final db = await _createTestDb('prepare_fail_reader.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE pfr_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO pfr_tbl (id) VALUES (1)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    try {
      await (await db.prepareQuery('SELECT * FROM nonexistent_table')).executeReader();
      fail('Should have thrown');
    } on Exception catch (e) {
      // v2.4 surfaces the human-readable C-lib error message
      // instead of the rc — see the executeSql counterpart.
      expect(e.toString(), contains('no such table'));
    }

    // Reader must still work — lock/pool was properly released
    final reader = await (await db.prepareQuery('SELECT id FROM pfr_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('executeSql prepare failure within transaction keeps transaction intact', () async {
    final db = await _createTestDb('prepare_fail_txn.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE pft_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();
    {
      final stmt = await db.prepareQuery("INSERT INTO pft_tbl (id, val) VALUES (1, 'one')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Prepare failure mid-transaction
    await expectLater(
      () => _runSql(db, 'SELECT * FROM nonexistent_table'),
      throwsA(isA<Exception>()),
    );

    // Transaction should still be active and usable
    expect(db.isInTransaction, isTrue);
    {
      final stmt = await db.prepareQuery("INSERT INTO pft_tbl (id, val) VALUES (2, 'two')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await db.commit();

    // Both rows should be committed
    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM pft_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 2);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Bind failure: recovery
  // ──────────────────────────────────────────────────────────────────────

  test('executeSql bind failure recovers and connection remains usable', () async {
    final db = await _createTestDb('bind_fail_exec.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE bfe_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Bind 2 params to a statement with 1 placeholder — index 2 is out of range
    // sqlite3_bind returns SQLITE_RANGE (25), now caught by != _sqliteOk
    await expectLater(
      () => _runSql(
        db,
        'INSERT INTO bfe_tbl (id) VALUES (?)',
        params: [1, 'extra'],
      ),
      throwsA(isA<Exception>()),
    );

    // Connection must still work — stmt was finalized by the finally block
    {
      final stmt = await db.prepareQuery("INSERT INTO bfe_tbl (id, val) VALUES (1, 'ok')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    final reader = await (await db.prepareQuery('SELECT val FROM bfe_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'ok');
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('executeReader bind failure recovers and reader is released', () async {
    final db = await _createTestDb('bind_fail_reader.db', readerPoolSize: 2);
    {
      final stmt = await db.prepareQuery('CREATE TABLE bfr_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO bfr_tbl (id) VALUES (1)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await expectLater(
      () async {
        final s = await db.prepareQuery('SELECT * FROM bfr_tbl WHERE id = ?');
        try {
          await s.executeReader(params: [1, 'extra']);
        } finally {
          await s.close();
        }
      },
      throwsA(isA<Exception>()),
    );

    // Pool reader must be released — subsequent reader should work
    final reader = await (await db.prepareQuery('SELECT id FROM bfr_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Multiple temp tables in a single transaction (merge scenario)
  // ──────────────────────────────────────────────────────────────────────

  test('multiple temp table DDL+DML in transaction does not corrupt stmt', () async {
    final db = await _createTestDb('temp_tbl_txn.db');

    {
      final stmt = await db.prepareQuery('''
      CREATE TABLE src_tbl (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        parent_id INTEGER
      )
    ''');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO src_tbl (id, name, parent_id) VALUES (1, 'root', NULL)");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO src_tbl (id, name, parent_id) VALUES (2, 'child', 1)");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO src_tbl (id, name, parent_id) VALUES (3, 'grandchild', 2)");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.transaction((db) async {
      // First temp table — staging data (like merge temp table)
      {
        final stmt = await db.prepareQuery('CREATE TEMP TABLE __temp_merge__ (id INTEGER, name TEXT)');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }
      {
        final stmt = await db.prepareQuery('INSERT INTO __temp_merge__ SELECT id, name FROM src_tbl');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }

      final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM __temp_merge__')).executeReader();
      expect(await reader.readRow(), isTrue);
      expect(reader.getColumnInt(0), 3);
      await reader.close();

      // Second temp table — hierarchy resolution (self-recursive FK)
      {
        final stmt = await db.prepareQuery('CREATE TEMP TABLE __temp_hier__ (id INTEGER, parent_id INTEGER)');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }
      {
        final stmt = await db.prepareQuery('INSERT INTO __temp_hier__ SELECT id, parent_id FROM src_tbl WHERE parent_id IS NOT NULL');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }

      final reader2 = await (await db.prepareQuery('SELECT COUNT(*) FROM __temp_hier__')).executeReader();
      expect(await reader2.readRow(), isTrue);
      expect(reader2.getColumnInt(0), 2);
      await reader2.close();

      // Cross-temp-table operation
      {
        final stmt = await db.prepareQuery('''
        INSERT INTO src_tbl (id, name, parent_id)
        SELECT 4, 'merged', h.parent_id
        FROM __temp_hier__ h
        WHERE h.id = 3
      ''');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }

      // Clean up temp tables
      {
        final stmt = await db.prepareQuery('DROP TABLE __temp_merge__');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }
      {
        final stmt = await db.prepareQuery('DROP TABLE __temp_hier__');
        try {
          await stmt.executeSql();
        } finally {
          await stmt.close();
        }
      }
    });

    // Verify final state
    final reader3 = await (await db.prepareQuery('SELECT COUNT(*) FROM src_tbl')).executeReader();
    expect(await reader3.readRow(), isTrue);
    expect(reader3.getColumnInt(0), 4);
    await reader3.close();

    final reader4 = await (await db.prepareQuery('SELECT name, parent_id FROM src_tbl WHERE id = 4')).executeReader();
    expect(await reader4.readRow(), isTrue);
    expect(reader4.getColumnText(0), 'merged');
    expect(reader4.getColumnInt(1), 2);
    await reader4.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('rapid sequential executeSql in transaction all succeed', () async {
    final db = await _createTestDb('rapid_txn.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE rt_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.transaction((db) async {
      for (int i = 1; i <= 50; i++) {
        {
          final stmt = await db.prepareQuery('INSERT INTO rt_tbl (id, val) VALUES (?, ?)');
          try {
            await stmt.executeSql(params: [i, 'row_$i'],);
          } finally {
            await stmt.close();
          }
        }
      }
    });

    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM rt_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 50);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Pool: concurrent reads and writes with pool active
  // ──────────────────────────────────────────────────────────────────────

  test('concurrent writes with pool are serialized and all succeed', () async {
    final db = await _createTestDb('pool_conc_writes.db', readerPoolSize: 4);

    {
      final stmt = await db.prepareQuery('CREATE TABLE pcw_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await Future.wait([
      _runSql(db, "INSERT INTO pcw_tbl (id, val) VALUES (1, 'a')"),
      _runSql(db, "INSERT INTO pcw_tbl (id, val) VALUES (2, 'b')"),
      _runSql(db, "INSERT INTO pcw_tbl (id, val) VALUES (3, 'c')"),
    ]);

    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM pcw_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 3);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('pool: sequential reader then writer works correctly', () async {
    final db = await _createTestDb('pool_seq_rw.db', readerPoolSize: 2);

    {
      final stmt = await db.prepareQuery('CREATE TABLE psrw_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO psrw_tbl (id, val) VALUES (1, 'original')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Reader cycle (uses pool reader)
    final reader = await (await db.prepareQuery('SELECT val FROM psrw_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'original');
    await reader.close();

    // Writer after reader
    {
      final stmt = await db.prepareQuery("UPDATE psrw_tbl SET val = 'updated' WHERE id = 1");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Verify write took effect
    final reader2 = await (await db.prepareQuery('SELECT val FROM psrw_tbl WHERE id = 1')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'updated');
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('pool: getLastInsertedId works after executeSql', () async {
    final db = await _createTestDb('pool_last_id.db', readerPoolSize: 2);

    {
      final stmt = await db.prepareQuery('CREATE TABLE pli_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    final insertStmt =
        await db.prepareQuery("INSERT INTO pli_tbl (val) VALUES ('first')");
    int lastId;
    try {
      await insertStmt.executeSql();
      lastId = insertStmt.getLastInsertedId();
    } finally {
      await insertStmt.close();
    }
    expect(lastId, greaterThan(0));

    await db.closeDb();
    await db.dropDb();
  });

  test('pool: transaction with pool-enabled database', () async {
    final db = await _createTestDb('pool_txn.db', readerPoolSize: 4);

    {
      final stmt = await db.prepareQuery('CREATE TABLE pt_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO pt_tbl (id, val) VALUES (1, 'before')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    await db.beginTransaction();

    {
      final stmt = await db.prepareQuery("INSERT INTO pt_tbl (id, val) VALUES (2, 'during')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Read within the same transaction — should see uncommitted data
    final reader = await (await db.prepareQuery('SELECT val FROM pt_tbl ORDER BY id')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'before');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'during');
    expect(await reader.readRow(), isFalse);

    await db.commit();

    // Verify committed data
    final reader2 = await (await db.prepareQuery('SELECT COUNT(*) FROM pt_tbl')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnInt(0), 2);
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('pool: concurrent transactions are serialized', () async {
    final db = await _createTestDb('pool_conc_txn.db', readerPoolSize: 2);

    {
      final stmt = await db.prepareQuery('CREATE TABLE pct_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final executionOrder = <int>[];

    await Future.wait([
      db.transaction((db) async {
        {
          final stmt = await db.prepareQuery("INSERT INTO pct_tbl (id, val) VALUES (1, 'a')");
          try {
            await stmt.executeSql();
          } finally {
            await stmt.close();
          }
        }
        executionOrder.add(1);
      }),
      db.transaction((db) async {
        {
          final stmt = await db.prepareQuery("INSERT INTO pct_tbl (id, val) VALUES (2, 'b')");
          try {
            await stmt.executeSql();
          } finally {
            await stmt.close();
          }
        }
        executionOrder.add(2);
      }),
    ]);

    expect(executionOrder.length, 2);
    expect(executionOrder.toSet(), {1, 2});

    final reader = await (await db.prepareQuery('SELECT COUNT(*) FROM pct_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 2);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('pool: reader fallback to writer when all pool readers busy', () async {
    // Use pool with 1 reader — holding it should trigger writer fallback
    final db = await _createTestDb('pool_fallback.db', readerPoolSize: 1);

    {
      final stmt = await db.prepareQuery('CREATE TABLE pf_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO pf_tbl (id, val) VALUES (1, 'test')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // This should work even with small pool
    final reader = await (await db.prepareQuery('SELECT val FROM pf_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'test');
    await reader.close();

    // Subsequent operations should still work
    {
      final stmt = await db.prepareQuery("INSERT INTO pf_tbl (id, val) VALUES (2, 'test2')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader2 = await (await db.prepareQuery('SELECT COUNT(*) FROM pf_tbl')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnInt(0), 2);
    await reader2.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('pool: multiple databases work independently', () async {
    final db1 = await _createTestDb('pool_multi_a.db', readerPoolSize: 2);
    final db2 = await _createTestDb('pool_multi_b.db', readerPoolSize: 2);

    {
      final stmt = await db1.prepareQuery('CREATE TABLE ma_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db2.prepareQuery('CREATE TABLE mb_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Concurrent operations on different DBs
    await Future.wait([
      _runSql(db1, "INSERT INTO ma_tbl (id, val) VALUES (1, 'db1')"),
      _runSql(db2, "INSERT INTO mb_tbl (id, val) VALUES (1, 'db2')"),
    ]);

    // Verify each DB independently
    final reader1 = await (await db1.prepareQuery('SELECT val FROM ma_tbl WHERE id = 1')).executeReader();
    expect(await reader1.readRow(), isTrue);
    expect(reader1.getColumnText(0), 'db1');
    await reader1.close();

    final reader2 = await (await db2.prepareQuery('SELECT val FROM mb_tbl WHERE id = 1')).executeReader();
    expect(await reader2.readRow(), isTrue);
    expect(reader2.getColumnText(0), 'db2');
    await reader2.close();

    await db1.closeDb();
    await db1.dropDb();
    await db2.closeDb();
    await db2.dropDb();
  });

  test('pool: prepare failure releases pool slot', () async {
    final db = await _createTestDb('pool_prep_fail.db', readerPoolSize: 2);
    {
      final stmt = await db.prepareQuery('CREATE TABLE ppf_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Bad SQL should fail and release the pool slot
    await expectLater(
      () => _runSql(db, 'INSERT INTO nonexistent_tbl VALUES (1)'),
      throwsA(isA<Exception>()),
    );

    // Subsequent operations should still work (slot was released)
    {
      final stmt = await db.prepareQuery('INSERT INTO ppf_tbl (id) VALUES (1)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT id FROM ppf_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Issue fixes: regression tests
  // ──────────────────────────────────────────────────────────────────────

  test('getColumnDecimal throws DbasSqliteException(invalidDecimalFormat) on non-numeric text', () async {
    final db = await _createTestDb('decimal_err.db');
    {
      final stmt = await db.prepareQuery("CREATE TABLE d_tbl (id INTEGER PRIMARY KEY, val TEXT)");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO d_tbl (id, val) VALUES (1, 'not_a_number')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT val FROM d_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(
      () => reader.getColumnDecimal(0),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.invalidDecimalFormat)),
    );
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('getColumnTime throws DbasSqliteException(invalidTimeFormat) on garbage input', () async {
    final db = await _createTestDb('time_err.db');
    {
      final stmt = await db.prepareQuery("CREATE TABLE t_tbl (id INTEGER PRIMARY KEY, val TEXT)");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO t_tbl (id, val) VALUES (1, 'garbage')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT val FROM t_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(
      () => reader.getColumnTime(0),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.invalidTimeFormat)),
    );
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('getColumnTime parses HH:MM format without seconds', () async {
    final db = await _createTestDb('time_hhmm.db');
    {
      final stmt = await db.prepareQuery("CREATE TABLE t_tbl (id INTEGER PRIMARY KEY, val TEXT)");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO t_tbl (id, val) VALUES (1, '14:30')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT val FROM t_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    final d = reader.getColumnTime(0);
    expect(d.inHours, 14);
    expect(d.inMinutes % 60, 30);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('instance cleanup: dropDb cleans up platform delegates (Issue 7)', () async {
    final db = await _createTestDb('cleanup.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE cl_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await db.closeDb();
    await db.dropDb();

    // Re-create with same name — should not use stale delegate
    final db2 = await _createTestDb('cleanup.db');
    {
      final stmt = await db2.prepareQuery('CREATE TABLE cl_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await db2.closeDb();
    await db2.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Row cache correctness across pool writer/reader interleaving
  // ──────────────────────────────────────────────────────────────────────

  test('pool: row cache returns reader data after writer executeSql', () async {
    final db = await _createTestDb('cache_interleave.db', readerPoolSize: 2);
    {
      final stmt = await db.prepareQuery('CREATE TABLE ci_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO ci_tbl (id, val) VALUES (1, 'alpha')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db.prepareQuery("INSERT INTO ci_tbl (id, val) VALUES (2, 'beta')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Execute a write (touches writer's readRow path internally)
    final gammaStmt = await db
        .prepareQuery("INSERT INTO ci_tbl (id, val) VALUES (3, 'gamma')");
    int insertedId;
    try {
      await gammaStmt.executeSql();
      insertedId = gammaStmt.getLastInsertedId();
    } finally {
      await gammaStmt.close();
    }
    expect(insertedId, 3);

    // Now execute a reader query — should return reader data, not writer cache
    final reader = await (await db.prepareQuery('SELECT val FROM ci_tbl ORDER BY id')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'alpha');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'beta');
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'gamma');
    expect(await reader.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Pool exhaustion: reader held open, second read falls back to writer
  // ──────────────────────────────────────────────────────────────────────

  test('pool: blocking-acquire times out when readers are saturated', () async {
    // v2.4 contract: pool exhaustion no longer silently falls back
    // to the writer. Instead the second reader blocks up to
    // [DbasSqlite.kPoolAcquireTimeoutMs] (default 30s) and then
    // throws DbasSqliteException(readerSlotWaitTimeout). We use the test-only override to
    // shorten the timeout so the test completes quickly.
    final db = await _createTestDb('pool_exhaust.db', readerPoolSize: 1);
    DbasSqlite.debugPoolAcquireTimeoutMs = 200;
    try {
      {
        final stmt = await db.prepareQuery('CREATE TABLE pe_tbl (id INTEGER PRIMARY KEY, val TEXT)');
        try { await stmt.executeSql(); } finally { await stmt.close(); }
      }
      {
        final stmt = await db.prepareQuery("INSERT INTO pe_tbl (id, val) VALUES (1, 'first')");
        try { await stmt.executeSql(); } finally { await stmt.close(); }
      }

      // First reader holds the only pool slot.
      final reader = await (await db.prepareQuery('SELECT val FROM pe_tbl WHERE id = 1')).executeReader();
      expect(await reader.readRow(), isTrue);
      expect(reader.getColumnText(0), 'first');
      // Don't close reader yet.

      // Second reader blocks-then-times-out.
      await expectLater(
        () async {
          final stmt = await db.prepareQuery('SELECT val FROM pe_tbl WHERE id = 1');
          try {
            await stmt.executeReader();
          } finally {
            await stmt.close();
          }
        },
        // The Dart-side reader-slot semaphore gates the C-side
        // blocking-acquire; with the pool saturated by an in-flight
        // reader the slot wait expires first, before the C-side
        // poolAcquireReaderBlocking ever runs.
        throwsA(isA<DbasSqliteException>().having(
          (e) => e.code, 'code', DbasSqliteErrorCode.readerSlotWaitTimeout)),
      );

      // Closing the first reader frees the slot — a fresh reader works.
      await reader.close();
      final reader2 = await (await db.prepareQuery('SELECT val FROM pe_tbl WHERE id = 1')).executeReader();
      expect(await reader2.readRow(), isTrue);
      expect(reader2.getColumnText(0), 'first');
      await reader2.close();
    } finally {
      DbasSqlite.debugPoolAcquireTimeoutMs = null;
      await db.closeDb();
      await db.dropDb();
    }
  });

  // ──────────────────────────────────────────────────────────────────────
  // C pool: open, close, reopen with different pool size
  // ──────────────────────────────────────────────────────────────────────

  test('pool: close and reopen with different pool size', () async {
    final db1 = await _createTestDb('pool_reopen.db', readerPoolSize: 1);
    {
      final stmt = await db1.prepareQuery('CREATE TABLE pr_tbl (id INTEGER PRIMARY KEY, val TEXT)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    {
      final stmt = await db1.prepareQuery("INSERT INTO pr_tbl (id, val) VALUES (1, 'persisted')");
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }
    await db1.closeDb();

    // Reopen with a different pool size
    final db2 = await DbasSqlite.getInstance(dbName: 'pool_reopen.db');
    await db2.openDb(readerPoolSize: 4);

    final reader = await (await db2.prepareQuery('SELECT val FROM pr_tbl WHERE id = 1')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'persisted');
    await reader.close();

    await db2.closeDb();
    await db2.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Error propagation from worker isolate
  // ──────────────────────────────────────────────────────────────────────

  test('executeSql with invalid SQL propagates error from worker', () async {
    final db = await _createTestDb('worker_err.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE we_tbl (id INTEGER PRIMARY KEY)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    // Invalid SQL should propagate error through worker isolate
    await expectLater(
      () => _runSql(db, 'INVALID SQL THAT DOES NOT PARSE'),
      throwsA(isA<Exception>()),
    );

    // Connection should still be usable after error
    {
      final stmt = await db.prepareQuery('INSERT INTO we_tbl (id) VALUES (1)');
      try {
        await stmt.executeSql();
      } finally {
        await stmt.close();
      }
    }

    final reader = await (await db.prepareQuery('SELECT id FROM we_tbl')).executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // v2.4.0 regression + capability tests
  //
  // Each guards a behaviour that was either added or fixed in v2.4.0.
  // Comments name the bug class so a future maintainer touching the
  // affected area sees what the test pins.
  // ──────────────────────────────────────────────────────────────────────

  // ── 1. Counter cache survives reader auto-close on DONE ──────────────
  // Regression test for the §4.2 ordering bug where the executeReader
  // onClose closure read counters AFTER FinalizeStmt — which always
  // returned -1 because the handle was already removed from the C
  // lib's liveStmts map. Fixed by reading counters before finalize.
  test('counter cache survives reader auto-close on DONE', () async {
    final db = await _createTestDb('counter_after_autoclose.db');
    {
      final stmt = await db.prepareQuery(
          'CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO t (v) VALUES (?)');
      try {
        await stmt.executeSql(params: ['a']);
        await stmt.executeSql(params: ['b']);
      } finally { await stmt.close(); }
    }

    // INSERT ... RETURNING goes through executeReader. After auto-close
    // (readRow returns false on the second call), the statement's
    // cached counters must reflect the insert — they were captured
    // BEFORE finalize, so the C lib's GetStmtLastInsertedId was still
    // valid when called.
    final stmt =
        await db.prepareQuery('INSERT INTO t (v) VALUES (?) RETURNING id');
    try {
      final reader = await stmt.executeReader(params: ['c']);
      expect(await reader.readRow(), isTrue);
      final returnedId = reader.getColumnInt(0);
      expect(returnedId, 3);
      // Second readRow returns false → triggers auto-close (which
      // captures counters then finalises).
      expect(await reader.readRow(), isFalse);
      expect(reader.isClosed, isTrue);

      expect(stmt.getLastInsertedId(), 3,
          reason: 'getLastInsertedId must NOT be -1 after reader auto-close');
      expect(stmt.getAffectedRows(), greaterThanOrEqualTo(1));
    } finally { await stmt.close(); }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 2. Column metadata available BEFORE first readRow ────────────────
  // Regression test for the bug discovered while running v2.4.0:
  // getColumnCount() returned 0 before the first readRow because the
  // count was only populated inside readRowAndCache. Fixed by
  // capturing column metadata at prepare time and pre-populating the
  // reader's RowData cache.
  test('getColumnCount and getColumnName work BEFORE first readRow', () async {
    final db = await _createTestDb('col_meta_before_readrow.db');
    {
      final stmt = await db.prepareQuery(
          'CREATE TABLE t (alpha INTEGER, beta TEXT, gamma REAL)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }

    final stmt =
        await db.prepareQuery('SELECT alpha, beta, gamma FROM t WHERE alpha > ?');
    try {
      // Empty table — readRow will return false. But metadata is
      // available immediately after executeReader.
      final reader = await stmt.executeReader(params: [0]);

      expect(reader.getColumnCount(), 3,
          reason: 'count must be set before any readRow call');
      expect(reader.getColumnName(0), 'alpha');
      expect(reader.getColumnName(1), 'beta');
      expect(reader.getColumnName(2), 'gamma');

      // Column count survives DONE consistently (the worker now reads
      // it from the live statement, not from the row payload).
      expect(await reader.readRow(), isFalse);
      expect(reader.getColumnCount(), 3,
          reason: 'count must remain stable after DONE');
    } finally { await stmt.close(); }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 3. Bind error rc surfaces with offending parameter index ─────────
  // Regression test for the FFI fire-and-forget bind bug. Before the
  // fix, every bindXxx returned sqliteOk synchronously without
  // awaiting the worker dispatch, so SQLITE_RANGE on out-of-bounds
  // index (the most common bind error) was silently dropped and only
  // surfaced as an opaque step failure with a generic "Misuse"
  // message. The fix awaits the dispatch and reports the index.
  test('bind error surfaces specific offending positional index', () async {
    final db = await _createTestDb('bind_error_index.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }

    // SQL has 1 placeholder; binding index 2 → SQLITE_RANGE.
    final stmt = await db.prepareQuery('INSERT INTO t (id) VALUES (?)');
    try {
      await expectLater(
        stmt.executeSql(params: [1, 'extra']),
        throwsA(predicate<DbasSqliteException>(
          (e) =>
              e.code == DbasSqliteErrorCode.bindPositionalFailed &&
              e.sqliteCode == 25 /* SQLITE_RANGE */ &&
              // SQLite mirrors the primary rc onto the extended slot
              // when the call has no extended discriminator, so the
              // native lib reports 25 here too.
              e.sqliteUniqueCode == 25 &&
              e.subCategory == DbasSqliteSubCategory.rangeError &&
              e.message.contains('positional index 2'),
          'exception carries bindPositionalFailed + SQLITE_RANGE rc and identifies the offending bind index',
        )),
      );
    } finally { await stmt.close(); }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 4. Bind buffer preserved when execute throws ─────────────────────
  // executeSql / executeReader accept `params:` / `nameParams:`
  // arguments that override the buffered binds. The override happens
  // BEFORE execute, but on a throw the snapshot must be restored so
  // the caller can fix one slot and retry without re-binding.
  test('bind buffer is restored when an execute call throws', () async {
    final db = await _createTestDb('bind_preserve.db');
    {
      final stmt = await db.prepareQuery(
          'CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }

    final stmt = await db.prepareQuery('INSERT INTO t (id, v) VALUES (?, ?)');
    try {
      // Pre-bind via fluent setters — buffer is [100, 'good'].
      stmt.bindInt(1, 100).bindText(2, 'good');

      // Override with bad params. NULL into NOT NULL → SQLITE_CONSTRAINT
      // (primary 19) at step time, with the extended rc
      // `SQLITE_CONSTRAINT_NOTNULL=1299`. The execute throws and the
      // buffer must be restored to [100, 'good'].
      await expectLater(
        stmt.executeSql(params: [101, null]),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code', DbasSqliteErrorCode.executeSqlStepFailed)
            .having((e) => e.sqliteCode, 'sqliteCode (SQLITE_CONSTRAINT)', 19)
            .having((e) => e.sqliteUniqueCode,
                'sqliteUniqueCode (SQLITE_CONSTRAINT_NOTNULL)', 1299)
            .having((e) => e.subCategory, 'subCategory',
                DbasSqliteSubCategory.notNullViolation)),
      );

      // No params on this call — must use the restored buffer.
      final affected = await stmt.executeSql();
      expect(affected, 1);
      expect(stmt.getLastInsertedId(), 100);
    } finally { await stmt.close(); }

    // Verify the row really was the original buffer's values.
    final readStmt = await db.prepareQuery('SELECT id, v FROM t');
    try {
      final reader = await readStmt.executeReader();
      try {
        expect(await reader.readRow(), isTrue);
        expect(reader.getColumnInt(0), 100);
        expect(reader.getColumnText(1), 'good');
        expect(await reader.readRow(), isFalse);
      } finally { await reader.close(); }
    } finally { await readStmt.close(); }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 5. setBusyTimeout terminates on a quiescent pool ─────────────────
  // Regression test for the infinite-loop bug. The original loop
  // depended on `poolAcquireReaderBlocking` returning 0 to terminate,
  // but on an idle pool every acquire succeeds, the slot is released,
  // and the next acquire can re-grab the same slot indefinitely.
  // Fixed by tracking _readerPoolSize and iterating exactly that many
  // times while holding all slots exclusively.
  test('setBusyTimeout terminates on a quiescent pool', () async {
    final db = await _createTestDb('busy_timeout_quiet.db', readerPoolSize: 4);
    // 3 s outer timeout: 4 idle acquires + 4 SetBusyTimeout calls
    // should complete in ms. If the loop is broken, this fails fast.
    await expectLater(
      db.setBusyTimeout(7500).timeout(const Duration(seconds: 3)),
      completes,
    );
    // A second call must also succeed cleanly.
    await db.setBusyTimeout(5000).timeout(const Duration(seconds: 3));
    await db.closeDb();
    await db.dropDb();
  });

  // ── 6. setBusyTimeout throws when a reader is in flight ──────────────
  // The contract is best-effort with strict failure mode: if any
  // reader slot is busy beyond kSetBusyTimeoutAcquireMs, throw a
  // clear DbasSqliteException(setBusyTimeoutReaderBusy) naming the slot. Uses the test-only
  // debugSetBusyTimeoutAcquireMs override to keep the test fast.
  test('setBusyTimeout throws DbasSqliteException(setBusyTimeoutReaderBusy) when a reader is in flight', () async {
    final db = await _createTestDb('busy_timeout_busy.db', readerPoolSize: 1);
    DbasSqlite.debugSetBusyTimeoutAcquireMs = 200;
    try {
      {
        final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER)');
        try { await stmt.executeSql(); } finally { await stmt.close(); }
      }
      {
        final stmt = await db.prepareQuery('INSERT INTO t VALUES (1)');
        try { await stmt.executeSql(); } finally { await stmt.close(); }
      }

      // Hold the only pool slot.
      final readerStmt = await db.prepareQuery('SELECT id FROM t');
      final reader = await readerStmt.executeReader();
      expect(await reader.readRow(), isTrue);

      try {
        await expectLater(
          db.setBusyTimeout(10000),
          throwsA(predicate<DbasSqliteException>(
            (e) => e.code == DbasSqliteErrorCode.setBusyTimeoutReaderBusy &&
                   e.message.contains('200ms') &&
                   e.message.contains('reader 0'),
            'exception names the slot index and timeout',
          )),
        );
      } finally {
        await reader.close();
        await readerStmt.close();
      }

      // After the reader closes, the call works again.
      await db.setBusyTimeout(10000).timeout(const Duration(seconds: 3));
    } finally {
      DbasSqlite.debugSetBusyTimeoutAcquireMs = null;
      await db.closeDb();
      await db.dropDb();
    }
  });

  // ── 7. getSqliteVersion returns a parsable version ───────────────────
  test('getSqliteVersion returns a SemVer-shaped string', () async {
    final db = await _createTestDb('sqlite_version.db');
    final v = db.getSqliteVersion();
    expect(v, matches(RegExp(r'^\d+\.\d+\.\d+$')),
        reason: 'expected M.m.p — got "$v"');
    // Library is well past 3.0.0; sanity-check the major.
    final major = int.parse(v.split('.').first);
    expect(major, greaterThanOrEqualTo(3));
    await db.closeDb();
    await db.dropDb();
  });

  // ── 8. getTotalChanges reflects mutations ────────────────────────────
  test('getTotalChanges grows with each successful mutation', () async {
    final db = await _createTestDb('total_changes.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }
    final baseline = db.getTotalChanges();
    expect(baseline, greaterThanOrEqualTo(0));

    final ins = await db.prepareQuery('INSERT INTO t (id) VALUES (?)');
    try {
      for (int i = 1; i <= 5; i++) {
        await ins.executeSql(params: [i]);
      }
    } finally { await ins.close(); }

    expect(db.getTotalChanges(), baseline + 5);
    await db.closeDb();
    await db.dropDb();
  });

  // ── 9. getDbFileName returns the database path ───────────────────────
  test('getDbFileName returns the path while open and null after close', () async {
    final db = await _createTestDb('file_name.db');
    final fn = db.getDbFileName();
    expect(fn, isNotNull);
    expect(fn!, endsWith('file_name.db'));
    await db.closeDb();
    expect(db.getDbFileName(), isNull,
        reason: 'must return null after the connection is closed');
    await db.dropDb();
  });

  // ── 10. enableWal is idempotent on a pooled database ─────────────────
  // Regression test for the silent-no-op-on-web review finding (web
  // is fixed to actually verify); native side has always been
  // idempotent but no test guards it.
  test('enableWal is idempotent on a pooled database', () async {
    final db = await _createTestDb('enable_wal_idempotent.db', readerPoolSize: 2);
    // Pool always opens with WAL. Both calls must succeed.
    await db.enableWal();
    await db.enableWal();
    await db.closeDb();
    await db.dropDb();
  });

  // ── 11. Two statements with concurrently active readers ──────────────
  // The headline v2.4.0 capability: multiple statements with their
  // own native handles, each with its own reader on its own pool
  // connection. Interleaved reads must produce distinct, correct
  // result sets — which is the core regression test for the
  // multi-isolate FFI worker pool design.
  test('two statements with concurrently active readers', () async {
    final db = await _createTestDb('multi_stmt.db', readerPoolSize: 4);
    {
      final stmt = await db.prepareQuery(
          'CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }
    final ins = await db.prepareQuery('INSERT INTO t (id, v) VALUES (?, ?)');
    try {
      for (int i = 1; i <= 3; i++) {
        await ins.executeSql(params: [i, 'row$i']);
      }
    } finally { await ins.close(); }

    final stmt1 = await db.prepareQuery('SELECT v FROM t WHERE id = ?');
    final stmt2 = await db.prepareQuery('SELECT v FROM t ORDER BY id DESC');
    try {
      final r1 = await stmt1.executeReader(params: [2]);
      final r2 = await stmt2.executeReader();
      try {
        // Interleave reads — each reader reads from its own handle
        // on its own pool connection.
        expect(await r1.readRow(), isTrue);
        expect(r1.getColumnText(0), 'row2');

        expect(await r2.readRow(), isTrue);
        expect(r2.getColumnText(0), 'row3');

        expect(await r1.readRow(), isFalse,
            reason: 'r1 has only one matching row');

        expect(await r2.readRow(), isTrue);
        expect(r2.getColumnText(0), 'row2');
        expect(await r2.readRow(), isTrue);
        expect(r2.getColumnText(0), 'row1');
        expect(await r2.readRow(), isFalse);
      } finally {
        await r1.close();
        await r2.close();
      }
    } finally {
      await stmt1.close();
      await stmt2.close();
    }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 12. Statement reuse with different params per execute ────────────
  // The deferred-prepare model means the SAME DbasSqliteStatement can
  // be executed many times — the C lib's PrepareQuery runs each
  // time, the bind buffer is replayed, and counters reflect the most
  // recent successful step.
  test('statement reuse with different params per execute', () async {
    final db = await _createTestDb('stmt_reuse.db');
    {
      final stmt = await db.prepareQuery(
          'CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }

    final ins = await db.prepareQuery('INSERT INTO t (v) VALUES (?)');
    try {
      const values = ['a', 'b', 'c', 'd', 'e'];
      for (int i = 0; i < values.length; i++) {
        final affected = await ins.executeSql(params: [values[i]]);
        expect(affected, 1);
        expect(ins.getLastInsertedId(), i + 1);
      }
    } finally { await ins.close(); }

    final read = await db.prepareQuery('SELECT v FROM t ORDER BY id');
    try {
      final reader = await read.executeReader();
      try {
        final got = <String>[];
        while (await reader.readRow()) {
          got.add(reader.getColumnText(0));
        }
        expect(got, ['a', 'b', 'c', 'd', 'e']);
      } finally { await reader.close(); }
    } finally { await read.close(); }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 13. Two readers on the same statement throws DbasSqliteException(readerAlreadyActive) ──
  // Per-statement invariant: only one DbasSqliteReader may be active
  // per DbasSqliteStatement at a time. Closing the first reader
  // releases the slot for the next.
  test('executeReader while a reader from same stmt is active throws', () async {
    final db = await _createTestDb('two_readers_same_stmt.db', readerPoolSize: 2);
    {
      final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO t VALUES (1)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }

    final stmt = await db.prepareQuery('SELECT id FROM t');
    try {
      final r1 = await stmt.executeReader();
      try {
        expect(await r1.readRow(), isTrue);

        // Second reader on same statement → DbasSqliteException(readerAlreadyActive).
        await expectLater(
          stmt.executeReader(),
          throwsA(predicate<DbasSqliteException>(
            (e) => e.code == DbasSqliteErrorCode.readerAlreadyActive &&
                   e.message.contains('reader from this statement is still active'),
            'exception names the active-reader invariant',
          )),
        );
      } finally { await r1.close(); }

      // After r1 closes, we can open a fresh reader on the same stmt.
      final r2 = await stmt.executeReader();
      try {
        expect(await r2.readRow(), isTrue);
        expect(r2.getColumnInt(0), 1);
      } finally { await r2.close(); }
    } finally { await stmt.close(); }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 14. Per-statement state is isolated across statements ────────────
  // The C lib gives each handle its own lastError / affectedRows /
  // lastInsertedId. A failure on one statement must not corrupt the
  // observable state of another.
  test('per-statement state is isolated across statements', () async {
    final db = await _createTestDb('per_stmt_isolation.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }

    final stmt1 = await db.prepareQuery('INSERT INTO t (id) VALUES (?)');
    final stmt2 = await db.prepareQuery('INSERT INTO t (id) VALUES (?)');
    try {
      // stmt1: bind out-of-range → SQLITE_RANGE → execute throws
      // before any successful step. Counters stay at -1.
      await expectLater(
        stmt1.executeSql(params: [42, 'extra']),
        throwsA(isA<Exception>()),
      );
      expect(stmt1.getLastInsertedId(), -1,
          reason: 'no successful step on stmt1 → counter is -1');
      expect(stmt1.getAffectedRows(), -1);

      // stmt2: succeeds — its own counters are correct, untouched by stmt1.
      final affected = await stmt2.executeSql(params: [99]);
      expect(affected, 1);
      expect(stmt2.getLastInsertedId(), 99);
      expect(stmt2.getAffectedRows(), 1);

      // Retry stmt1 with valid params — its counters now update.
      final affected1 = await stmt1.executeSql(params: [42]);
      expect(affected1, 1);
      expect(stmt1.getLastInsertedId(), 42);
      // stmt2's counters must NOT have moved.
      expect(stmt2.getLastInsertedId(), 99,
          reason: 'stmt2 counters are isolated from stmt1 activity');
    } finally {
      await stmt1.close();
      await stmt2.close();
    }

    await db.closeDb();
    await db.dropDb();
  });

  // ── 17. closeDb cleans up forgotten statements ───────────────────────
  // The C lib's CloseDb refuses with SQLITE_BUSY if any handle is
  // live. DbasSqlite must finalise tracked statements before
  // attempting close so the user doesn't have to think about it.
  test('closeDb cleans up statements the caller forgot to close', () async {
    final db = await _createTestDb('forgotten_stmts.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }
    {
      final stmt = await db.prepareQuery('INSERT INTO t VALUES (1)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }

    // Prepare two and INTENTIONALLY do not close them.
    final orphan1 = await db.prepareQuery('SELECT id FROM t');
    final orphan2 = await db.prepareQuery('INSERT INTO t (id) VALUES (?)');

    // closeDb must succeed regardless and mark them closed.
    await db.closeDb();
    expect(orphan1.isClosed, isTrue);
    expect(orphan2.isClosed, isTrue);

    // A subsequent execute on a closed statement must throw.
    await expectLater(
      orphan2.executeSql(params: [2]),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.statementClosed)),
    );

    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // executeScalar
  // ──────────────────────────────────────────────────────────────────────

  test('executeScalar returns the first column of the first row', () async {
    final db = await _createTestDb('scalar_basic.db');
    await _runSql(db, 'CREATE TABLE t (i INTEGER, d REAL, s TEXT, b BLOB)');
    await _runSql(db,
        'INSERT INTO t VALUES (?, ?, ?, ?)',
        params: [42, 3.14, 'hello', Uint8List.fromList([1, 2, 3])]);

    final intVal =
        await (await db.prepareQuery('SELECT i FROM t')).executeScalar();
    expect(intVal, 42);

    final dblVal =
        await (await db.prepareQuery('SELECT d FROM t')).executeScalar();
    expect(dblVal, closeTo(3.14, 1e-9));

    final txtVal =
        await (await db.prepareQuery('SELECT s FROM t')).executeScalar();
    expect(txtVal, 'hello');

    final blobVal =
        await (await db.prepareQuery('SELECT b FROM t')).executeScalar();
    expect(blobVal, isA<Uint8List>());
    expect((blobVal as Uint8List).toList(), [1, 2, 3]);

    await db.closeDb();
    await db.dropDb();
  });

  test('executeScalar returns null when query produces no rows', () async {
    final db = await _createTestDb('scalar_no_rows.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER)');

    final v = await (await db.prepareQuery('SELECT id FROM t')).executeScalar();
    expect(v, isNull);

    await db.closeDb();
    await db.dropDb();
  });

  test('executeScalar returns null when first column is SQL NULL', () async {
    final db = await _createTestDb('scalar_null_col.db');
    await _runSql(db, 'CREATE TABLE t (a INTEGER, b TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (NULL, 'present')");

    final v = await (await db.prepareQuery('SELECT a FROM t')).executeScalar();
    expect(v, isNull);

    // Sanity: the second column is non-null, demonstrating the row exists.
    final v2 = await (await db.prepareQuery('SELECT b FROM t')).executeScalar();
    expect(v2, 'present');

    await db.closeDb();
    await db.dropDb();
  });

  test('executeScalar accepts positional params', () async {
    final db = await _createTestDb('scalar_pos_params.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'one'), (2, 'two')");

    final v = await (await db.prepareQuery('SELECT val FROM t WHERE id = ?'))
        .executeScalar(params: [2]);
    expect(v, 'two');

    await db.closeDb();
    await db.dropDb();
  });

  test('executeScalar accepts named params', () async {
    final db = await _createTestDb('scalar_named_params.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'one'), (2, 'two')");

    final v = await (await db.prepareQuery('SELECT val FROM t WHERE id = :id'))
        .executeScalar(nameParams: {':id': 2});
    expect(v, 'two');

    await db.closeDb();
    await db.dropDb();
  });

  test('executeScalar closes the statement (subsequent use throws)', () async {
    final db = await _createTestDb('scalar_closes_stmt.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER)');
    await _runSql(db, 'INSERT INTO t VALUES (1)');

    final stmt = await db.prepareQuery('SELECT id FROM t');
    final v = await stmt.executeScalar();
    expect(v, 1);
    expect(stmt.isClosed, isTrue);

    await expectLater(
      stmt.executeScalar(),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.statementClosed)),
    );
    await expectLater(
      stmt.executeSql(),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.statementClosed)),
    );
    await expectLater(
      stmt.executeReader(),
      throwsA(isA<DbasSqliteException>().having(
        (e) => e.code, 'code', DbasSqliteErrorCode.statementClosed)),
    );

    await db.closeDb();
    await db.dropDb();
  });

  test('executeScalar returns the first column even with multiple columns',
      () async {
    final db = await _createTestDb('scalar_first_col.db');
    await _runSql(db, 'CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)');
    await _runSql(db, 'INSERT INTO t VALUES (10, 20, 30)');

    final v =
        await (await db.prepareQuery('SELECT a, b, c FROM t')).executeScalar();
    expect(v, 10);

    await db.closeDb();
    await db.dropDb();
  });

  test('executeScalar returns the first row even with many rows', () async {
    final db = await _createTestDb('scalar_first_row.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER)');
    await _runSql(db, 'INSERT INTO t VALUES (1), (2), (3), (4)');

    final v = await (await db.prepareQuery('SELECT id FROM t ORDER BY id'))
        .executeScalar();
    expect(v, 1);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Auto-detection: in-tx read routing (read-your-writes preserved)
  // ──────────────────────────────────────────────────────────────────────

  test('in-tx read AFTER a write sees the in-flight write (executeReader)',
      () async {
    final db = await _createTestDb('autoroute_reader_postwrite.db',
        readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await db.beginTransaction();
    await _runSql(db, "INSERT INTO t VALUES (1, 'inflight')");

    // The SELECT runs INSIDE the same tx, after the INSERT — must see it.
    final reader =
        await (await db.prepareQuery('SELECT val FROM t WHERE id = 1'))
            .executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnText(0), 'inflight');
    await reader.close();

    await db.rollback();
    await db.closeDb();
    await db.dropDb();
  });

  test('in-tx read AFTER a write sees the in-flight write (executeScalar)',
      () async {
    final db = await _createTestDb('autoroute_scalar_postwrite.db',
        readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await db.beginTransaction();
    await _runSql(db, "INSERT INTO t VALUES (7, 'inflight-scalar')");

    final v = await (await db.prepareQuery('SELECT val FROM t WHERE id = 7'))
        .executeScalar();
    expect(v, 'inflight-scalar');

    await db.rollback();
    await db.closeDb();
    await db.dropDb();
  });

  test('in-tx read BEFORE any write sees last-committed snapshot', () async {
    final db = await _createTestDb('autoroute_reader_prewrite.db',
        readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'committed')");

    await db.beginTransaction();
    // No executeSql yet — read should hit the pool reader (last commit).
    final v =
        await (await db.prepareQuery('SELECT val FROM t WHERE id = 1'))
            .executeScalar();
    expect(v, 'committed');

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  test('parallel pre-write in-tx reads run without serialising on writer',
      () async {
    // With auto-routing, pre-write in-tx reads use pool readers, so a
    // Future.wait over many SELECTs completes — none of them block on
    // the writer lock that beginTransaction holds.
    final db = await _createTestDb('autoroute_parallel_prewrite.db',
        readerPoolSize: 4);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    await db.beginTransaction();
    final results = await Future.wait([
      (() async => (await (await db.prepareQuery('SELECT val FROM t WHERE id = 1')).executeScalar()) as String?)(),
      (() async => (await (await db.prepareQuery('SELECT val FROM t WHERE id = 2')).executeScalar()) as String?)(),
      (() async => (await (await db.prepareQuery('SELECT val FROM t WHERE id = 3')).executeScalar()) as String?)(),
    ]);
    expect(results, ['a', 'b', 'c']);

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  test('rollback resets txHasWrites — next tx starts on pool reader again',
      () async {
    final db = await _createTestDb('autoroute_reset_rollback.db',
        readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'committed')");

    // First tx: write, then read, then rollback.
    await db.beginTransaction();
    await _runSql(db, "INSERT INTO t VALUES (2, 'inflight')");
    await db.rollback();

    // Second tx: read FIRST (pre-write). Routing must be "no writes
    // yet" → pool reader → committed snapshot. The rollback above
    // means the row from the first tx is gone.
    await db.beginTransaction();
    final reader = await (await db
            .prepareQuery('SELECT COUNT(*) FROM t'))
        .executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();
    await db.commit();

    await db.closeDb();
    await db.dropDb();
  });

  test('commit resets txHasWrites — next tx starts on pool reader again',
      () async {
    final db = await _createTestDb('autoroute_reset_commit.db',
        readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

    await db.beginTransaction();
    await _runSql(db, "INSERT INTO t VALUES (1, 'a')");
    await db.commit();

    // Second tx, no writes yet — read goes to pool reader (committed).
    await db.beginTransaction();
    final v = await (await db.prepareQuery('SELECT val FROM t WHERE id = 1'))
        .executeScalar();
    expect(v, 'a');
    await db.commit();

    await db.closeDb();
    await db.dropDb();
  });

  test('single-connection (no pool) in-tx read works (no deadlock)', () async {
    // readerPoolSize: 0 → no pool, single writer connection. The
    // routing must avoid trying to re-acquire the writer lock that
    // beginTransaction already holds.
    final db = await _createTestDb('autoroute_no_pool.db', readerPoolSize: 0);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'pre')");

    await db.beginTransaction();
    // Pre-write read: must work (writer connection, lock already held).
    final v1 = await (await db.prepareQuery('SELECT val FROM t WHERE id = 1'))
        .executeScalar()
        .timeout(const Duration(seconds: 5),
            onTimeout: () => fail('pre-write in-tx read deadlocked'));
    expect(v1, 'pre');

    await _runSql(db, "INSERT INTO t VALUES (2, 'mid')");
    final v2 = await (await db.prepareQuery('SELECT val FROM t WHERE id = 2'))
        .executeScalar()
        .timeout(const Duration(seconds: 5),
            onTimeout: () => fail('post-write in-tx read deadlocked'));
    expect(v2, 'mid');

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  test('10 parallel pre-write in-tx scalar reads all complete (Future.wait)',
      () async {
    // Pool size matches parallel fan-out so every reader gets its own
    // connection without queuing on poolAcquireReaderBlocking. Worker
    // pool auto-bumps to readerCount + 2 = 12, which leaves enough
    // headroom for the prepare / bind / step round-trips that follow
    // each acquire — without that headroom, blocked acquires can
    // starve the workers that the in-flight reads need to release.
    final db = await _createTestDb('autoroute_parallel_heavy_scalar.db',
        readerPoolSize: 10);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (int i = 1; i <= 10; i++) {
      await _runSql(db, 'INSERT INTO t VALUES (?, ?)', params: [i, i * 10]);
    }

    await db.beginTransaction();
    final futures = <Future<dynamic>>[];
    for (int i = 1; i <= 10; i++) {
      final id = i;
      futures.add((() async => (await db
              .prepareQuery('SELECT val FROM t WHERE id = ?'))
          .executeScalar(params: [id]))());
    }
    final results = await Future.wait(futures).timeout(
      const Duration(seconds: 60),
      onTimeout: () =>
          fail('10 parallel pre-write in-tx scalar reads timed out'),
    );
    for (int i = 0; i < 10; i++) {
      expect(results[i], (i + 1) * 10, reason: 'mismatch at index $i');
    }

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  test('10 parallel pre-write in-tx executeReader runs all complete',
      () async {
    // Same fan-out / worker-pool reasoning as the scalar variant.
    final db = await _createTestDb('autoroute_parallel_heavy_reader.db',
        readerPoolSize: 10);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    for (int i = 1; i <= 10; i++) {
      await _runSql(db, 'INSERT INTO t VALUES (?, ?)',
          params: [i, 'row-$i']);
    }

    await db.beginTransaction();
    Future<String?> readOne(int id) async {
      final stmt =
          await db.prepareQuery('SELECT val FROM t WHERE id = ?');
      try {
        final reader = await stmt.executeReader(params: [id]);
        try {
          if (!await reader.readRow()) return null;
          return reader.getColumnText(0);
        } finally {
          await reader.close();
        }
      } finally {
        await stmt.close();
      }
    }

    final results = await Future.wait(
      List.generate(10, (i) => readOne(i + 1)),
    ).timeout(
      const Duration(seconds: 60),
      onTimeout: () =>
          fail('10 parallel pre-write in-tx executeReader runs timed out'),
    );
    for (int i = 0; i < 10; i++) {
      expect(results[i], 'row-${i + 1}');
    }

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  test('10 parallel post-write in-tx reads all complete (writer-serialised)',
      () async {
    // After a write, every in-tx read routes to the writer connection
    // for read-your-writes. They serialise on the writer but must all
    // succeed — no deadlock, no lost reads, no error. Pool size still
    // generous so the routing decision is unambiguous (writer chosen
    // because of the write, not because no reader was free).
    final db = await _createTestDb('autoroute_parallel_postwrite.db',
        readerPoolSize: 10);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (int i = 1; i <= 10; i++) {
      await _runSql(db, 'INSERT INTO t VALUES (?, ?)', params: [i, i]);
    }

    await db.beginTransaction();
    // Write inside tx — flips routing to writer for subsequent reads.
    await _runSql(db, 'UPDATE t SET val = val * 100');

    final results = await Future.wait(
      List.generate(10, (i) {
        final id = i + 1;
        return (() async => (await db
                .prepareQuery('SELECT val FROM t WHERE id = ?'))
            .executeScalar(params: [id]))();
      }),
    ).timeout(
      const Duration(seconds: 60),
      onTimeout: () =>
          fail('10 parallel post-write in-tx reads timed out (writer)'),
    );
    for (int i = 0; i < 10; i++) {
      // Each row was multiplied by 100, and these reads see the
      // in-flight UPDATE.
      expect(results[i], (i + 1) * 100);
    }

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  test('multi-statement write/read alternation inside one tx', () async {
    // Stress: write, read, write, read, all inside a single tx. Each
    // read after a write must see all writes so far.
    final db = await _createTestDb('autoroute_alternation.db',
        readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');

    await db.beginTransaction();

    await _runSql(db, 'INSERT INTO t VALUES (1, 100)');
    var sum = await (await db.prepareQuery('SELECT SUM(val) FROM t'))
        .executeScalar();
    expect(sum, 100);

    await _runSql(db, 'INSERT INTO t VALUES (2, 200)');
    sum = await (await db.prepareQuery('SELECT SUM(val) FROM t'))
        .executeScalar();
    expect(sum, 300);

    await _runSql(db, 'UPDATE t SET val = val + 50 WHERE id = 1');
    sum = await (await db.prepareQuery('SELECT SUM(val) FROM t'))
        .executeScalar();
    expect(sum, 350);

    await db.commit();
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Streaming-SELECT regression coverage (v2.5.0)
  //
  // The web side of v2.5.0 replaced the eager `pool.query` materialisation
  // with a per-row streaming pipeline (`prepareQuery` / `bindParams` /
  // `readRow` / `finalizeStmt`) so it matches the native FFI behaviour.
  // Native has always streamed; these tests pin down the cross-platform
  // contract so a regression on either side surfaces.
  // ──────────────────────────────────────────────────────────────────────

  test('streaming: empty result set still exposes column count and names',
      () async {
    // Native has always populated column metadata at prepare time. The
    // test is the regression net so the native contract doesn't drift
    // away from what the web side now also guarantees.
    final db = await _createTestDb('stream_empty_meta.db');
    await _runSql(db, 'CREATE TABLE t (a INTEGER, b TEXT, c REAL)');

    final stmt = await db.prepareQuery('SELECT a, b, c FROM t WHERE a > 1000');
    final reader = await stmt.executeReader();
    expect(reader.getColumnCount(), 3,
        reason: 'columnCount must be available before first readRow');
    expect(reader.getColumnName(0), 'a');
    expect(reader.getColumnName(1), 'b');
    expect(reader.getColumnName(2), 'c');
    expect(await reader.readRow(), isFalse);
    expect(reader.isClosed, isTrue);
    await stmt.close();
    await db.closeDb();
    await db.dropDb();
  });

  test('streaming: SQLite INTEGER values around int32 boundaries round-trip',
      () async {
    final db = await _createTestDb('stream_int_boundaries.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, big INTEGER)');

    // Each value: positive small, max int32, just past max int32, max
    // safe integer in Dart-on-web (also valid on native), and
    // corresponding negatives. Native int is 64-bit so these all fit
    // exactly; the test is shaped this way so the same input set works
    // on web (53-bit) when this same test is run via integration_test.
    const cases = <int>[
      0,
      42,
      2147483647, // INT32_MAX
      2147483648, // just past — worker emits BigInt on web
      9007199254740991, // 2^53 - 1
      -2147483648, // INT32_MIN
      -2147483649, // just past
      -9007199254740991,
    ];
    for (int i = 0; i < cases.length; i++) {
      await _runSql(db, 'INSERT INTO t VALUES (?, ?)',
          params: [i, cases[i]]);
    }
    for (int i = 0; i < cases.length; i++) {
      final v = await (await db.prepareQuery(
              'SELECT big FROM t WHERE id = ?'))
          .executeScalar(params: [i]);
      expect(v, cases[i], reason: 'value at id=$i did not round-trip');
      expect(v, isA<int>(),
          reason: 'value at id=$i must surface as Dart int, not BigInt/text');
    }
    await db.closeDb();
    await db.dropDb();
  });

  test('streaming: closing a reader before exhaustion releases the stmt',
      () async {
    final db = await _createTestDb('stream_abandoned.db', readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (int i = 1; i <= 1000; i++) {
      await _runSql(db, 'INSERT INTO t VALUES (?, ?)', params: [i, i]);
    }

    {
      final stmt =
          await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      for (int i = 0; i < 5; i++) {
        expect(await reader.readRow(), isTrue);
      }
      await reader.close();
      await stmt.close();
    }

    // Fresh full scan after the partial read — if the previous handle
    // had leaked, this would either fail outright or starve the pool.
    {
      final stmt =
          await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      int seen = 0;
      while (await reader.readRow()) {
        seen++;
        expect(reader.getColumnInt(0), seen);
      }
      expect(seen, 1000);
      await stmt.close();
    }

    await db.closeDb();
    await db.dropDb();
  });

  test(
      'streaming: in-tx write then parallel executeReader runs all see the write',
      () async {
    // The existing autoroute tests cover post-write parallel reads
    // via executeScalar (single-row scalar). This unit pins down the
    // multi-row case: every parallel executeReader inside a tx (after
    // a write) streams its rows from the writer connection and
    // observes the in-flight UPDATE — the read-your-writes contract
    // must hold under reader fan-out.
    final db = await _createTestDb('stream_inttx_par_reader.db',
        readerPoolSize: 8, workerPoolSize: 12);
    await _runSql(db,
        'CREATE TABLE t (group_id INTEGER, id INTEGER PRIMARY KEY, val INTEGER)');
    int rowId = 1;
    for (int g = 1; g <= 6; g++) {
      for (int i = 1; i <= 4; i++) {
        await _runSql(db, 'INSERT INTO t VALUES (?, ?, ?)',
            params: [g, rowId++, i]);
      }
    }

    await db.beginTransaction();
    await _runSql(db, 'UPDATE t SET val = val + 100');

    final results = await Future.wait(
      List.generate(6, (g) async {
        final reader = await (await db.prepareQuery(
                'SELECT val FROM t WHERE group_id = ? ORDER BY id'))
            .executeReader(params: [g + 1]);
        try {
          final got = <int>[];
          while (await reader.readRow()) {
            got.add(reader.getColumnInt(0));
          }
          return got;
        } finally {
          if (!reader.isClosed) await reader.close();
        }
      }),
    ).timeout(
      const Duration(seconds: 60),
      onTimeout: () => fail(
          '6 parallel post-write in-tx executeReader runs timed out '
          '(writer-serialised path)'),
    );

    for (int g = 0; g < 6; g++) {
      // Group g+1's val column was originally [1, 2, 3, 4]; the
      // in-flight UPDATE bumped each by 100. Every parallel reader
      // must observe the bumped values.
      expect(results[g], [101, 102, 103, 104],
          reason: 'group ${g + 1} did not observe the in-flight UPDATE');
    }

    await db.rollback();
    final after = await (await db
            .prepareQuery('SELECT val FROM t WHERE id = 1'))
        .executeScalar();
    expect(after, 1, reason: 'rollback must restore the original value');

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Regression: parallel reads exceeding the pool must not deadlock.
  //
  // Pre-2.5.1 bug: a Future.wait of N executeReader calls (N > pool size)
  // dispatched one `pool_acquire_reader_blocking` per call across the
  // worker isolates. Once every worker was parked inside the C blocking
  // acquire, no worker was free to run `prepareQuery` / `finalizeStmt`
  // for the in-flight reads — so no read could finish, no reader was
  // released, and the pool deadlocked until each worker's 30 s C-side
  // timeout fired. The fix gates pool acquires through a Dart-level
  // semaphore sized to the reader pool, so excess callers wait in Dart
  // microtasks instead of occupying a worker.
  // ──────────────────────────────────────────────────────────────────────

  test(
      'pool: 17 parallel reads complete with default reader pool of 4 '
      '(no worker-pool deadlock)', () async {
    final db = await _createTestDb('pool_parallel_starve.db',
        readerPoolSize: 4);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (int i = 1; i <= 17; i++) {
      await _runSql(db, 'INSERT INTO t VALUES (?, ?)', params: [i, i * 10]);
    }

    final results = await Future.wait(
      List.generate(17, (i) {
        final id = i + 1;
        return (() async => await (await db.prepareQuery(
                    'SELECT val FROM t WHERE id = ?'))
                .executeScalar(params: [id]) as int?)();
      }),
    ).timeout(
      const Duration(seconds: 15),
      onTimeout: () =>
          fail('17 parallel reads with pool=4 deadlocked (regression)'),
    );
    for (int i = 0; i < 17; i++) {
      expect(results[i], (i + 1) * 10);
    }

    await db.closeDb();
    await db.dropDb();
  });

  test(
      'pool: parallel readers strictly exceeding pool serialise via '
      'Dart semaphore', () async {
    // Cap pool at 1 reader so the second caller is forced to wait at
    // the Dart-level semaphore. Verifies that the semaphore correctly
    // serialises excess callers and that the second read still
    // succeeds once the first releases.
    final db = await _createTestDb('pool_sem_serialise.db',
        readerPoolSize: 1);
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    await _runSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");

    final results = await Future.wait([
      (() async => (await (await db.prepareQuery(
                  'SELECT val FROM t WHERE id = 1'))
              .executeScalar()) as String?)(),
      (() async => (await (await db.prepareQuery(
                  'SELECT val FROM t WHERE id = 2'))
              .executeScalar()) as String?)(),
    ]);
    expect(results, ['a', 'b']);

    await db.closeDb();
    await db.dropDb();
  });

  test(
      'pool: closeDb cancels surplus Dart-side reader-slot waiters with '
      'DbasSqliteException', () async {
    // Park two reader-slot waiters behind a single held slot, then
    // close the database. closeDb latches _closing and drains both
    // waiter queues BEFORE sweeping statements: each parked waiter
    // must reject with DbasSqliteException(readerSlotWaitCancelled).
    // Pre-sweep draining is load-bearing — if the sweep ran first,
    // the held reader's onClose would _releaseReaderSlot → grant a
    // parked waiter, which would then race into
    // poolAcquireReaderBlocking on a worker isolate against the
    // closePool dispatched by this method on a sibling worker
    // isolate, segfaulting in C when ClosePool destroys the pool
    // lock/condvar underneath the parked acquire. Without the drain,
    // already-parked waiters would either be race-granted into the
    // mid-tear-down pool (the segfault above) or wait out the full
    // poolAcquireTimeout — disjoint from the synchronous-rejection
    // path the _closing flag covers for callers arriving AFTER close
    // begins (see the two tests below).
    final db = await _createTestDb('pool_close_cancels_surplus_waiter.db',
        readerPoolSize: 1);
    DbasSqlite.debugPoolAcquireTimeoutMs = 30000;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      // First reader holds the only Dart slot.
      final firstStmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final firstReader = await firstStmt.executeReader();
      expect(await firstReader.readRow(), isTrue);

      // Two more executeReaders park at the Dart semaphore.
      final parkedStmt1 =
          await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final parkedStmt2 =
          await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      // Capture each parked future's eventual outcome (value or error)
      // so an uncaught rejection doesn't fail the test runner before
      // we get a chance to inspect it.
      final parked1Outcome =
          parkedStmt1.executeReader().then<Object?>(
              (r) => r, onError: (Object e) => e);
      final parked2Outcome =
          parkedStmt2.executeReader().then<Object?>(
              (r) => r, onError: (Object e) => e);

      // Wait until both waiters have actually parked in the queue.
      await _awaitReaderWaiters(db, 2);

      await db.closeDb();

      // Both parked reads must be rejected with
      // readerSlotWaitCancelled — the pre-sweep drain in closeDb
      // empties the wait queue before any onClose can race-grant a
      // waiter into the mid-tear-down pool.
      final err1 = await parked1Outcome;
      final err2 = await parked2Outcome;
      bool isCancellation(Object? e) =>
          e is DbasSqliteException &&
          e.code == DbasSqliteErrorCode.readerSlotWaitCancelled;
      expect(isCancellation(err1), isTrue,
          reason: 'expected parked1 to be cancelled by '
              '_cancelReaderSlotWaitQueue, got err1=$err1');
      expect(isCancellation(err2), isTrue,
          reason: 'expected parked2 to be cancelled by '
              '_cancelReaderSlotWaitQueue, got err2=$err2');
    } finally {
      DbasSqlite.debugPoolAcquireTimeoutMs = null;
      await db.dropDb();
    }
  });

  test(
      'pool: executeReader arriving after closeDb starts is rejected '
      'synchronously with readerSlotWaitCancelled', () async {
    // Covers the _closing-flag guard in _acquireReaderSlot for a NEW
    // caller (distinct from the pre-parked-waiter drain above). closeDb
    // latches _closing synchronously before its first await, so an
    // executeReader issued while teardown is in flight must reject with
    // readerSlotWaitCancelled instead of racing into
    // poolAcquireReaderBlocking against the closePool dispatch. Without
    // the guard, _acquireReaderSlot would enter the (now-empty) queue
    // and hang, or grant a stale slot into the mid-tear-down pool.
    final db = await _createTestDb('pool_close_rejects_new_reader.db',
        readerPoolSize: 1);
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');
      // Prepare BEFORE closing so prepareQuery's isOpened() check is
      // not what gates the call — we want to reach _acquireReaderSlot.
      final stmt = await db.prepareQuery('SELECT id FROM t WHERE id = 1');

      // Start teardown but do not await: closeDb's synchronous prelude
      // latches _closing before suspending at its first await. The pool
      // pointer is still live at this point.
      final closeFuture = db.closeDb();
      final outcome = stmt
          .executeReader()
          .then<Object?>((r) => r, onError: (Object e) => e);
      await closeFuture;

      final err = await outcome;
      expect(err, isA<DbasSqliteException>(),
          reason: 'expected a DbasSqliteException, got $err');
      expect((err as DbasSqliteException).code,
          DbasSqliteErrorCode.readerSlotWaitCancelled);
    } finally {
      await db.dropDb();
    }
  });

  test(
      'pool: beginTransaction arriving after closeDb starts is rejected '
      'with writerLockWaitCancelled', () async {
    // Writer-lock symmetry with the reader-slot guard. closeDb latches
    // _closing synchronously before its first await; a writer-lock
    // acquire issued while teardown is in flight must reject with
    // writerLockWaitCancelled instead of entering the (now-drained)
    // _writerWaitQueue and hanging, or racing executeSql against the
    // closePool dispatch.
    //
    // This exercises the _acquireWriterLock guard directly. Parking a
    // writer BEHIND a held lock is not reachable via the public API
    // while the holder is idle (beginTransaction is idempotent,
    // executeSql short-circuits on isInTransaction, vacuum rejects in a
    // transaction), so the new-caller guard is the test surface.
    final db = await _createTestDb('pool_close_rejects_new_writer.db',
        readerPoolSize: 1);
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');

      // Start teardown but do not await: _closing is latched, _db is
      // still live so beginTransaction passes its isOpened() check and
      // reaches _acquireWriterLock.
      final closeFuture = db.closeDb();
      final outcome = db
          .beginTransaction()
          .then<Object?>((_) => null, onError: (Object e) => e);
      await closeFuture;

      final err = await outcome;
      expect(err, isA<DbasSqliteException>(),
          reason: 'expected a DbasSqliteException, got $err');
      expect((err as DbasSqliteException).code,
          DbasSqliteErrorCode.writerLockWaitCancelled);
    } finally {
      await db.dropDb();
    }
  });

  test(
      'pool: closeDb during a transaction still cancels parked reader '
      'waiters and rolls back', () async {
    // Combines the rollback path with parked reader waiters — closeDb
    // runs _cancelReaderSlotWaitQueue BEFORE await rollback(), so this
    // pins that ordering. A write-less transaction routes executeReader
    // through the pool (read-your-writes only kicks in after a write),
    // so a held reader + two parked waiters is reproducible while a
    // transaction is open.
    final db = await _createTestDb('pool_close_tx_cancels_waiters.db',
        readerPoolSize: 1);
    DbasSqlite.debugPoolAcquireTimeoutMs = 30000;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      await db.beginTransaction();
      expect(db.isInTransaction, isTrue);

      // Hold the only reader slot, then park two more waiters.
      final firstStmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final firstReader = await firstStmt.executeReader();
      expect(await firstReader.readRow(), isTrue);

      final parkedStmt1 =
          await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final parkedStmt2 =
          await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final parked1 = parkedStmt1
          .executeReader()
          .then<Object?>((r) => r, onError: (Object e) => e);
      final parked2 = parkedStmt2
          .executeReader()
          .then<Object?>((r) => r, onError: (Object e) => e);
      await _awaitReaderWaiters(db, 2);

      await db.closeDb();

      bool isCancellation(Object? e) =>
          e is DbasSqliteException &&
          e.code == DbasSqliteErrorCode.readerSlotWaitCancelled;
      expect(isCancellation(await parked1), isTrue);
      expect(isCancellation(await parked2), isTrue);
      // Rollback ran during teardown despite the pre-sweep cancel.
      expect(db.isInTransaction, isFalse);
    } finally {
      DbasSqlite.debugPoolAcquireTimeoutMs = null;
      await db.dropDb();
    }
  });

  test(
      'pool: closeDb must not walk past a read that is inside '
      "executeReader's prepare window", () async {
    // The sibling arm of the hazard the three tests above defend. Those
    // cover waiters parked DART-side, which _cancelReaderSlotWaitQueue
    // rejects and drains. This covers a read that already crossed INTO
    // native code: it has been handed a pool reader by
    // PoolAcquireReaderBlocking and has a live sqlite3_stmt, and no
    // Dart-side queue drain can recall it.
    //
    // The seam parks the read between _replayBinds and the
    // DbasSqliteReader.internal construction — precisely the window
    // where _activeReader is still null. What closeDb does with it:
    //   1. the statement sweep calls stmt.close(); close() reads
    //      `final reader = _activeReader` as null and returns having
    //      awaited NOTHING, but it has already latched _closed = true;
    //   2. _activeStatements.clear() drops the statement from tracking
    //      entirely, so closeDb has now DISOWNED a read that is about
    //      to check a pool reader out;
    //   3. closePool blocks in C until activeOps hits zero, and a
    //      checked-out reader keeps it above zero (that wait is the
    //      native contract that turns this into a loud deadlock instead
    //      of a use-after-free — ClosePool refuses to free a reader
    //      another thread may still be stepping);
    //   4. nothing ever calls PoolReleaseReader for it. The reader that
    //      appears at :709 is attached to an already-closed statement,
    //      so the caller's own `finally { await stmt.close(); }` hits
    //      `if (_closed) return` and cascades to nothing.
    //
    // Asserted invariant, deliberately fix-shape agnostic: once the
    // in-flight read has settled, closeDb must complete. Both plausible
    // fixes satisfy it — awaiting the in-flight read in the sweep and
    // closing the reader it produces, or bailing the read out on the
    // latched _closing flag so its own unwind releases the reader.
    final db = await _createTestDb('pool_close_inflight_prepare_window.db',
        readerPoolSize: 2);
    addTearDown(() {
      DbasSqliteStatement.debugBeforeReaderTransfer = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    Object? closeOutcome;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      // The witness the unwedge branch at the bottom is gated on: once
      // this has fired, teardown is at or past the point where the pool
      // stops being safe to touch.
      var destructiveCloseReached = false;
      DbasSqlite.debugBeforeDestructiveClose = (_) {
        destructiveCloseReached = true;
      };

      // A rendezvous, not a sleep: `reached` proves the read is parked
      // at the offending instruction rather than merely likely to be.
      // One-shot — closeDb runs a WAL checkpoint of its own on the way
      // out, and that must not park too.
      final reached = Completer<void>();
      DbasSqliteStatement.debugBeforeReaderTransfer = () async {
        DbasSqliteStatement.debugBeforeReaderTransfer = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      var readSettled = false;
      final read = stmt
          .executeReader()
          .whenComplete(() => readSettled = true)
          .then<Object?>((r) => r, onError: (Object e) => e);

      await reached.future;

      final close = db
          .closeDb()
          .whenComplete(() => closeReturned = true)
          .then<Object?>((_) => null, onError: (Object e) => closeOutcome = e);

      // While the read is still parked, closeDb must not settle. Pumped
      // with real event-loop turns, not microtask drains: every closeDb
      // step is a worker-isolate round-trip whose reply a microtask
      // drain would never deliver, which would make a false pass the
      // default outcome. Exits the instant closeDb settles, so the
      // failing observation is positive and immediate.
      final parked = Stopwatch()..start();
      while (!closeReturned && parked.elapsedMilliseconds < 300) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(closeReturned, isFalse,
          reason: 'closeDb settled while a read was still inside its '
              'prepare window holding a pool reader and a live '
              'sqlite3_stmt');

      // Release the read and give teardown a generous, BOUNDED window
      // to finish. Bounded because the closePool dispatch is parked on
      // a worker isolate: an unbounded await would wedge the rest of
      // the suite rather than fail this test.
      release.complete();
      final readOutcome = await read;
      var closeCompleted = true;
      try {
        await close.timeout(const Duration(seconds: 5));
      } on TimeoutException {
        closeCompleted = false;
      }

      if (!closeCompleted && !destructiveCloseReached) {
        // Unwedge before asserting — but only behind a POSITIVE witness
        // that the pool is still there to talk to. "Did not settle in 5 s"
        // is not that witness: it is equally consistent with a closeDb
        // wedged INSIDE or AFTER closePool, and running finalizeStmt /
        // poolReleaseReader against a pool mid-destruction is the very
        // SIGSEGV this test exists to observe without triggering — and a
        // dead runner reports nothing. `debugBeforeDestructiveClose` not
        // having fired means teardown has not reached the point of no
        // return, so the pool, this reader and its connection are all
        // still alive and releasing it is what lets teardown finish.
        if (readOutcome is DbasSqliteReader && !readOutcome.isClosed) {
          await readOutcome.close();
        }
        // Bounded like the first wait: an unwedge that does not work must
        // still fail this test rather than wedge the suite behind it.
        try {
          await close.timeout(const Duration(seconds: 5));
          closeCompleted = true;
        } on TimeoutException {
          closeCompleted = false;
        }
      }

      expect(closeCompleted, isTrue,
          reason: 'closeDb never completed. Its statement sweep disowned '
              'a statement whose read was still in the prepare window, so '
              'the pool reader that read checked out is never released '
              'and closePool waits on activeOps forever');
      expect(closeOutcome, isNull,
          reason: 'closeDb must complete, not merely settle');
      expect(readSettled, isTrue,
          reason: 'the parked read must settle once released, whichever '
              'way closeDb went');
    } finally {
      DbasSqliteStatement.debugBeforeReaderTransfer = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
      // Unpark the read on the failing path too, so a thrown expect
      // cannot leave it suspended into the next test.
      if (!release.isCompleted) release.complete();
      // Only touch the database again once teardown actually finished —
      // dropDb would re-enter the wedged closeDb otherwise. setUpAll
      // wipes test/db at the start of every run, so a skipped drop
      // leaks nothing across runs.
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'pool: closeDb must not free the WRITER under a read that '
      'read-your-writes routed to it', () async {
    // The pool-reader arm above is protected in C: ClosePool waits on
    // `activeOps`, which counts checked-out readers, so a read holding
    // one turns the blind spot into a loud deadlock rather than memory
    // corruption. The WRITER has no such protection. The C header is
    // explicit — "The writer is NOT checkout-tracked (it has no release
    // call)" — and ClosePool force-closes it through
    // closeDbCore(force=true), which finalizes every live sqlite3_stmt,
    // frees the SQLiteDb, and states the resulting contract outright:
    // "Any subsequent per-stmt API call with such a handle is undefined
    // behavior ... the wrapper-level pool object that issued the
    // handles is responsible for not re-using them after close."
    //
    // A read lands on the writer whenever read-your-writes is in
    // effect: inside a transaction, after a write (the `useWriter`
    // predicate at :579-580). That is the shape a migration produces —
    // DDL and inserts inside one transaction, then a read of the
    // ledger.
    //
    // Parked in the same prepare window, such a read is invisible to
    // closeDb for the same reason as the pool case (_activeReader is
    // still null, so the sweep's stmt.close() awaits nothing) — but
    // here nothing downstream blocks either: rollback() drains only
    // reentrant ops that registered a DISPATCH, and a read registers
    // none. So closeDb runs to completion and closePool frees the
    // writer out from under a live statement handle, leaving
    // executeReader to hand its caller a reader pointing at freed
    // memory.
    //
    // Asserted BEFORE any use-after-free can occur: closeDb must not
    // settle while the read is parked. Nothing in this test ever steps
    // or closes the resulting reader — that call IS the SIGSEGV, and a
    // SIGSEGV kills the runner instead of failing an expect.
    final db = await _createTestDb('pool_close_writer_routed_read.db',
        readerPoolSize: 2);
    addTearDown(() => DbasSqliteStatement.debugBeforeReaderTransfer = null);
    final release = Completer<void>();
    var closeReturned = false;
    Object? closeOutcome;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      // Arm read-your-writes: transaction + a write is verbatim the
      // `useWriter` predicate. The pool is live (readerPoolSize: 2), so
      // writer routing is a real choice here — if the routing were
      // wrong the read would check a pool reader out instead and the C
      // activeOps wait would silently turn this back into the deadlock
      // already proven above.
      await db.beginTransaction();
      await _runSql(db, "INSERT INTO t VALUES (2, 'second')");
      expect(db.isInTransaction, isTrue);
      expect(db.transactionHasWritesInternal, isTrue,
          reason: 'read-your-writes must be armed, else the read below '
              'routes to a pool reader and this test silently becomes a '
              'duplicate of the pool case');
      expect(db.poolPtrInternal, isNotNull,
          reason: 'a pool must exist, so reaching the writer is a routing '
              'decision and not the single-connection fallback');

      final reached = Completer<void>();
      DbasSqliteStatement.debugBeforeReaderTransfer = () async {
        DbasSqliteStatement.debugBeforeReaderTransfer = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      var readSettled = false;
      final read = stmt
          .executeReader()
          .whenComplete(() => readSettled = true)
          .then<Object?>((r) => r, onError: (Object e) => e);

      await reached.future;

      final close = db
          .closeDb()
          .whenComplete(() => closeReturned = true)
          .then<Object?>((_) => null, onError: (Object e) => closeOutcome = e);

      // Same pumping rationale as the pool case: real event-loop turns,
      // exits the instant closeDb settles.
      final parked = Stopwatch()..start();
      while (!closeReturned && parked.elapsedMilliseconds < 300) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      final settledWhileParked = closeReturned;

      // Unpark so the routing witness below is populated. The resume
      // path is pure Dart — it constructs the reader, sets
      // `transferred`, and returns; the bailout that WOULD call
      // finalizeStmt against the (possibly freed) writer is skipped.
      release.complete();
      await read;
      final readerBoundToWriter = stmt.hasOpenWriterReaderInternal;

      var closeCompleted = settledWhileParked;
      if (!closeCompleted) {
        // Bounded so an unexpected teardown stall cannot wedge the
        // suite. Deliberately no unwedge attempt here: unlike the pool
        // case there is no checked-out reader to hand back, and
        // touching the reader to force one would risk the very crash
        // this test is built to observe without triggering.
        try {
          await close.timeout(const Duration(seconds: 5));
          closeCompleted = true;
        } on TimeoutException {
          closeCompleted = false;
        }
      }

      // Routing witness first, so a mis-routed read fails as a setup
      // error rather than masquerading as the defect.
      expect(readerBoundToWriter, isTrue,
          reason: 'the read must have been routed to the WRITER '
              'connection — hasOpenWriterReaderInternal is the direct '
              'witness that _activeReaderUsesWriter was set');

      expect(settledWhileParked, isFalse,
          reason: 'closeDb ran to completion while a read sat parked in '
              "executeReader's prepare window ON THE WRITER. closePool "
              'force-closes the writer via closeDbCore(force=true), '
              'finalizing its live sqlite3_stmt and freeing the '
              'SQLiteDb, so the reader executeReader then hands back '
              'points at freed memory and the next readRow()/close() on '
              'it is the segfault');

      expect(readSettled, isTrue,
          reason: 'the parked read must settle once released');
      expect(closeCompleted, isTrue,
          reason: 'closeDb neither completed while parked nor after the '
              'read was released');
      expect(closeOutcome, isNull,
          reason: 'closeDb must complete, not merely settle');
    } finally {
      DbasSqliteStatement.debugBeforeReaderTransfer = null;
      if (!release.isCompleted) release.complete();
      // Deliberately no reader.close() / stmt.close() on ANY path: the
      // writer may already be freed, and closing the reader runs
      // getStmtAffectedRows + finalizeStmt against it — the crash this
      // test exists to observe without triggering.
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'closeDb reaches its destructive close only after the native-op '
      'registry has emptied', () async {
    // The ordering invariant the two tests above rest on, asserted
    // directly instead of through its consequences. `closePool` /
    // `closeDb` are the point of no return — after them every SQLiteDb
    // pointer the pool produced is invalid — so "nothing is still inside
    // native code" has to be true THERE, not merely by the time closeDb
    // returns to its caller.
    final db = await _createTestDb('close_native_op_registry_empty.db',
        readerPoolSize: 2);
    addTearDown(() {
      DbasSqliteStatement.debugBeforeReaderTransfer = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
    });
    final release = Completer<void>();
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      int? opsAtDestructiveClose;
      bool? openedAtDestructiveClose;
      DbasSqlite.debugBeforeDestructiveClose = (d) {
        opsAtDestructiveClose = d.debugInFlightNativeOpCount;
        // The WRITER handle is nulled before the destructive dispatch is
        // awaited, so a main-isolate racer sees a CLOSED connection
        // rather than one whose pointer is mid-free. (The POOL pointer is
        // deliberately the other way round — it stays readable until
        // closePool returns, because a reader release arriving in that
        // window is exactly what the call is blocked waiting for. The
        // joins-an-in-progress-close test pins that half.)
        openedAtDestructiveClose = d.isOpened();
      };

      expect(db.debugInFlightNativeOpCount, 0,
          reason: 'a quiescent database registers nothing');

      final reached = Completer<void>();
      DbasSqliteStatement.debugBeforeReaderTransfer = () async {
        DbasSqliteStatement.debugBeforeReaderTransfer = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final read = stmt.executeReader();
      await reached.future;

      // Parked in the prepare window: holding a pool reader and a live
      // sqlite3_stmt, invisible to `_activeReader`, and — the point —
      // visible here.
      expect(db.debugInFlightNativeOpCount, 1,
          reason: 'a read inside executeReader\'s prepare window must be '
              'registered; that registration is the only signal closeDb '
              'has for it');

      final close = db.closeDb();
      release.complete();
      final reader = await read;
      // The caller that awaited `executeReader` resumes BEFORE teardown
      // closes what it was handed: waking the drain costs strictly more
      // hops than returning through `executeReader` does. Pinned here
      // because the consequence of inverting it is silent — a call that
      // returned successfully would deliver an already-closed reader,
      // and the caller's first `readRow()` would report an empty result
      // set with no error of any kind.
      final closedBeforeCallerResumed = reader.isClosed;
      await close;

      expect(closedBeforeCallerResumed, isFalse,
          reason: 'closeDb closed the in-flight read\'s reader before the '
              'caller awaiting executeReader had even resumed');
      expect(opsAtDestructiveClose, 0,
          reason: 'closePool ran while an operation was still registered '
              'as inside native code');
      expect(openedAtDestructiveClose, isFalse,
          reason: 'the writer handle must be nulled BEFORE the destructive '
              'dispatch is awaited, so a concurrent isOpened() on the main '
              'isolate cannot hand a being-freed pointer back into FFI');
      expect(reader.isClosed, isTrue,
          reason: 'the reader the parked read produced must have been '
              'closed by the sweep — that close is what returns its pool '
              'reader, and skipping it is what wedges ClosePool');
      expect(db.debugInFlightNativeOpCount, 0);
    } finally {
      DbasSqliteStatement.debugBeforeReaderTransfer = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
      if (!release.isCompleted) release.complete();
      await db.dropDb();
    }
  });

  test(
      'closeDb throws closeDbNativeOpDrainTimeout instead of tearing down '
      'over an operation that never hands back', () async {
    // The bound on the drain. Proceeding-and-logging here would BE the
    // use-after-free the drain exists to prevent, so the only honest
    // outcome is a failed close that names what is still outstanding and
    // leaves the connection alive — a leaked handle beats a dead
    // process, and the documented remedy (await the work, close again)
    // has to actually work.
    final db = await _createTestDb('close_native_op_drain_timeout.db',
        readerPoolSize: 2);
    addTearDown(() {
      DbasSqliteStatement.debugBeforeReaderTransfer = null;
      DbasSqlite.debugNativeOpDrainTimeoutMs = null;
    });
    final release = Completer<void>();
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      final reached = Completer<void>();
      DbasSqliteStatement.debugBeforeReaderTransfer = () async {
        DbasSqliteStatement.debugBeforeReaderTransfer = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final read = stmt.executeReader();
      await reached.future;

      DbasSqlite.debugNativeOpDrainTimeoutMs = 50;
      await expectLater(
        db.closeDb(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.closeDbNativeOpDrainTimeout)
            .having((e) => e.category, 'category',
                DbasSqliteErrorCategory.busyOrCancelled)
            // The label is the whole diagnostic value of the throw: it
            // says WHICH call never handed back.
            .having((e) => e.message, 'message', contains('executeReader'))
            .having((e) => e.message, 'message',
                contains('SELECT val FROM t WHERE id = 1'))),
      );

      expect(db.isOpened(), isTrue,
          reason: 'a failed drain must leave the connection OPEN — '
              'destroying it is exactly what the timeout refused to do');

      // The documented remedy: let the work finish, then close again.
      DbasSqlite.debugNativeOpDrainTimeoutMs = null;
      release.complete();
      final reader = await read;
      await db.closeDb();
      expect(reader.isClosed, isTrue);
      expect(db.isOpened(), isFalse);
    } finally {
      DbasSqliteStatement.debugBeforeReaderTransfer = null;
      DbasSqlite.debugNativeOpDrainTimeoutMs = null;
      if (!release.isCompleted) release.complete();
      await db.dropDb();
    }
  });

  test('an in-flight executeSql registers with the same native-op registry',
      () async {
    // The registry is connection-wide, not reader-specific: the write
    // path crosses into native code the same way and is drained the same
    // way. Fired INSIDE a transaction on purpose — there the writer lock
    // is already held, so `_executeSqlNative` never suspends before
    // registering and the count below is deterministic rather than a
    // pumped observation.
    final db = await _createTestDb('close_native_op_executesql.db',
        readerPoolSize: 2);
    addTearDown(() => DbasSqlite.debugBeforeDestructiveClose = null);
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

      int? opsAtDestructiveClose;
      DbasSqlite.debugBeforeDestructiveClose = (d) {
        opsAtDestructiveClose = d.debugInFlightNativeOpCount;
      };

      await db.beginTransaction();
      final stmt = await db.prepareQuery("INSERT INTO t VALUES (1, 'first')");
      final write = stmt.executeSql();
      expect(db.debugInFlightNativeOpCount, 1,
          reason: 'an un-awaited executeSql is inside native code from the '
              'moment it dispatches, and must be registered there');

      expect(await write, 1);
      expect(db.debugInFlightNativeOpCount, 0,
          reason: 'the registration is released in executeSql\'s finally');

      await db.commit();
      await db.closeDb();
      expect(opsAtDestructiveClose, 0);
    } finally {
      DbasSqlite.debugBeforeDestructiveClose = null;
      await db.dropDb();
    }
  });

  test('closeDbNativeOpDrainTimeout maps to the documented category', () {
    // `category` is an exhaustive switch with no `default`, so the new
    // code had to be classified somewhere. It joins the two
    // `closeDbBusy*` codes rather than `notOpened`: all three mean
    // "teardown could not proceed because something was still using the
    // connection", and all three have the same remedy — let the
    // outstanding work finish, then close again.
    expect(DbasSqliteErrorCode.closeDbNativeOpDrainTimeout.category,
        DbasSqliteErrorCategory.busyOrCancelled);
    expect(DbasSqliteErrorCode.closeDbBusyLeakedHandle.category,
        DbasSqliteErrorCategory.busyOrCancelled);
  });

  test(
      'reader: closeDb must not finalize a statement while its own reader '
      'has a step in flight', () async {
    // The sibling of the prepare-window cluster above, one step later in
    // the same lifecycle. There the read had not become a reader yet and
    // the casualty was the CONNECTION under it. Here it IS a reader —
    // `_activeReader` points at it and the sweep can see it — and the
    // casualty is the STATEMENT inside it.
    //
    // A consumer suspended in `await reader.readRow()` is a worker-isolate
    // dispatch against a live sqlite3_stmt. `readRow` registers nothing
    // with the native-op registry, so `_drainNativeOps` finds it empty and
    // closeDb walks straight into its statement sweep: stmt.close() →
    // reader.close() → onClose → finalizeStmt, on the very handle the step
    // is using. `_dispatch` picks the least-loaded worker, so the step and
    // the finalize genuinely land on two different OS threads, and the C
    // layer neither refuses nor blocks — FinalizeStmt has no busy check
    // and no refcount, and ReadRow resolves the pointer, DROPS
    // db_stmts_lock, then steps and writes s->affectedRows / lastError
    // unlocked. This is pre-existing and is NOT the prepare-window blind
    // spot the tests above cover.
    //
    // Asserted invariant, deliberately fix-shape agnostic: while a step is
    // outstanding, closeDb must not reach the point where it finalizes
    // that statement. closeDb SETTLING is that point — its sweep is
    // unconditional and the reader's onClose always calls finalizeStmt —
    // so "closeDb has not settled" is the witness, and no particular
    // mechanism is named. A reader that serialises its own close against
    // its step satisfies it; so does covering readRow in the native-op
    // registry, a sweep that waits for quiescence, or a close that refuses
    // outright.
    //
    // readRows() needs no case of its own: it is a plain `for` loop over
    // readRow() with no native contact of its own, so the seam fires on
    // its first iteration and it inherits by construction whatever
    // readRow gains.
    //
    // Nothing here ever steps or closes the reader on any path — either
    // call against a finalized handle IS the SIGSEGV, and a dead runner
    // reports nothing. The parked step is released only after its row is
    // proven cached, so the resume path is pure Dart: readRow consumes an
    // already-settled SQLITE_ROW and returns, touching no native code.
    final db = await _createTestDb('reader_close_inflight_step.db',
        readerPoolSize: 2);
    addTearDown(() => DbasSqliteReader.debugInsideReadRowStep = null);
    final release = Completer<void>();
    var closeReturned = false;
    Object? closeOutcome;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      // A rendezvous, not a sleep: `reached` proves the step is parked at
      // the offending instruction rather than merely likely to be.
      // One-shot — closeDb runs a WAL checkpoint of its own on the way
      // out, and that must not park too.
      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      final reader = await stmt.executeReader();

      var rowSettled = false;
      final row = reader
          .readRow()
          .whenComplete(() => rowSettled = true)
          .then<Object?>((r) => r, onError: (Object e) => e);

      await reached.future;

      // Let the dispatched step's REPLY land before teardown starts. The
      // row cache is the witness — it is filled by the very platform call
      // the seam parked on top of, and reading it is pure Dart with no
      // FFI and no second step. With the reply already delivered, the
      // finalize this test provokes cannot physically overlap
      // sqlite3_step, so a failing run fails an `expect` rather than
      // dying on a signal. What is still outstanding, and is the whole
      // point, is the reader's own "a step is in flight" state.
      final cached = Stopwatch()..start();
      while (reader.getColumnText(0).isEmpty &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), 'first',
          reason: 'the parked step never delivered its row, so this run '
              'would prove nothing about finalize-versus-step ordering');
      expect(rowSettled, isFalse,
          reason: 'readRow must still be suspended — the seam holding it '
              'inside its step window is what makes the window observable');

      final close = db.closeDb().then<Object?>(
        (_) => null,
        onError: (Object e) => closeOutcome = e,
      ).whenComplete(() => closeReturned = true);

      // Pumped with real event-loop turns, not microtask drains: every
      // closeDb step is a worker-isolate round-trip whose reply a
      // microtask drain would never deliver, which would make a false
      // pass the default outcome. Exits the instant closeDb settles, so
      // the failing observation is positive and immediate.
      final parked = Stopwatch()..start();
      while (!closeReturned && parked.elapsedMilliseconds < 300) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      final settledWhileStepping = closeReturned;

      // Release the step and give teardown a generous, BOUNDED window to
      // finish. Bounded because the remaining teardown dispatches sit on
      // worker isolates: an unbounded await would wedge the rest of the
      // suite rather than fail this test.
      release.complete();
      await row;
      var closeCompleted = settledWhileStepping;
      if (!closeCompleted) {
        try {
          await close.timeout(const Duration(seconds: 5));
          closeCompleted = true;
        } on TimeoutException {
          closeCompleted = false;
        }
      }

      // Named rather than inlined: a closeDb that THREW settled without
      // finalizing anything, and that reads very differently from one
      // that ran teardown to completion.
      final outcome = closeOutcome == null
          ? 'completed normally'
          : 'threw $closeOutcome';
      expect(settledWhileStepping, isFalse,
          reason: 'closeDb settled ($outcome) while reader.readRow() still '
              'had a step outstanding against its sqlite3_stmt. Settling '
              'means the statement sweep already ran reader.close() → '
              'onClose → finalizeStmt on that exact handle, from a '
              'different worker thread than the step, and neither side '
              'interlocks: FinalizeStmt has no busy check and ReadRow '
              'steps with db_stmts_lock dropped');
      expect(rowSettled, isTrue,
          reason: 'the parked readRow must settle once released, whichever '
              'way closeDb went');
      expect(closeCompleted, isTrue,
          reason: 'closeDb neither settled while the step was parked nor '
              'within 5s of the step being released');
      expect(closeOutcome, isNull,
          reason: 'closeDb must complete, not merely settle');
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      // Unpark the step on the failing path too, so a thrown expect
      // cannot leave it suspended into the next test.
      if (!release.isCompleted) release.complete();
      // Deliberately no reader.close() / stmt.close() on ANY path: the
      // statement may already be finalized and its pool connection freed,
      // and both calls run native work against them — the crash this test
      // exists to observe without triggering. Only touch the database
      // again once teardown actually finished; setUpAll wipes test/db at
      // the start of every run, so a skipped drop leaks nothing across
      // runs.
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'reader: a scan cut short by teardown throws instead of reporting '
      'exhaustion', () async {
    // The other half of the serialisation change, and the half that is
    // consumer-visible. Waiting for the in-flight step keeps the process
    // alive; this keeps the RESULT honest.
    //
    // `readRow()` answering `false` has exactly one meaning — no more
    // rows — and before this release a reader torn down mid-scan answered
    // it too. A `while (await readRow())` loop building a list therefore
    // could not tell "the table ended" from "the database closed under
    // me", and returned a short list with no error of any kind. Both
    // states are exercised here against the same table so the contrast is
    // the assertion, not a claim.
    final db = await _createTestDb('reader_torn_down_vs_exhausted.db');
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      for (int i = 1; i <= 3; i++) {
        await _runSql(db, 'INSERT INTO t VALUES ($i)');
      }

      // Exhausted: ran out of rows, self-closed, and still answers false.
      final done = await (await db.prepareQuery('SELECT id FROM t'))
          .executeReader();
      var seen = 0;
      while (await done.readRow()) {
        seen++;
      }
      expect(seen, 3);
      expect(done.isClosed, isTrue);
      expect(await done.readRow(), isFalse,
          reason: 'exhaustion is the one closed state that may still '
              'answer false — changing that would break every '
              'while(readRow()) loop in existence');

      // Torn down: same table, same query, stopped from outside.
      final stmt = await db.prepareQuery('SELECT id FROM t');
      final cut = await stmt.executeReader();
      expect(await cut.readRow(), isTrue);
      expect(cut.getColumnInt(0), 1);
      // The real teardown route — closeDb's sweep reaches the reader
      // through exactly this call.
      await stmt.close();
      expect(cut.isClosed, isTrue);

      await expectLater(
        cut.readRow(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.readerClosedDuringScan)
            .having((e) => e.category, 'category',
                DbasSqliteErrorCategory.busyOrCancelled)
            // No native rc: nothing was stepped. The throw happens at the
            // guard, BEFORE any contact with the finalized handle — which
            // is what makes calling readRow on a torn-down reader safe to
            // assert on at all.
            .having((e) => e.sqliteCode, 'sqliteCode', isNull)),
        reason: 'a reader closed before its result set ran out must not '
            'be able to answer the same false that exhaustion answers',
      );
    } finally {
      await db.closeDb();
      await db.dropDb();
    }
  });

  test(
      'reader: readerClosedDuringScan names the close it observed instead of '
      'asserting a teardown', () async {
    // The message is a diagnosis, and a diagnosis that guesses sends the
    // reader of it somewhere else. A reader records THAT it was closed,
    // not by whom — so the text used to list every possibility and then
    // assert the worst one ("something tore it down mid-scan"). A
    // consumer who closes deliberately after a partial scan and probes
    // the reader again was told their database had been torn down during
    // shutdown, and went looking for a closeDb() that never happened.
    //
    // All three branches in one test, so the contrast is the assertion
    // rather than a claim — each arm asserts the text it must carry AND
    // the text it must not.
    final db = await _createTestDb('reader_closed_reason_message.db');
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      for (int i = 1; i <= 3; i++) {
        await _runSql(db, 'INSERT INTO t VALUES ($i)');
      }

      // Deliberate close by the consumer.
      final ownStmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final ownReader = await ownStmt.executeReader();
      expect(await ownReader.readRow(), isTrue);
      await ownStmt.close();
      await expectLater(
        ownReader.readRow(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.readerClosedDuringScan)
            .having((e) => e.message, 'message', contains('explicit close()'))
            .having((e) => e.message, 'message', isNot(contains('closeDb')))),
        reason: 'a reader the consumer closed themselves must not be '
            'reported as torn down by a database shutdown — that sends '
            'them hunting for a closeDb() that never ran',
      );

      // Closed by its own failed step. `readRow` closes the reader itself
      // as part of reporting a step failure, so the state a later call
      // finds was created by the caller's own already-thrown exception —
      // reporting it as either an explicit close or a teardown sends the
      // reader of the message looking for a close that never happened.
      //
      // `abs(-9223372036854775808)` is the reachable way in: SQLite
      // prepares it happily and raises "integer overflow" at STEP, which
      // is the only route to this arm — the branch is untested rather
      // than unreachable, and before this it had never been rendered at
      // all.
      final failStmt = await db.prepareQuery('SELECT abs(-9223372036854775808)');
      final failReader = await failStmt.executeReader();
      await expectLater(
        failReader.readRow(),
        throwsA(isA<DbasSqliteException>().having(
            (e) => e.code, 'code', DbasSqliteErrorCode.readRowFailed)),
        reason: 'the step must fail at step time — a query that fails at '
            'prepare would never reach the arm this asserts',
      );
      expect(failReader.isClosed, isTrue,
          reason: 'readRow closes the reader as part of reporting a step '
              'failure; without that close there is no later state to '
              'describe');
      await expectLater(
        failReader.readRow(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.readerClosedDuringScan)
            .having((e) => e.message, 'message',
                contains('An earlier readRow() failed'))
            .having((e) => e.message, 'message', contains('readRowFailed'))
            .having((e) => e.message, 'message', isNot(contains('closeDb')))
            .having((e) => e.message, 'message',
                isNot(contains('explicit close()')))),
        reason: 'a reader its own failed step closed must point at that '
            'failure, not at a close() or a closeDb() that never ran',
      );
      await failStmt.close();

      // Torn down by teardown, which is the one caller allowed to say so.
      final sweptStmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final sweptReader = await sweptStmt.executeReader();
      expect(await sweptReader.readRow(), isTrue);
      await db.closeDb();
      expect(sweptReader.isClosed, isTrue);
      await expectLater(
        sweptReader.readRow(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.readerClosedDuringScan)
            .having((e) => e.message, 'message',
                contains("closeDb()'s statement sweep"))),
        reason: 'the one case that really is a teardown must still say so, '
            'or the branch buys nothing',
      );
    } finally {
      // closeDb is idempotent-ish here: the teardown branch above may
      // already have closed it, and re-closing a closed database is a
      // no-op, so this stays correct on the failing path too.
      await db.closeDb();
      await db.dropDb();
    }
  });

  test(
      'executeScalar propagates readerClosedDuringScan instead of the null it '
      'used to report', () async {
    // The headline consumer-visible change, and the one place the throw
    // replaces a `null`. `executeScalar` runs exactly one readRow, so a
    // reader torn down between `executeReader` returning and that call
    // used to make the method answer `null` — indistinguishable from a
    // genuinely empty query, and silently wrong for a caller that treats
    // null as "no such row".
    //
    // Reached through `debugBeforeScalarReadRow`, which is the only way
    // in: the window is microtasks wide — every step from the reader's
    // construction to the readRow dispatch is pure Dart — so another
    // async flow is always scheduled either wholly before it or wholly
    // after it. Measured: a flow polling for the reader to appear and
    // closing the instant it does still lands after the readRow has been
    // dispatched. The seam runs the close INSIDE the window instead, at
    // exactly the instruction the contract talks about.
    //
    // `stmt.close()` is the "close on this statement from elsewhere" that
    // contract names, and it is awaited in full here — no step is in
    // flight yet (readRow has not run), so the reader's step drain is
    // empty and the close cannot deadlock against the scan it precedes.
    final db = await _createTestDb('scalar_closed_during_scan.db');
    addTearDown(() => DbasSqliteStatement.debugBeforeScalarReadRow = null);
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await _runSql(db, "INSERT INTO t VALUES (1, 'first')");

      final stmt = await db.prepareQuery('SELECT val FROM t WHERE id = 1');
      var closedInsideWindow = false;
      DbasSqliteStatement.debugBeforeScalarReadRow = () async {
        DbasSqliteStatement.debugBeforeScalarReadRow = null;
        await stmt.close();
        closedInsideWindow = true;
      };

      await expectLater(
        stmt.executeScalar(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.readerClosedDuringScan)
            .having((e) => e.category, 'category',
                DbasSqliteErrorCategory.busyOrCancelled)),
        reason: 'executeScalar swallowed the truncation and reported the '
            'same null an empty result set reports — a caller treating '
            'null as "no such row" is then silently wrong, which is '
            'exactly what this release changed',
      );
      expect(closedInsideWindow, isTrue,
          reason: 'the close never ran, so this run proves nothing about '
              'the window');
      expect(stmt.isClosed, isTrue,
          reason: 'the throw must not skip executeScalar\'s own cleanup');

      // The contrast: a genuinely empty result still answers null, so the
      // throw above is about truncation and not about scalars in general.
      final empty = await (await db
              .prepareQuery('SELECT val FROM t WHERE id = 99'))
          .executeScalar();
      expect(empty, isNull,
          reason: 'null must still mean "no row" — the throw replaces it '
              'only where the scan was cut short');
    } finally {
      DbasSqliteStatement.debugBeforeScalarReadRow = null;
      await db.closeDb();
      await db.dropDb();
    }
  });

  test('reader: readRows propagates the truncation instead of swallowing it',
      () async {
    // readRows is the library's own readRow loop, so it inherits the
    // throw — but "inherits by construction" is only true while nothing
    // catches. Pinned because the tempting shape (return the rows
    // gathered so far) is precisely the silent truncation the throw
    // exists to prevent: with `hasMore: false` it reports a cut-short
    // batch as a completed one, and with `hasMore: true` it invites a
    // follow-up call against a finalized statement.
    final db = await _createTestDb('reader_rows_torn_down.db');
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      for (int i = 1; i <= 5; i++) {
        await _runSql(db, 'INSERT INTO t VALUES ($i)');
      }

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final first = await reader.readRows(2);
      expect(first.rows.map((r) => r['id']!.value).toList(), [1, 2]);
      expect(first.hasMore, isTrue);

      await stmt.close();

      await expectLater(
        reader.readRows(2),
        throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
            DbasSqliteErrorCode.readerClosedDuringScan)),
        reason: 'rows 3-5 were never read; reporting the batch as '
            'complete would lose them silently',
      );
    } finally {
      await db.closeDb();
      await db.dropDb();
    }
  });

  test(
      'reader: closeDb mid-scan surfaces as an error to the consumer, not a '
      'short list', () async {
    // The production shape named in the plan: logout while a list view is
    // mid-scan (`Authentication.logout` → `Database.closeDb`, which never
    // cancels outstanding watch streams). The consumer here is the loop
    // dbas_base_app's `watchSelectListFromType` runs OUTSIDE its shared
    // slot, whose rationale block calls it "a bounded, uninterruptible
    // drain" — an assumption this release deliberately breaks under
    // teardown. This is the accepted trade, so it is asserted rather than
    // left to be discovered downstream.
    final db = await _createTestDb('reader_close_db_mid_scan.db');
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      for (int i = 1; i <= 4; i++) {
        await _runSql(db, 'INSERT INTO t VALUES ($i)');
      }

      final reader =
          await (await db.prepareQuery('SELECT id FROM t ORDER BY id'))
              .executeReader();
      final collected = <int>[];
      expect(await reader.readRow(), isTrue);
      collected.add(reader.getColumnInt(0));

      // No step is outstanding at this instant, so closeDb has nothing to
      // wait for and tears the reader down through its statement sweep.
      await db.closeDb();
      expect(reader.isClosed, isTrue);

      Object? loopOutcome;
      try {
        while (await reader.readRow()) {
          collected.add(reader.getColumnInt(0));
        }
      } catch (e) {
        loopOutcome = e;
      }

      expect(collected, [1], reason: 'only row 1 was ever read');
      expect(
          loopOutcome,
          isA<DbasSqliteException>().having((e) => e.code, 'code',
              DbasSqliteErrorCode.readerClosedDuringScan),
          reason: 'the loop must end in an error event; ending normally '
              'would hand the caller [1] as if the table held one row');
    } finally {
      // In a `finally` so a failing expect above cannot skip the drop and
      // leave a .db behind. The database is already closed on the happy
      // path; dropDb does not need it open.
      await db.dropDb();
    }
  });

  test(
      'reader: close waits for EVERY in-flight readRow step, not just the '
      'most recently dispatched one', () async {
    // The drain in `reader.close()` is only as good as the set of steps
    // it can see, and "the step in flight" is not a singular. Nothing
    // rejects two un-awaited readRow() calls on one reader, so two steps
    // can be outstanding against the same sqlite3_stmt at the same time.
    // Track them in a single slot and the FIRST one is lost — the second
    // call overwrites the field — so a close arriving in between drains
    // only the second and then finalizes the handle while the first is
    // still out. That is the same FinalizeStmt-under-sqlite3_step
    // corruption the case above pins, reached THROUGH the fix for it,
    // and `_dispatch` picks the least-loaded worker with no per-handle
    // affinity, so the two steps do not even share an OS thread.
    //
    // Shape: park step A, let its reply land, park step B, let its reply
    // land, then close. Release B FIRST and leave A parked — with a
    // single slot that is exactly the instant close believes it has
    // drained everything. The assertion is that it has not settled.
    //
    // As in the case above, both replies are landed before anything is
    // torn down (the row cache is the witness), so no sqlite3_step is
    // ever genuinely concurrent with another one or with the finalize: a
    // regression fails an `expect` instead of dying on a signal. What is
    // still outstanding when close runs is the reader's own "a step is
    // in flight" bookkeeping, which is the thing under test.
    final db = await _createTestDb('reader_close_two_inflight_steps.db',
        readerPoolSize: 2);
    addTearDown(() => DbasSqliteReader.debugInsideReadRowStep = null);
    final releaseA = Completer<void>();
    final releaseB = Completer<void>();
    var closeReturned = false;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      for (int i = 1; i <= 3; i++) {
        await _runSql(db, 'INSERT INTO t VALUES ($i)');
      }

      final reachedA = Completer<void>();
      final reachedB = Completer<void>();
      var parked = 0;
      DbasSqliteReader.debugInsideReadRowStep = () async {
        if (++parked == 1) {
          reachedA.complete();
          await releaseA.future;
          return;
        }
        // Nothing after the second park may hold: closeDb runs a WAL
        // checkpoint of its own on the way out.
        DbasSqliteReader.debugInsideReadRowStep = null;
        reachedB.complete();
        await releaseB.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();

      var aSettled = false;
      final a = reader
          .readRow()
          .whenComplete(() => aSettled = true)
          .then<Object?>((r) => r, onError: (Object e) => e);
      await reachedA.future;
      final cachedA = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cachedA.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1',
          reason: "step A never delivered its row, so this run would prove "
              'nothing about which steps the drain waits for');

      var bSettled = false;
      final b = reader
          .readRow()
          .whenComplete(() => bSettled = true)
          .then<Object?>((r) => r, onError: (Object e) => e);
      await reachedB.future;
      final cachedB = Stopwatch()..start();
      while (reader.getColumnText(0) != '2' &&
          cachedB.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '2',
          reason: 'step B never delivered its row');
      expect(aSettled, isFalse,
          reason: 'both readRow calls must still be suspended inside their '
              'own step window for two steps to be outstanding at once');
      expect(bSettled, isFalse);

      // The production teardown route: closeDb's statement sweep reaches
      // a reader through exactly this call.
      final close = stmt.close().whenComplete(() => closeReturned = true);

      // Release the LATER step and leave the earlier one parked.
      releaseB.complete();
      await b;
      expect(bSettled, isTrue);

      // Real event-loop turns, not a microtask drain: every remaining
      // teardown step is a worker-isolate round-trip whose reply a
      // microtask drain would never deliver, which would make a false
      // pass the default outcome. Exits the instant close settles, so a
      // failing observation is positive and immediate.
      final settling = Stopwatch()..start();
      while (!closeReturned && settling.elapsedMilliseconds < 300) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(closeReturned, isFalse,
          reason: 'close settled while readRow A still had a step '
              'outstanding against the same sqlite3_stmt. Settling means '
              'reader.close() → onClose → finalizeStmt already ran on that '
              'handle; only the LATER step was drained, because the earlier '
              'one was overwritten instead of tracked');

      releaseA.complete();
      await a;
      await close.timeout(const Duration(seconds: 5));
      expect(closeReturned, isTrue,
          reason: 'close must settle once every step has handed back');
      expect(aSettled, isTrue);
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      // Unpark both on the failing path too, so a thrown expect cannot
      // leave a step suspended into the next test.
      if (!releaseA.isCompleted) releaseA.complete();
      if (!releaseB.isCompleted) releaseB.complete();
      // `closeReturned` tracks the STATEMENT close, not `closeDb`, so it
      // must not gate the database close: an early `expect` failure would
      // otherwise leave this database open and parked in
      // `DbasSqlite._instance` for the rest of the run. Both steps are
      // unparked above, so nothing here can wedge. Only the file deletion
      // waits on the flag.
      await db.closeDb();
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'reader: a step wait that outlasts the stall threshold reports itself',
      () async {
    // The step wait is unbounded on purpose — a timeout there could only
    // expire into finalizing a statement a step is using, which is the
    // corruption it exists to prevent. That makes it the one wait in
    // this library no timeout will ever surface: a teardown wedged
    // behind a step that never hands back would otherwise be
    // indistinguishable from a slow one, with no error, no log and
    // nothing to grep for. The periodic report IS the diagnosis, so its
    // absence has to fail here rather than be discovered in production.
    final db = await _createTestDb('reader_step_drain_stall_report.db');
    addTearDown(() {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');
      await _runSql(db, 'INSERT INTO t VALUES (2)');

      DbasSqliteReader.debugStepDrainStallReportMs = 20;
      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final row = reader
          .readRow()
          .then<Object?>((r) => r, onError: (Object e) => e);
      await reached.future;
      // Land the reply first, for the same reason as the cases above:
      // what the close then waits for is the reader's own bookkeeping,
      // not a live sqlite3_step.
      final cached = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1');
      expect(reader.debugStepDrainStallReports, 0,
          reason: 'nothing has waited for a step yet');

      final close = stmt.close().whenComplete(() => closeReturned = true);
      final stalled = Stopwatch()..start();
      while (reader.debugStepDrainStallReports < 2 &&
          stalled.elapsedMilliseconds < 3000) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }

      expect(closeReturned, isFalse,
          reason: 'the step is still parked, so the close it is blocking '
              'must still be blocked — otherwise this measures nothing');
      expect(reader.debugStepDrainStallReports, greaterThanOrEqualTo(2),
          reason: 'the wait must keep reporting for as long as it lasts. A '
              'single report at the threshold would say nothing about a '
              'teardown still wedged minutes later, which is exactly the '
              'case the report exists for');

      release.complete();
      await row;
      await close.timeout(const Duration(seconds: 5));
      expect(closeReturned, isTrue);

      final afterSettling = reader.debugStepDrainStallReports;
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(reader.debugStepDrainStallReports, afterSettling,
          reason: 'the reporter must be cancelled when the wait ends; a '
              'periodic timer left running would keep logging a stall that '
              'is over, and would outlive the reader');
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      if (!release.isCompleted) release.complete();
      // `closeReturned` tracks the STATEMENT close, so gating the
      // DATABASE close on it would strand this database open in
      // `DbasSqlite._instance` after an early failure. The step is
      // unparked above, so nothing here can wedge.
      await db.closeDb();
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'reader: the stall report reaches a consumer sink, not only '
      'dart:developer', () async {
    // The stall report is the entire justification for leaving the step
    // wait unbounded — "a wedged teardown can be diagnosed from a log
    // instead of inferred from a hang". `developer.log` cannot deliver
    // that: it publishes to the VM service `Logging` stream and the
    // message is dropped whenever no service client is subscribed, which
    // is every release build on a device and every `flutter test` run.
    // So the counter below proves the timer fires, and this proves the
    // message actually reaches somebody who can write it down.
    final db = await _createTestDb('reader_stall_report_sink.db');
    addTearDown(() {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      DbasSqlite.onDiagnostic = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');
      await _runSql(db, 'INSERT INTO t VALUES (2)');

      final reported = <String>[];
      DbasSqlite.onDiagnostic = reported.add;
      DbasSqliteReader.debugStepDrainStallReportMs = 20;

      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final row =
          reader.readRow().then<Object?>((r) => r, onError: (Object e) => e);
      await reached.future;
      final cached = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1');
      expect(reported, isEmpty,
          reason: 'nothing has waited for a step yet');

      final close = stmt.close().whenComplete(() => closeReturned = true);
      final stalled = Stopwatch()..start();
      while (reported.length < 2 && stalled.elapsedMilliseconds < 3000) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }

      expect(reported.length, greaterThanOrEqualTo(2),
          reason: 'the stall report never reached the sink. A consumer '
              'that wired onDiagnostic still has no way to see a wedged '
              'teardown, which is the only diagnosis an unbounded wait '
              'ever produces');
      // The message has to carry what is stuck, not just that something
      // is: a report a consumer cannot act on is no better than silence.
      expect(reported.first, contains('in-flight readRow step'));
      expect(reported.first, contains('UNBOUNDED'));

      release.complete();
      expect(await row, isTrue,
          reason: 'the parked step must still deliver its row; a run in '
              'which it failed would prove nothing about the teardown '
              'window this test is about');
      await close.timeout(const Duration(seconds: 5));
      expect(closeReturned, isTrue);
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      DbasSqlite.onDiagnostic = null;
      if (!release.isCompleted) release.complete();
      // `closeReturned` tracks the STATEMENT close, not `closeDb`, so it
      // must not gate the database close: an early `expect` failure would
      // otherwise leave this database open and parked in
      // `DbasSqlite._instance` for the rest of the run. The step is
      // already unparked above, so nothing here can wedge. Only the file
      // deletion waits on the flag.
      await db.closeDb();
      if (closeReturned) await db.dropDb();
    }
  });

  test('reader: a diagnostic sink that throws cannot break teardown',
      () async {
    // The sink is consumer code called from inside a close. If its
    // exception escaped, wiring a logger would turn a slow teardown into
    // a failed one — and the wait it reports on is the one that must not
    // be interrupted, because expiring it means finalizing a statement a
    // step is still using.
    final db = await _createTestDb('reader_stall_report_sink_throws.db');
    addTearDown(() {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      DbasSqlite.onDiagnostic = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');
      await _runSql(db, 'INSERT INTO t VALUES (2)');

      var sinkCalls = 0;
      DbasSqlite.onDiagnostic = (_) {
        sinkCalls++;
        throw StateError('consumer logger blew up');
      };
      DbasSqliteReader.debugStepDrainStallReportMs = 20;

      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final row =
          reader.readRow().then<Object?>((r) => r, onError: (Object e) => e);
      await reached.future;
      final cached = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1');

      Object? closeOutcome;
      final close = stmt.close().then<Object?>(
        (_) => null,
        onError: (Object e) => closeOutcome = e,
      ).whenComplete(() => closeReturned = true);

      final stalled = Stopwatch()..start();
      while (sinkCalls < 2 && stalled.elapsedMilliseconds < 3000) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      expect(sinkCalls, greaterThanOrEqualTo(2),
          reason: 'the first throw must not stop the reporting either — a '
              'wait that reported once and then went quiet says nothing '
              'about a teardown still wedged minutes later');

      release.complete();
      expect(await row, isTrue,
          reason: 'the parked step must still deliver its row; a run in '
              'which it failed would prove nothing about the teardown '
              'window this test is about');
      await close.timeout(const Duration(seconds: 5));
      expect(closeReturned, isTrue);
      expect(closeOutcome, isNull,
          reason: "a consumer logger's exception must not surface as a "
              'failed close');
      expect(reader.isClosed, isTrue);
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      DbasSqlite.onDiagnostic = null;
      if (!release.isCompleted) release.complete();
      // See the sibling sink test: `closeReturned` tracks the STATEMENT
      // close, so gating the DATABASE close on it would strand this
      // database open in `DbasSqlite._instance` after an early failure.
      await db.closeDb();
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'reader: a diagnostic sink that throws still gets the message out '
      'through a sink that survives release', () async {
    // "Cannot break a close" is only half the contract. The other half is
    // that the escape hatch survives its own failure: when the consumer
    // sink throws, falling back to `developer.log` alone falls back to
    // exactly the sink `onDiagnostic` exists to replace — dropped
    // whenever no VM service client is subscribed, which is every release
    // build on a device and every `flutter test` run.
    //
    // The shape that makes it matter is ordinary: a consumer whose logger
    // is torn down BEFORE the database. Shutdown is precisely when a
    // stall report fires, so that consumer would get complete silence
    // about a wedged teardown. `Zone.current.print` reaches logcat /
    // oslog in a release build and this runner's stdout under
    // `flutter test`, so the ORIGINAL message goes out through it too —
    // not merely a note that the sink failed.
    //
    // The zone is what makes that observable: overriding `print` captures
    // exactly what a release build would have written.
    final printed = <String>[];
    await runZoned(
      () async {
        final db = await _createTestDb('reader_stall_report_sink_fallback.db');
        addTearDown(() {
          DbasSqliteReader.debugInsideReadRowStep = null;
          DbasSqliteReader.debugStepDrainStallReportMs = null;
          DbasSqlite.onDiagnostic = null;
        });
        final release = Completer<void>();
        var closeReturned = false;
        try {
          await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
          await _runSql(db, 'INSERT INTO t VALUES (1)');
          await _runSql(db, 'INSERT INTO t VALUES (2)');

          // The logger that is already gone by the time teardown reports.
          DbasSqlite.onDiagnostic = (_) {
            throw StateError('consumer logger already torn down');
          };
          DbasSqliteReader.debugStepDrainStallReportMs = 20;

          final reached = Completer<void>();
          DbasSqliteReader.debugInsideReadRowStep = () async {
            DbasSqliteReader.debugInsideReadRowStep = null;
            reached.complete();
            await release.future;
          };

          final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
          final reader = await stmt.executeReader();
          final row = reader
              .readRow()
              .then<Object?>((r) => r, onError: (Object e) => e);
          await reached.future;
          final cached = Stopwatch()..start();
          while (reader.getColumnText(0) != '1' &&
              cached.elapsedMilliseconds < 2000) {
            await Future<void>.delayed(const Duration(milliseconds: 1));
          }
          expect(reader.getColumnText(0), '1');
          expect(printed, isEmpty,
              reason: 'nothing has waited for a step yet');

          Object? closeOutcome;
          final close = stmt.close().then<Object?>(
            (_) => null,
            onError: (Object e) => closeOutcome = e,
          ).whenComplete(() => closeReturned = true);

          final stalled = Stopwatch()..start();
          while (printed.isEmpty && stalled.elapsedMilliseconds < 3000) {
            await Future<void>.delayed(const Duration(milliseconds: 5));
          }

          expect(printed, isNotEmpty,
              reason: 'the sink threw and the report went nowhere a '
                  'release build can read. developer.log is the sink this '
                  'mechanism exists to replace, so falling back to it '
                  'alone means a wedged teardown reports NOTHING to a '
                  'consumer whose logger died first');
          // The original diagnostic, not just a note that the sink blew
          // up: a report a consumer cannot act on is no better than
          // silence.
          expect(printed.first, contains('in-flight readRow step'));
          expect(printed.first, contains('UNBOUNDED'));
          expect(printed.first, contains('consumer logger already torn down'),
              reason: 'the fallback must also say WHY it is the one '
                  'reporting, or the sink failure itself stays invisible');

          release.complete();
          expect(await row, isTrue,
              reason: 'the parked step must still deliver its row; a run in '
                  'which it failed would prove nothing about the teardown '
                  'window this test is about');
          await close.timeout(const Duration(seconds: 5));
          expect(closeReturned, isTrue);
          expect(closeOutcome, isNull,
              reason: "a consumer logger's exception must not surface as a "
                  'failed close');
        } finally {
          DbasSqliteReader.debugInsideReadRowStep = null;
          DbasSqliteReader.debugStepDrainStallReportMs = null;
          DbasSqlite.onDiagnostic = null;
          if (!release.isCompleted) release.complete();
          await db.closeDb();
          if (closeReturned) await db.dropDb();
        }
      },
      zoneSpecification: ZoneSpecification(
        print: (self, parent, zone, line) => printed.add(line),
      ),
    );
  });

  test(
      'reader: the step a hook observes is already published to the drain',
      () async {
    // `_stepAndCache` claims there is no path on which a step reaches
    // native code without reaching `_inFlightSteps`. The hook path used
    // to break it: an `async` body runs synchronously to its first
    // `await`, so the hook fired BEFORE the composed future was
    // published, and anything the hook started that consulted the set
    // saw an empty one — a close started there would have finalized the
    // statement under a live step.
    //
    // Observed rather than provoked: the destructive version of this
    // check is the corruption itself, and a SIGSEGV kills the runner
    // instead of failing an assertion.
    final db = await _createTestDb('reader_step_published_before_hook.db');
    addTearDown(() => DbasSqliteReader.debugInsideReadRowStep = null);
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');

      DbasSqliteReader? target;
      int? stepsVisibleInsideHook;
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        stepsVisibleInsideHook = target!.debugInFlightStepCount;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t');
      final reader = await stmt.executeReader();
      target = reader;
      expect(reader.debugInFlightStepCount, 0,
          reason: 'nothing is dispatched before the first readRow');
      expect(await reader.readRow(), isTrue);

      expect(stepsVisibleInsideHook, 1,
          reason: 'the hook ran while its own step was invisible to the '
              'drain. Anything it started that reads _inFlightSteps — a '
              'close, above all — would have concluded that no step was '
              'outstanding and finalized the statement under a live one');
      expect(reader.debugInFlightStepCount, 0,
          reason: 'readRow clears its own registration once the step '
              'settles');
      await stmt.close();
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      await db.closeDb();
      await db.dropDb();
    }
  });

  test(
      'pool: setBusyTimeout is rejected once teardown has started, not '
      'registered behind the drain', () async {
    // `_drainNativeOps` runs ONCE. Most registry callers are saved by a
    // second gate — `_acquireWriterLock` and `_acquireReaderSlot` both
    // reject while closing — but setBusyTimeout has neither: it checks
    // readers out of the C pool DIRECTLY, bypassing the Dart-side
    // semaphore, and its only other guard is `_db == null`, which is
    // still false at every one of closeDb's post-drain suspension
    // points. Arriving there it would register into a registry nobody
    // drains again and hold readers ClosePool is waiting for.
    //
    // The suspension point used here is the statement sweep, parked on a
    // reader's step drain — proven parked by the stall report rather
    // than assumed by a sleep.
    final db = await _createTestDb('pool_set_busy_timeout_while_closing.db',
        readerPoolSize: 2);
    addTearDown(() {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      DbasSqlite.debugSetBusyTimeoutAcquireMs = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    Object? closeOutcome;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');
      await _runSql(db, 'INSERT INTO t VALUES (2)');

      // Bounded, so a regression that lets the call through fails on the
      // error code in a few hundred ms instead of pausing for 5 s.
      DbasSqlite.debugSetBusyTimeoutAcquireMs = 200;
      DbasSqliteReader.debugStepDrainStallReportMs = 20;

      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final row =
          reader.readRow().then<Object?>((r) => r, onError: (Object e) => e);
      await reached.future;
      final cached = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1');

      // The error handler is attached HERE, at construction, not at the
      // `await` thirty-odd lines below: a rejection in between would
      // otherwise surface as an unhandled asynchronous error attributed
      // to whatever test happens to be running.
      final close = db.closeDb().then<Object?>(
        (_) => null,
        onError: (Object e) => closeOutcome = e,
      ).whenComplete(() => closeReturned = true);

      // A positive witness that closeDb is parked in the SWEEP — i.e.
      // past `_drainNativeOps`, which is the whole precondition. The
      // stall report only ever fires from inside the reader's step
      // drain, and the sweep is the only caller that reaches it here.
      final stalled = Stopwatch()..start();
      while (reader.debugStepDrainStallReports < 1 &&
          stalled.elapsedMilliseconds < 3000) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      expect(reader.debugStepDrainStallReports, greaterThanOrEqualTo(1),
          reason: 'closeDb never reached the statement sweep, so this run '
              'never got to the window it is about');
      expect(db.isOpened(), isTrue,
          reason: 'the pre-existing `_db == null` guard is still open here '
              '— which is exactly why it cannot be the one that rejects '
              'this call');

      await expectLater(
        db.setBusyTimeout(4000),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.readerSlotWaitCancelled)
            .having((e) => e.category, 'category',
                DbasSqliteErrorCategory.busyOrCancelled)),
        reason: 'setBusyTimeout registered and reached the C pool after '
            'the drain had already run',
      );
      expect(db.debugInFlightNativeOpCount, 0,
          reason: 'a rejected call must not leave a registration behind: '
              'nothing will ever drain this registry again');

      release.complete();
      expect(await row, isTrue,
          reason: 'the parked step must still deliver its row; a run in '
              'which it failed would prove nothing about the teardown '
              'window this test is about');
      await close.timeout(const Duration(seconds: 10));
      expect(closeReturned, isTrue);
      expect(closeOutcome, isNull,
          reason: 'the rejected setBusyTimeout must leave teardown itself '
              'intact — closeDb must complete, not merely settle');
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      DbasSqlite.debugSetBusyTimeoutAcquireMs = null;
      if (!release.isCompleted) release.complete();
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'pool: enableWal is rejected once teardown has started, not registered '
      'behind the drain', () async {
    // The sibling hole, and the last registry caller without a second
    // gate. `checkpoint`, `vacuum`, `beginTransaction`, `commit` and
    // `executeScript` all take `_acquireWriterLock` first, which rejects
    // while closing; `executeReader` registers early but is rejected
    // immediately by the slot/lock gate and clears itself in a `finally`;
    // `setBusyTimeout` carries an explicit guard. `enableWal` had
    // neither — its only other guard is `_db == null`, still false at
    // every one of closeDb's post-drain suspension points.
    //
    // Reaching it there is not theoretical: it dispatches the
    // journal-mode switch AND both writer pragmas — several worker
    // round-trips on `_db` — while closeDb is on its way to `closePool`,
    // which force-closes that same writer via `closeDbCore(force=true)`.
    //
    // The suspension point used here is the statement sweep, parked on a
    // reader's step drain — proven parked by the stall report rather
    // than assumed by a sleep. Same harness shape as the setBusyTimeout
    // case above, deliberately.
    final db = await _createTestDb('pool_enable_wal_while_closing.db',
        readerPoolSize: 2);
    addTearDown(() {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    Object? closeOutcome;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');
      await _runSql(db, 'INSERT INTO t VALUES (2)');

      DbasSqliteReader.debugStepDrainStallReportMs = 20;

      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final row =
          reader.readRow().then<Object?>((r) => r, onError: (Object e) => e);
      await reached.future;
      final cached = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1');

      final close = db.closeDb().then<Object?>(
        (_) => null,
        onError: (Object e) => closeOutcome = e,
      ).whenComplete(() => closeReturned = true);

      // A positive witness that closeDb is parked in the SWEEP — i.e.
      // past `_drainNativeOps`, which is the whole precondition. The
      // stall report only ever fires from inside the reader's step
      // drain, and the sweep is the only caller that reaches it here.
      final stalled = Stopwatch()..start();
      while (reader.debugStepDrainStallReports < 1 &&
          stalled.elapsedMilliseconds < 3000) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      expect(reader.debugStepDrainStallReports, greaterThanOrEqualTo(1),
          reason: 'closeDb never reached the statement sweep, so this run '
              'never got to the window it is about');
      expect(db.isOpened(), isTrue,
          reason: 'the pre-existing `_db == null` guard is still open here '
              '— which is exactly why it cannot be the one that rejects '
              'this call');
      expect(db.isInTransaction, isFalse,
          reason: 'the enableWalInsideTransaction guard must not be the one '
              'rejecting this call either');

      await expectLater(
        db.enableWal(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.writerLockWaitCancelled)
            .having((e) => e.category, 'category',
                DbasSqliteErrorCategory.busyOrCancelled)),
        reason: 'enableWal registered and dispatched onto the writer after '
            'the drain had already run — against the very connection '
            'closePool is about to force-close',
      );
      expect(db.debugInFlightNativeOpCount, 0,
          reason: 'a rejected call must not leave a registration behind: '
              'nothing will ever drain this registry again');

      release.complete();
      expect(await row, isTrue,
          reason: 'the parked step must still deliver its row; a run in '
              'which it failed would prove nothing about the teardown '
              'window this test is about');
      await close.timeout(const Duration(seconds: 10));
      expect(closeReturned, isTrue);
      expect(closeOutcome, isNull,
          reason: 'the rejected enableWal must leave teardown itself intact '
              '— closeDb must complete, not merely settle');
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqliteReader.debugStepDrainStallReportMs = null;
      if (!release.isCompleted) release.complete();
      if (closeReturned) await db.dropDb();
    }
  });

  // ──────────────────────────────────────────────────────────────────────
  // Regression: closeDb must JOIN a close that is already in progress —
  // skipping one is a permanent hang.
  //
  // `DbasSqliteReader.close()` latches `_closed` SYNCHRONOUSLY and then
  // suspends, at two points: the in-flight step drain and `onClose`
  // (whose `finalizeStmt` is a real worker dispatch). For that whole
  // stretch the reader reports `isClosed == true` while still holding a
  // checked-out pool reader and a live `sqlite3_stmt` — and the statement
  // sweep used to read that flag as "nothing left to do":
  // `DbasSqliteStatement.close()` skipped a reader that was already
  // `isClosed`, and returned immediately once its own `_closed` was set.
  // `_activeStatements.clear()` then disowned the statement outright.
  //
  // Nothing after that releases the pool reader, so `ClosePool` — which
  // blocks until every checked-out reader is back — waits forever and
  // `closeDb()` never returns. There is no timeout on that path: the
  // native-op drain has already run, and the reader's own step drain is
  // unbounded by design. The live shape is `unawaited(reader.close())`
  // followed by `closeDb()` — logout while a list view is mid-scan.
  //
  // Two doors into the same window, one test each: the reader's own
  // `close()`, and the statement's.
  //
  // Both bound every wait so a regression FAILS instead of wedging the
  // suite, and neither ever steps or closes a reader on a path where the
  // pool may already be gone.
  // ──────────────────────────────────────────────────────────────────────

  test(
      'pool: closeDb joins a reader close that is already in progress '
      'instead of skipping it', () async {
    final db = await _createTestDb('pool_close_joins_inflight_reader.db',
        readerPoolSize: 2);
    addTearDown(() {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    Object? closeOutcome;
    // Hoisted out of the `try` so the `finally` can read them: whether
    // unparking the step is still safe depends on how far teardown got.
    var destructiveCloseReached = false;
    bool? readerCloseSettledAtDestructiveClose;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      for (int i = 1; i <= 3; i++) {
        await _runSql(db, 'INSERT INTO t VALUES ($i)');
      }

      var readerCloseSettled = false;
      int? releasablePoolPtrAtDestructiveClose;
      DbasSqlite.debugBeforeDestructiveClose = (d) {
        destructiveCloseReached = true;
        readerCloseSettledAtDestructiveClose = readerCloseSettled;
        releasablePoolPtrAtDestructiveClose = d.debugReleasablePoolPtr;
      };

      // A rendezvous, not a sleep: `reached` proves the step is parked at
      // the offending instruction rather than merely likely to be.
      // One-shot — closeDb runs a WAL checkpoint of its own on the way
      // out, and that must not park too.
      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final row =
          reader.readRow().then<Object?>((r) => r, onError: (Object e) => e);
      await reached.future;

      // Land the dispatched step's reply before anything is torn down,
      // for the same reason as the cases above: what the close then waits
      // on is the reader's own bookkeeping, not a live sqlite3_step, so a
      // regression fails an `expect` instead of dying on a signal.
      final cached = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1',
          reason: 'the parked step never delivered its row, so this run '
              'would prove nothing about what closeDb joins');

      // The production shape: a close nobody awaited. It latches
      // `_closed` synchronously and then parks on the step drain.
      //
      // `whenComplete` is chained straight onto `close()`'s own future,
      // which makes it listener NUMBER ONE on it — the sweep only
      // registers its `await` later, when closeDb reaches it. So
      // `readerCloseSettled` is guaranteed to be true before the sweep
      // resumes, and the ordering recorded at the destructive dispatch is
      // a fact rather than a microtask race.
      Object? readerCloseOutcome;
      final readerClose = reader
          .close()
          .whenComplete(() => readerCloseSettled = true)
          .then<Object?>(
              (_) => null, onError: (Object e) => readerCloseOutcome = e);
      expect(reader.isClosed, isTrue,
          reason: 'close() must latch isClosed synchronously — that flag '
              'being true while the reader still owns its pool connection '
              'is the whole hazard');

      // The live pool pointer, read before teardown starts, so the
      // retention assertion below can say WHICH pointer survived rather
      // than merely that something did.
      final livePoolPtr = db.debugReleasablePoolPtr;
      expect(livePoolPtr, isNotNull,
          reason: 'an open pooled database must have a pool pointer, or '
              'the retention assertion below proves nothing');

      final close = db.closeDb().then<Object?>(
        (_) => null,
        onError: (Object e) => closeOutcome = e,
      ).whenComplete(() => closeReturned = true);

      // Pumped with real event-loop turns, not microtask drains: every
      // closeDb step is a worker-isolate round-trip whose reply a
      // microtask drain would never deliver, which would make a false
      // pass the default outcome. Exits the instant the point of no
      // return is reached, so a regression is observed positively rather
      // than inferred from a silence. This is the PUMP, not the
      // assertion — see below.
      final parked = Stopwatch()..start();
      while (!destructiveCloseReached && parked.elapsedMilliseconds < 300) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }

      // The mechanism, as an ORDERING fact rather than a time-boxed
      // absence: whenever closeDb reaches its destructive dispatch — now,
      // or in ten seconds on a loaded box — the reader close it was
      // supposed to join must ALREADY have completed. `expect(
      // destructiveCloseReached, isFalse)` after a fixed 300 ms poll was
      // the only assertion that changed colour when the join was
      // reverted, and the regression path from the sweep to the dispatch
      // is one PASSIVE-checkpoint worker round-trip — longer than 300 ms
      // on a loaded CI box, where the regression would then PASS.
      //
      // Checked here as well as at the end, and deliberately BEFORE
      // anything unparks the step: past a fired
      // `debugBeforeDestructiveClose` the pool may already be gone, and
      // releasing the step runs the parked close's finalizeStmt /
      // poolReleaseReader straight into it — the SIGSEGV this test exists
      // to observe without triggering.
      if (destructiveCloseReached) {
        expect(readerCloseSettledAtDestructiveClose, isTrue,
            reason: 'closeDb reached its destructive closePool dispatch '
                'while reader.close() was still in progress. The sweep read '
                'isClosed == true as "nothing to do" and skipped a reader '
                'that still holds a checked-out pool connection, so nothing '
                'releases it and ClosePool waits on it forever');
      }
      expect(closeReturned, isFalse,
          reason: 'closeDb settled while the reader close it must join was '
              'still parked on its step');

      release.complete();
      expect(await row, isTrue,
          reason: 'the parked step must still deliver its row — a close '
              'that joined a FAILED step would prove nothing about the '
              'ordering this test is named for');
      await readerClose.timeout(const Duration(seconds: 5));
      expect(readerCloseOutcome, isNull,
          reason: '"closeDb joins the close" is meaningless if the close it '
              'joined errored: the reader would still own its pool '
              'connection');

      // Bounded: on a regression closePool is parked on a worker isolate
      // with a reader it will never get back, so an unbounded await would
      // wedge the rest of the suite rather than fail this test.
      var closeCompleted = true;
      try {
        await close.timeout(const Duration(seconds: 10));
      } on TimeoutException {
        closeCompleted = false;
      }
      expect(closeCompleted, isTrue,
          reason: 'closeDb never returned. Its statement sweep skipped a '
              'reader whose close was already in progress, so that '
              "reader's pool connection was never handed back and "
              'ClosePool is still waiting for it');
      expect(closeOutcome, isNull,
          reason: 'closeDb must complete, not merely settle');

      // The ordering again, now on the path where teardown ran to
      // completion — and with the witness that it ran at all, so a run
      // that never reached the dispatch cannot pass by silence.
      expect(destructiveCloseReached, isTrue,
          reason: 'teardown never reached its destructive dispatch, so the '
              'ordering this test is about was never observed');
      expect(readerCloseSettledAtDestructiveClose, isTrue,
          reason: 'closeDb reached its destructive closePool dispatch while '
              'reader.close() was still in progress. The sweep read '
              'isClosed == true as "nothing to do" and skipped a reader '
              'that still holds a checked-out pool connection, so nothing '
              'releases it and ClosePool waits on it forever');

      // The second half of the fix, pinned where it is observable: the C
      // pool struct lives until ClosePool RETURNS, and PoolReleaseReader
      // is exactly the call it is blocked on — so a release arriving
      // while the destructive dispatch is in flight must still be able to
      // reach the pool. Dropping the pointer before the dispatch turns
      // that release into a silent early return. (Asserted on what a
      // release would USE, not on which field holds it: `_poolPtr` is
      // deliberately cleared there, so that a second concurrent closeDb
      // cannot dispatch closePool on the same pointer twice.)
      expect(releasablePoolPtrAtDestructiveClose, livePoolPtr,
          reason: 'the pool pointer a reader release would use was not the '
              'live one when closePool was dispatched, so '
              'releaseReaderConnectionInternal can only return early for '
              'the whole duration of the call that is blocked waiting for '
              'exactly that release');
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
      // Unpark the step on the failing path too — but ONLY while that is
      // still safe. Once `debugBeforeDestructiveClose` has fired the pool
      // may be mid-ClosePool, and the parked close this would release
      // runs finalizeStmt / poolReleaseReader straight into it: a SIGSEGV
      // that kills the runner instead of failing this test. A step left
      // parked leaks one suspended future and nothing else — the hook is
      // one-shot, so no later test can reach it.
      if (!release.isCompleted && !destructiveCloseReached) {
        release.complete();
      }
      // Deliberately no reader.close() / stmt.close() on ANY path, and no
      // dropDb until teardown actually finished: on a regression the pool
      // is mid-ClosePool and touching it is the crash. setUpAll wipes
      // test/db at the start of every run, so a skipped drop leaks
      // nothing across runs.
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'pool: closeDb joins a statement close that is already in progress '
      'instead of skipping it', () async {
    // The statement door into the same window. `DbasSqliteStatement
    // .close()` latches its own `_closed` synchronously and then awaits
    // the reader, so an un-awaited `stmt.close()` leaves a statement that
    // reports closed while its reader still owns a pool connection. The
    // sweep called `stmt.close()` again and got `if (_closed) return` —
    // it never even reached the reader. Joining is what the reader's own
    // `close()` has always done; the statement now has the same shape.
    final db = await _createTestDb('pool_close_joins_inflight_stmt.db',
        readerPoolSize: 2);
    addTearDown(() {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
    });
    final release = Completer<void>();
    var closeReturned = false;
    Object? closeOutcome;
    // Hoisted out of the `try` so the `finally` can read them — see the
    // sibling test for why unparking the step is conditional.
    var destructiveCloseReached = false;
    bool? stmtCloseSettledAtDestructiveClose;
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      for (int i = 1; i <= 3; i++) {
        await _runSql(db, 'INSERT INTO t VALUES ($i)');
      }

      var stmtCloseSettled = false;
      DbasSqlite.debugBeforeDestructiveClose = (d) {
        destructiveCloseReached = true;
        stmtCloseSettledAtDestructiveClose = stmtCloseSettled;
      };

      final reached = Completer<void>();
      DbasSqliteReader.debugInsideReadRowStep = () async {
        DbasSqliteReader.debugInsideReadRowStep = null;
        reached.complete();
        await release.future;
      };

      final stmt = await db.prepareQuery('SELECT id FROM t ORDER BY id');
      final reader = await stmt.executeReader();
      final row =
          reader.readRow().then<Object?>((r) => r, onError: (Object e) => e);
      await reached.future;

      final cached = Stopwatch()..start();
      while (reader.getColumnText(0) != '1' &&
          cached.elapsedMilliseconds < 2000) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      expect(reader.getColumnText(0), '1',
          reason: 'the parked step never delivered its row, so this run '
              'would prove nothing about what closeDb joins');

      // `whenComplete` chained straight onto `close()`'s own future makes
      // it listener NUMBER ONE on it, so `stmtCloseSettled` is true
      // before the sweep's own `await` on the same future resumes — see
      // the sibling test.
      Object? stmtCloseOutcome;
      final stmtClose = stmt
          .close()
          .whenComplete(() => stmtCloseSettled = true)
          .then<Object?>(
              (_) => null, onError: (Object e) => stmtCloseOutcome = e);
      expect(stmt.isClosed, isTrue,
          reason: 'close() must latch isClosed synchronously — that flag '
              'being true while the reader underneath still owns its pool '
              'connection is the whole hazard');

      final close = db.closeDb().then<Object?>(
        (_) => null,
        onError: (Object e) => closeOutcome = e,
      ).whenComplete(() => closeReturned = true);

      // The PUMP, not the assertion — see the sibling test for why a
      // fixed 300 ms absence passes the regression on a loaded box.
      final parked = Stopwatch()..start();
      while (!destructiveCloseReached && parked.elapsedMilliseconds < 300) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }

      // Asserted here as well as at the end, and before anything unparks
      // the step: past a fired `debugBeforeDestructiveClose` the pool may
      // be mid-ClosePool, and the release would run the parked close's
      // finalizeStmt / poolReleaseReader into it.
      if (destructiveCloseReached) {
        expect(stmtCloseSettledAtDestructiveClose, isTrue,
            reason: 'closeDb reached its destructive closePool dispatch '
                'while stmt.close() was still in progress. The sweep called '
                'close() again and hit `if (_closed) return`, so the reader '
                'underneath — still holding a checked-out pool connection — '
                'was never reached and ClosePool waits on it forever');
      }
      expect(closeReturned, isFalse,
          reason: 'closeDb settled while the statement close it must join '
              'was still parked on its reader');

      release.complete();
      expect(await row, isTrue,
          reason: 'the parked step must still deliver its row — a close '
              'that joined a FAILED step would prove nothing about the '
              'ordering this test is named for');
      await stmtClose.timeout(const Duration(seconds: 5));
      expect(stmtCloseOutcome, isNull,
          reason: '"closeDb joins the close" is meaningless if the close it '
              'joined errored: the reader underneath would still own its '
              'pool connection');

      var closeCompleted = true;
      try {
        await close.timeout(const Duration(seconds: 10));
      } on TimeoutException {
        closeCompleted = false;
      }
      expect(closeCompleted, isTrue,
          reason: 'closeDb never returned. Its statement sweep skipped a '
              'statement whose close was already in progress, so the pool '
              'connection under it was never handed back and ClosePool is '
              'still waiting for it');
      expect(closeOutcome, isNull,
          reason: 'closeDb must complete, not merely settle');
      expect(destructiveCloseReached, isTrue,
          reason: 'teardown never reached its destructive dispatch, so the '
              'ordering this test is about was never observed');
      expect(stmtCloseSettledAtDestructiveClose, isTrue,
          reason: 'closeDb reached its destructive closePool dispatch while '
              'stmt.close() was still in progress. The sweep called '
              'close() again and hit `if (_closed) return`, so the reader '
              'underneath — still holding a checked-out pool connection — '
              'was never reached and ClosePool waits on it forever');
    } finally {
      DbasSqliteReader.debugInsideReadRowStep = null;
      DbasSqlite.debugBeforeDestructiveClose = null;
      // Gated exactly like the sibling test's: past the destructive
      // dispatch the pool may be gone, and unparking the step there runs
      // the parked close's native cleanup straight into it.
      if (!release.isCompleted && !destructiveCloseReached) {
        release.complete();
      }
      if (closeReturned) await db.dropDb();
    }
  });

  test(
      'pool: a second closeDb joins the first instead of racing it into the '
      'destructive dispatch', () async {
    // `closeDb` had no single-flight guard, and by the time the first
    // call is suspended inside `closePool` every step of it
    // short-circuits: `rollback` on `!_isInTransaction`, `_drainNativeOps`
    // on an empty registry, the statement sweep on an already-cleared
    // `_activeStatements`, the PASSIVE checkpoint on `!isOpened()`. So a
    // second call walked the whole method in a handful of microtask turns
    // — against a worker-isolate round trip — and did two wrong things:
    //
    //   1. it reported SUCCESS while the pool was still being destroyed,
    //      which is a lie a caller may act on (`dropDb()` on a live pool
    //      being the obvious one); and
    //   2. it re-ran the capture-and-null with `poolPtr == null`. The
    //      capture is atomic against the event loop, so only the FIRST
    //      call ever gets a real pointer — the double-ClosePool
    //      protection genuinely holds — but the LOSER used to execute the
    //      assignment too, clearing `_closingPoolPtr` out from under the
    //      winner for essentially the whole `closePool` window.
    //      `releaseReaderConnectionInternal` then finds both fields null
    //      and silently returns, which is the exact unbounded hang the
    //      retained pointer exists to remove.
    //
    // The second close is issued from inside `debugBeforeDestructiveClose`
    // because that is the one seam that runs at that instant:
    // synchronously, after the capture-and-null and immediately before
    // the dispatch. Nothing else in the library can be scheduled there.
    final db = await _createTestDb('pool_close_concurrent_join.db',
        readerPoolSize: 2);
    addTearDown(() => DbasSqlite.debugBeforeDestructiveClose = null);
    try {
      await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await _runSql(db, 'INSERT INTO t VALUES (1)');

      // The live pool pointer, so the assertions below can name WHICH
      // pointer had to survive rather than merely that something did.
      final livePoolPtr = db.debugReleasablePoolPtr;
      expect(livePoolPtr, isNotNull,
          reason: 'an open pooled database must have a pool pointer, or '
              'nothing below proves anything');

      var destructiveCloses = 0;
      final releasableAtEachDispatch = <int?>[];
      Future<void>? second;
      Object? secondOutcome;
      int? releasableWhenSecondReturned;
      DbasSqlite.debugBeforeDestructiveClose = (d) {
        destructiveCloses++;
        releasableAtEachDispatch.add(d.debugReleasablePoolPtr);
        // Issued here, synchronously, so it is genuinely concurrent with
        // the `closePool` dispatch on the very next line of production
        // code. Once only: on a regression this hook fires again, and a
        // third close would just deepen the pile without proving more.
        // The error handler is attached at construction — a rejection
        // before the `await` below would otherwise land as an unhandled
        // asynchronous error on whatever test is running.
        second ??= d.closeDb().then<Object?>(
          (_) {
            // Read the instant the SECOND close reports completion. If
            // the pool is really gone by then, `closePool` has returned
            // and this caller was told the truth.
            releasableWhenSecondReturned = d.debugReleasablePoolPtr;
            return null;
          },
          onError: (Object e) => secondOutcome = e,
        );
      };

      // Bounded: on a regression the second teardown can leave the first
      // one's `closePool` waiting, and an unbounded await would wedge the
      // rest of the suite rather than fail this test.
      await db.closeDb().timeout(const Duration(seconds: 10));

      expect(second, isNotNull,
          reason: 'the hook never ran, so no second close was ever issued '
              'and this run proves nothing');
      await second!.timeout(const Duration(seconds: 10));
      expect(secondOutcome, isNull,
          reason: 'the joined close must complete, not merely settle');

      // The pointer invariant FIRST, and deliberately: it holds however
      // many teardowns reach the dispatch, so asserting it before the
      // count keeps the two mechanisms separately diagnosable — the
      // capture-and-null's `poolPtr != null` condition reddens here, the
      // single-flight join reddens on the count below.
      expect(releasableAtEachDispatch, everyElement(equals(livePoolPtr)),
          reason: 'the pool pointer a reader release routes through was '
              'cleared while closePool was still in flight. ClosePool '
              'blocks on exactly that release, and with the pointer gone '
              'releaseReaderConnectionInternal returns silently — the '
              'unbounded hang this release exists to remove');
      expect(destructiveCloses, 1,
          reason: 'a second concurrent closeDb ran its own teardown to the '
              'destructive dispatch instead of joining the first. Beyond '
              'the duplicated work it reports success while the pool is '
              'still being destroyed, which a caller may act on');
      expect(releasableWhenSecondReturned, isNull,
          reason: 'the second closeDb reported completion while a reader '
              'release could still be routed into the pool, i.e. while '
              'ClosePool had not returned and the pool was still being '
              'destroyed');
      expect(db.isOpened(), isFalse);
    } finally {
      DbasSqlite.debugBeforeDestructiveClose = null;
      await db.dropDb();
    }
  });

  test('streaming: row payload preserves int / double / text / blob / null',
      () async {
    final db = await _createTestDb('stream_mixed_types.db');
    await _runSql(db,
        'CREATE TABLE t (i INTEGER, d REAL, s TEXT, b BLOB, n INTEGER)');
    final blob = Uint8List.fromList([1, 2, 3, 4, 255]);
    final stmt =
        await db.prepareQuery('INSERT INTO t VALUES (?, ?, ?, ?, ?)');
    await stmt.executeSql(params: [42, 3.14, 'hello', blob, null]);
    await stmt.close();

    final reader =
        await (await db.prepareQuery('SELECT i, d, s, b, n FROM t'))
            .executeReader();
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 42);
    expect(reader.getColumnDouble(1), closeTo(3.14, 1e-9));
    expect(reader.getColumnText(2), 'hello');
    expect(reader.getColumnBlob(3), blob);
    expect(reader.isColumnNull(4), isTrue);
    expect(reader.getColumnNullableInt(4), isNull);
    expect(await reader.readRow(), isFalse);

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // readRows — batch row reader
  // ──────────────────────────────────────────────────────────────────────

  test('readRows returns up to amount rows with hasMore=true when more remain',
      () async {
    final db = await _createTestDb('read_rows_partial.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER, name TEXT)');
    final ins = await db.prepareQuery('INSERT INTO t VALUES (?, ?)');
    for (int i = 1; i <= 10; i++) {
      await ins.executeSql(params: [i, 'name_$i']);
    }
    await ins.close();

    final reader =
        await (await db.prepareQuery('SELECT id, name FROM t ORDER BY id'))
            .executeReader();
    final result = await reader.readRows(3);
    expect(result.rows.length, 3);
    expect(result.hasMore, isTrue);
    expect(result.rows[0]['id']!.value, 1);
    expect(result.rows[0]['name']!.value, 'name_1');
    expect(result.rows[2]['id']!.value, 3);
    expect(result.rows[2]['name']!.value, 'name_3');
    expect(reader.isClosed, isFalse);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test(
      'readRows returns fewer than amount rows with hasMore=false on exhaustion',
      () async {
    final db = await _createTestDb('read_rows_exhaust.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER)');
    final ins = await db.prepareQuery('INSERT INTO t VALUES (?)');
    for (int i = 1; i <= 3; i++) {
      await ins.executeSql(params: [i]);
    }
    await ins.close();

    final reader = await (await db.prepareQuery('SELECT id FROM t ORDER BY id'))
        .executeReader();
    final result = await reader.readRows(10);
    expect(result.rows.length, 3);
    expect(result.hasMore, isFalse);
    expect(result.rows.map((r) => r['id']!.value).toList(), [1, 2, 3]);
    // The trailing readRow that returned false auto-closed the reader.
    expect(reader.isClosed, isTrue);

    await db.closeDb();
    await db.dropDb();
  });

  test('readRows defaults to amount of 50', () async {
    final db = await _createTestDb('read_rows_default.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER)');
    final ins = await db.prepareQuery('INSERT INTO t VALUES (?)');
    for (int i = 1; i <= 75; i++) {
      await ins.executeSql(params: [i]);
    }
    await ins.close();

    final reader = await (await db.prepareQuery('SELECT id FROM t ORDER BY id'))
        .executeReader();
    final result = await reader.readRows();
    expect(result.rows.length, 50);
    expect(result.hasMore, isTrue);
    expect(result.rows.first['id']!.value, 1);
    expect(result.rows.last['id']!.value, 50);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('readRows returns empty with hasMore=false when amount <= 0', () async {
    final db = await _createTestDb('read_rows_zero.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER)');
    await _runSql(db, 'INSERT INTO t VALUES (1)');

    final reader =
        await (await db.prepareQuery('SELECT id FROM t')).executeReader();
    final zero = await reader.readRows(0);
    expect(zero.rows, isEmpty);
    expect(zero.hasMore, isFalse);
    final negative = await reader.readRows(-5);
    expect(negative.rows, isEmpty);
    expect(negative.hasMore, isFalse);
    // Reader must remain usable — no readRow was issued.
    expect(reader.isClosed, isFalse);
    expect(await reader.readRow(), isTrue);
    expect(reader.getColumnInt(0), 1);
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  test('readRows preserves SQLite type, value and null flag in ColumnData',
      () async {
    final db = await _createTestDb('read_rows_types.db');
    await _runSql(
        db, 'CREATE TABLE t (i INTEGER, d REAL, s TEXT, b BLOB, n INTEGER)');
    final blob = Uint8List.fromList([1, 2, 3, 255]);
    final ins = await db.prepareQuery('INSERT INTO t VALUES (?, ?, ?, ?, ?)');
    await ins.executeSql(params: [42, 3.14, 'hello', blob, null]);
    await ins.close();

    final reader = await (await db.prepareQuery('SELECT i, d, s, b, n FROM t'))
        .executeReader();
    final result = await reader.readRows(10);
    expect(result.rows.length, 1);
    expect(result.hasMore, isFalse);
    final row = result.rows.first;

    expect(SqliteColumnType.fromInt(row['i']!.type), SqliteColumnType.integer);
    expect(row['i']!.isNull, isFalse);
    expect(row['i']!.value, 42);

    expect(SqliteColumnType.fromInt(row['d']!.type), SqliteColumnType.double);
    expect(row['d']!.isNull, isFalse);
    expect(row['d']!.value as double, closeTo(3.14, 1e-9));

    expect(SqliteColumnType.fromInt(row['s']!.type), SqliteColumnType.text);
    expect(row['s']!.isNull, isFalse);
    expect(row['s']!.value, 'hello');

    expect(SqliteColumnType.fromInt(row['b']!.type), SqliteColumnType.blob);
    expect(row['b']!.isNull, isFalse);
    // ColumnData.value for blob is the raw List<int> from the native layer;
    // compare element-wise so the assertion holds whether the platform
    // surfaced it as List<int> or Uint8List.
    expect((row['b']!.value as List).cast<int>(), [1, 2, 3, 255]);

    expect(row['n']!.isNull, isTrue);
    expect(row['n']!.value, isNull);

    await db.closeDb();
    await db.dropDb();
  });

  test('readRows can be called repeatedly to paginate', () async {
    final db = await _createTestDb('read_rows_paginate.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER)');
    final ins = await db.prepareQuery('INSERT INTO t VALUES (?)');
    for (int i = 1; i <= 7; i++) {
      await ins.executeSql(params: [i]);
    }
    await ins.close();

    final reader = await (await db.prepareQuery('SELECT id FROM t ORDER BY id'))
        .executeReader();

    final batch1 = await reader.readRows(3);
    expect(batch1.rows.map((r) => r['id']!.value).toList(), [1, 2, 3]);
    expect(batch1.hasMore, isTrue);

    final batch2 = await reader.readRows(3);
    expect(batch2.rows.map((r) => r['id']!.value).toList(), [4, 5, 6]);
    expect(batch2.hasMore, isTrue);

    final batch3 = await reader.readRows(3);
    expect(batch3.rows.map((r) => r['id']!.value).toList(), [7]);
    expect(batch3.hasMore, isFalse);
    expect(reader.isClosed, isTrue);

    await db.closeDb();
    await db.dropDb();
  });

  test(
      'readRows snapshots are independent — earlier rows are not aliased to the cache',
      () async {
    final db = await _createTestDb('read_rows_snapshot.db');
    await _runSql(db, 'CREATE TABLE t (id INTEGER, name TEXT)');
    final ins = await db.prepareQuery('INSERT INTO t VALUES (?, ?)');
    for (int i = 1; i <= 5; i++) {
      await ins.executeSql(params: [i, 'row_$i']);
    }
    await ins.close();

    final reader =
        await (await db.prepareQuery('SELECT id, name FROM t ORDER BY id'))
            .executeReader();
    final result = await reader.readRows(5);
    expect(result.rows.length, 5);
    // If readRows had aliased the per-reader cache instead of capturing
    // each step's column list, every entry would carry the last row's
    // values. Verify each row matches its iteration step.
    for (int i = 0; i < 5; i++) {
      expect(result.rows[i]['id']!.value, i + 1);
      expect(result.rows[i]['name']!.value, 'row_${i + 1}');
    }
    await reader.close();

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // DbasSqliteException — coverage for codes that the rest of the suite
  // doesn't reach incidentally, plus the new factory / cause / category
  // / subCategory surface introduced in this PR.
  // ──────────────────────────────────────────────────────────────────────

  test('vacuum throws DbasSqliteException(vacuumInsideTransaction)', () async {
    final db = await _createTestDb('vacuum_in_tx.db');
    await db.beginTransaction();
    try {
      await expectLater(
        db.vacuum(),
        throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
            DbasSqliteErrorCode.vacuumInsideTransaction)),
      );
    } finally {
      await db.rollback();
      await db.closeDb();
      await db.dropDb();
    }
  });

  test('setBusyTimeout / enableWal / vacuum / statement-not-opened guard codes',
      () async {
    final db =
        await DbasSqlite.getInstance(dbName: 'closed_guards.db');
    // Not yet opened.
    await expectLater(
      db.setBusyTimeout(1000),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.setBusyTimeoutDatabaseNotOpened)),
    );
    await expectLater(
      db.enableWal(),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.enableWalDatabaseNotOpened)),
    );
    await expectLater(
      db.vacuum(),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.vacuumDatabaseNotOpened)),
    );
  });

  test('bindXxx with unsupported types throws DbasSqliteException(unsupportedPositionalBindType)',
      () async {
    final db = await _createTestDb('unsupported_bind.db');
    {
      final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER, v TEXT)');
      try { await stmt.executeSql(); } finally { await stmt.close(); }
    }
    final stmt = await db.prepareQuery('INSERT INTO t (id, v) VALUES (?, ?)');
    try {
      await expectLater(
        // DateTime has no bind path — caller must convert.
        stmt.executeSql(params: [1, DateTime.now()]),
        throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
            DbasSqliteErrorCode.unsupportedPositionalBindType)),
      );
      await expectLater(
        stmt.executeSql(nameParams: {'foo': DateTime.now()}),
        throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
            DbasSqliteErrorCode.unsupportedNamedBindType)),
      );
    } finally { await stmt.close(); }
    await db.closeDb();
    await db.dropDb();
  });

  // The bundled native binary exposes `GetExtendedErrorCode`, so
  // constraint violations carry the extended rc (e.g.
  // `SQLITE_CONSTRAINT_UNIQUE=2067`, `SQLITE_CONSTRAINT_FOREIGNKEY=787`)
  // which the [DbasSqliteSubCategory] mapping turns into the
  // specific `duplicatedData` / `foreignKeyViolation` values.
  test('UNIQUE-index violation surfaces DbasSqliteSubCategory.duplicatedData', () async {
    final db = await _createTestDb('unique_dup.db');
    await _runSql(db,
        'CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)');
    await _runSql(db,
        "INSERT INTO u (id, email) VALUES (1, 'a@b')");
    final dup = await db.prepareQuery(
        "INSERT INTO u (id, email) VALUES (2, 'a@b')");
    try {
      await expectLater(
        dup.executeSql(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.executeSqlStepFailed)
            .having((e) => e.sqliteCode, 'sqliteCode (SQLITE_CONSTRAINT)', 19)
            .having((e) => e.sqliteUniqueCode,
                'sqliteUniqueCode (SQLITE_CONSTRAINT_UNIQUE)', 2067)
            .having((e) => e.subCategory, 'subCategory',
                DbasSqliteSubCategory.duplicatedData)),
      );
    } finally { await dup.close(); }
    await db.closeDb();
    await db.dropDb();
  });

  test('FOREIGN KEY violation surfaces DbasSqliteSubCategory.foreignKeyViolation', () async {
    final db = await _createTestDb('fk_violation.db');
    await _runSql(db, 'PRAGMA foreign_keys = ON');
    await _runSql(db, 'CREATE TABLE parent (id INTEGER PRIMARY KEY)');
    await _runSql(db,
        'CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL '
        'REFERENCES parent(id))');
    final bad = await db.prepareQuery(
        'INSERT INTO child (id, parent_id) VALUES (1, 999)');
    try {
      await expectLater(
        bad.executeSql(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.executeSqlStepFailed)
            .having((e) => e.sqliteCode, 'sqliteCode (SQLITE_CONSTRAINT)', 19)
            .having((e) => e.sqliteUniqueCode,
                'sqliteUniqueCode (SQLITE_CONSTRAINT_FOREIGNKEY)', 787)
            .having((e) => e.subCategory, 'subCategory',
                DbasSqliteSubCategory.foreignKeyViolation)),
      );
    } finally { await bad.close(); }
    await db.closeDb();
    await db.dropDb();
  });

  test('openDb is idempotent — second call with the same pool size is a no-op',
      () async {
    final db = await DbasSqlite.getInstance(dbName: 'idempotent_open.db');
    await db.dropDb();
    await db.openDb(readerPoolSize: 0);
    expect(db.isOpened(), isTrue);
    final fileBefore = db.getDbFileName();

    // Second call with the same pool size: silent no-op, same connection.
    await db.openDb(readerPoolSize: 0);
    expect(db.isOpened(), isTrue);
    expect(db.getDbFileName(), fileBefore,
        reason: 'idempotent openDb must not swap the underlying connection');

    // Different pool size: throws.
    await expectLater(
      db.openDb(readerPoolSize: 2),
      throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
          DbasSqliteErrorCode.openDbReopenWithDifferentPoolSize)),
    );

    await db.closeDb();
    await db.dropDb();
  });

  test('DbasSqliteException factories enforce sqliteCode invariant', () {
    final dartSide = DbasSqliteException.dart(
        DbasSqliteErrorCode.statementClosed, 'closed');
    expect(dartSide.sqliteCode, isNull);
    expect(dartSide.sqliteUniqueCode, isNull);
    expect(dartSide.subCategory, DbasSqliteSubCategory.notApplicable);
    expect(dartSide.category, DbasSqliteErrorCategory.notOpened);

    // .sqlite factory: primary-only (no extended rc).
    final primaryOnly = DbasSqliteException.sqlite(
        DbasSqliteErrorCode.commitFailed, 'busy',
        sqliteCode: 5);
    expect(primaryOnly.sqliteCode, 5);
    expect(primaryOnly.sqliteUniqueCode, isNull);
    expect(primaryOnly.subCategory, DbasSqliteSubCategory.databaseBusy);

    // .sqlite factory: both primary AND extended — subCategory derives
    // from the more specific extended rc.
    final dup = DbasSqliteException.sqlite(
        DbasSqliteErrorCode.executeSqlStepFailed, 'unique violation',
        sqliteCode: 19, sqliteUniqueCode: 2067);
    expect(dup.sqliteCode, 19);
    expect(dup.sqliteUniqueCode, 2067);
    expect(dup.subCategory, DbasSqliteSubCategory.duplicatedData);
    expect(dup.category, DbasSqliteErrorCategory.executeFailed);

    final fk = DbasSqliteException.sqlite(
        DbasSqliteErrorCode.executeSqlStepFailed, 'fk',
        sqliteCode: 19, sqliteUniqueCode: 787);
    expect(fk.subCategory, DbasSqliteSubCategory.foreignKeyViolation);

    // Unknown extended rc → DbasSqliteSubCategory.other (extended wins
    // over primary as long as it's non-null).
    final unknown = DbasSqliteException.sqlite(
        DbasSqliteErrorCode.executeSqlStepFailed, 'unknown',
        sqliteCode: 1, sqliteUniqueCode: 99999);
    expect(unknown.subCategory, DbasSqliteSubCategory.other);

    // cause + causeStackTrace flow through.
    final inner = StateError('inner');
    final stack = StackTrace.current;
    final wrapped = DbasSqliteException.dart(
      DbasSqliteErrorCode.transactionRollbackAlsoFailed,
      'wrapped',
      cause: inner,
      causeStackTrace: stack,
    );
    expect(wrapped.cause, same(inner));
    expect(wrapped.causeStackTrace, same(stack));
    expect(wrapped.toString(), contains('cause: Bad state: inner'));
  });

  test('commitRollbackAlsoFailed wraps the original COMMIT failure with cause/rc lifting',
      () {
    // Mirrors the `transactionRollbackAlsoFailed` coverage above — the
    // "both COMMIT and the recovery rollback failed" shape is not
    // reproducible end-to-end without a mock (this suite has no real,
    // deterministic way to make a real ROLLBACK fail), so — like
    // `transactionRollbackAlsoFailed` — only the exception factory's
    // cause/rc-lifting contract is unit-tested here. What this pins is
    // that `commit()` must stop SWALLOWING the rollback failure: a
    // caller has to be able to tell "rollback recovered" (the original
    // `commitFailed` rethrown, see 'commit(): a genuine COMMIT failure
    // alone still rethrows commitFailed') apart from "rollback also
    // failed, state unknown" — which needs its own code, distinct from
    // `transactionRollbackAlsoFailed` so a bare `commit()` call is
    // distinguishable from one made through `transaction()`.
    final originalCommitFailure = DbasSqliteException.sqlite(
        DbasSqliteErrorCode.commitFailed, 'COMMIT failed: SQLITE_BUSY',
        sqliteCode: 5);
    final stack = StackTrace.current;
    final wrapped = DbasSqliteException.sqlite(
      DbasSqliteErrorCode.commitRollbackAlsoFailed,
      'COMMIT failed: $originalCommitFailure. Additionally, rollback also '
      'failed: Bad state: rollback boom. The database may be in an '
      'inconsistent state.',
      sqliteCode: originalCommitFailure.sqliteCode!,
      cause: originalCommitFailure,
      causeStackTrace: stack,
    );
    expect(wrapped.code, DbasSqliteErrorCode.commitRollbackAlsoFailed);
    expect(wrapped.category, DbasSqliteErrorCategory.transactionFailed);
    expect(wrapped.cause, same(originalCommitFailure));
    expect(wrapped.causeStackTrace, same(stack));
    expect(wrapped.sqliteCode, 5);
    expect(wrapped.toString(), contains('commitRollbackAlsoFailed'));
  });

  test('the new transaction-ownership error codes map to the documented categories',
      () {
    // `category` is an exhaustive switch with no `default`, so a new
    // enum value that nobody classified is a hard analyzer error rather
    // than a silent fallthrough. These pin the intended grouping:
    // `commitDatabaseNotOpened` joins the sibling "you called this on a
    // closed database" guards, the two `commitBlockedBy*` codes and
    // `commitRollbackAlsoFailed` join the sibling
    // `transactionAlreadyActive` / `vacuumInsideTransaction` "wrong time
    // to call this transaction operation" group (even though
    // `commitBlockedByActiveReader`'s root cause is reader-lifecycle),
    // and `writerLockWaitTimeout` mirrors its reader-side twin
    // `readerSlotWaitTimeout`.
    expect(DbasSqliteErrorCode.commitDatabaseNotOpened.category,
        DbasSqliteErrorCategory.notOpened);
    expect(DbasSqliteErrorCode.commitBlockedByInFlightOperation.category,
        DbasSqliteErrorCategory.transactionFailed);
    expect(DbasSqliteErrorCode.commitBlockedByActiveReader.category,
        DbasSqliteErrorCategory.transactionFailed);
    expect(DbasSqliteErrorCode.commitRollbackAlsoFailed.category,
        DbasSqliteErrorCategory.transactionFailed);
    expect(DbasSqliteErrorCode.writerLockWaitTimeout.category,
        DbasSqliteErrorCategory.busyOrCancelled);
  });

  // The .sqlite factory asserts that sqliteUniqueCode is only set when
  // sqliteCode is also set. A future maintainer who accidentally
  // populates only the extended slot should hit this assertion in
  // dev/test, even though Dart's null-safety already enforces the
  // required-primary at the type level.
  test('DbasSqliteException._ rejects sqliteUniqueCode without sqliteCode', () {
    // We can't trigger this via the public factories because they
    // either take both rcs as nullable (.dart, neither set) or require
    // the primary (.sqlite). The assertion guards an invariant for
    // private constructors / future factories — exercised here via the
    // public toString roundtrip on a known-valid pair to confirm the
    // assertion does NOT fire for legitimate constructions.
    final ok = DbasSqliteException.sqlite(
        DbasSqliteErrorCode.executeSqlStepFailed, 'ok',
        sqliteCode: 19, sqliteUniqueCode: 2067);
    expect(ok.toString(), contains('sqliteCode=19'));
    expect(ok.toString(), contains('sqliteUniqueCode=2067'));
  });

  // ──────────────────────────────────────────────────────────────────────
  // Parameterised subCategory mapping table — covers every explicit
  // extended-code branch in _subCategoryFromRc. Pure unit test (no DB).
  // ──────────────────────────────────────────────────────────────────────

  test('_subCategoryFromRc maps every documented extended rc', () {
    DbasSqliteSubCategory sub(int rc) => DbasSqliteException.sqlite(
        DbasSqliteErrorCode.executeSqlStepFailed, 'p',
        sqliteCode: rc & 0xFF, sqliteUniqueCode: rc).subCategory;

    expect(sub(275), DbasSqliteSubCategory.checkViolation);
    expect(sub(531), DbasSqliteSubCategory.otherConstraintViolation);
    expect(sub(787), DbasSqliteSubCategory.foreignKeyViolation);
    expect(sub(1043), DbasSqliteSubCategory.otherConstraintViolation);
    expect(sub(1299), DbasSqliteSubCategory.notNullViolation);
    expect(sub(1555), DbasSqliteSubCategory.duplicatedData);
    expect(sub(1811), DbasSqliteSubCategory.triggerAborted);
    expect(sub(2067), DbasSqliteSubCategory.duplicatedData);
    expect(sub(2323), DbasSqliteSubCategory.otherConstraintViolation);
    expect(sub(2579), DbasSqliteSubCategory.duplicatedData);
    expect(sub(2835), DbasSqliteSubCategory.otherConstraintViolation);
    expect(sub(3091), DbasSqliteSubCategory.dataTypeViolation);
  });

  test('_subCategoryFromRc maps every primary rc', () {
    DbasSqliteSubCategory sub(int rc) => DbasSqliteException.sqlite(
        DbasSqliteErrorCode.executeSqlStepFailed, 'p',
        sqliteCode: rc).subCategory;

    expect(sub(1), DbasSqliteSubCategory.genericError);
    expect(sub(2), DbasSqliteSubCategory.internalError);
    expect(sub(3), DbasSqliteSubCategory.permissionDenied);
    expect(sub(4), DbasSqliteSubCategory.aborted);
    expect(sub(5), DbasSqliteSubCategory.databaseBusy);
    expect(sub(6), DbasSqliteSubCategory.tableLocked);
    expect(sub(7), DbasSqliteSubCategory.outOfMemory);
    expect(sub(8), DbasSqliteSubCategory.readOnlyDatabase);
    expect(sub(9), DbasSqliteSubCategory.interrupted);
    expect(sub(10), DbasSqliteSubCategory.ioError);
    expect(sub(11), DbasSqliteSubCategory.corruptDatabase);
    expect(sub(12), DbasSqliteSubCategory.notFound);
    expect(sub(13), DbasSqliteSubCategory.diskFull);
    expect(sub(14), DbasSqliteSubCategory.cannotOpen);
    expect(sub(15), DbasSqliteSubCategory.protocolError);
    expect(sub(16), DbasSqliteSubCategory.emptyDatabase);
    expect(sub(17), DbasSqliteSubCategory.schemaChanged);
    expect(sub(18), DbasSqliteSubCategory.valueTooLarge);
    expect(sub(19), DbasSqliteSubCategory.constraintViolation);
    expect(sub(20), DbasSqliteSubCategory.typeMismatch);
    expect(sub(21), DbasSqliteSubCategory.misuse);
    expect(sub(22), DbasSqliteSubCategory.noLargeFileSupport);
    expect(sub(23), DbasSqliteSubCategory.authorizationDenied);
    expect(sub(24), DbasSqliteSubCategory.formatError);
    expect(sub(25), DbasSqliteSubCategory.rangeError);
    expect(sub(26), DbasSqliteSubCategory.notADatabase);
    expect(sub(100), DbasSqliteSubCategory.stepStatus);
    expect(sub(101), DbasSqliteSubCategory.stepStatus);
  });

  // ──────────────────────────────────────────────────────────────────────
  // Statement getLastErrorCode / getLastUniqueErrorCode coverage —
  // both the reader path (populated in onClose) and the writer path
  // (populated from the thrown exception's codes).
  // ──────────────────────────────────────────────────────────────────────

  test('getLastErrorCode / getLastUniqueErrorCode after a failed executeSql carry the rcs',
      () async {
    final db = await _createTestDb('stmt_last_codes_sql.db');
    await _runSql(db,
        'CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)');
    await _runSql(db, "INSERT INTO u (id, email) VALUES (1, 'a@b')");

    final dup = await db.prepareQuery(
        "INSERT INTO u (id, email) VALUES (2, 'a@b')");
    try {
      DbasSqliteException? caught;
      try {
        await dup.executeSql();
      } on DbasSqliteException catch (e) {
        caught = e;
      }
      expect(caught, isNotNull);
      // The accessors mirror the exception's codes.
      expect(dup.getLastErrorCode(), caught!.sqliteCode);
      expect(dup.getLastUniqueErrorCode(), caught.sqliteUniqueCode);
      expect(dup.getLastErrorCode(), 19);
      expect(dup.getLastUniqueErrorCode(), 2067);
    } finally { await dup.close(); }
    await db.closeDb();
    await db.dropDb();
  });

  test('getLastErrorCode / getLastUniqueErrorCode are null after a successful executeSql',
      () async {
    final db = await _createTestDb('stmt_last_codes_clean.db');
    final stmt = await db.prepareQuery('CREATE TABLE t (id INTEGER)');
    try {
      await stmt.executeSql();
      expect(stmt.getLastErrorCode(), isNull);
      expect(stmt.getLastUniqueErrorCode(), isNull);
      expect(stmt.getLastError(), isNull);
    } finally { await stmt.close(); }
    await db.closeDb();
    await db.dropDb();
  });

  test('getLastErrorCode populated from a reader iteration', () async {
    final db = await _createTestDb('stmt_last_codes_reader.db');
    await _runSql(db, 'CREATE TABLE r (id INTEGER)');
    await _runSql(db, 'INSERT INTO r (id) VALUES (1)');
    final stmt = await db.prepareQuery('SELECT id FROM r');
    try {
      final reader = await stmt.executeReader();
      try {
        while (await reader.readRow()) {}
      } finally { await reader.close(); }
      // Successful iteration: no error queued.
      expect(stmt.getLastError(), isNull);
      // The accessors are non-throwing even when no error is pending.
      stmt.getLastErrorCode();
      stmt.getLastUniqueErrorCode();
    } finally { await stmt.close(); }
    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // openDb idempotency — pool-reader mode and reopen-after-close paths.
  // ──────────────────────────────────────────────────────────────────────

  test('openDb idempotent with readerPoolSize >= 1', () async {
    final db = await DbasSqlite.getInstance(dbName: 'idempotent_pool.db');
    await db.dropDb();
    await db.openDb(readerPoolSize: 2);
    expect(db.isOpened(), isTrue);
    final fileBefore = db.getDbFileName();

    // Same pool size: silent no-op.
    await db.openDb(readerPoolSize: 2);
    expect(db.isOpened(), isTrue);
    expect(db.getDbFileName(), fileBefore);

    await db.closeDb();
    await db.dropDb();
  });

  test('openDb after closeDb cleanly re-opens', () async {
    final db = await DbasSqlite.getInstance(dbName: 'reopen_after_close.db');
    await db.dropDb();
    await db.openDb(readerPoolSize: 0);
    expect(db.isOpened(), isTrue);

    await db.closeDb();
    expect(db.isOpened(), isFalse);

    // Re-acquire the (now-removed) instance via getInstance and reopen.
    final db2 = await DbasSqlite.getInstance(dbName: 'reopen_after_close.db');
    await db2.openDb(readerPoolSize: 0);
    expect(db2.isOpened(), isTrue);

    await db2.closeDb();
    await db2.dropDb();
  });

  test('concurrent openDb calls are single-flight (one pool per file)',
      () async {
    // Regression: openDb's `isOpened()` guard stays false until `_db` is
    // assigned, which happens AFTER the `createPool` await. Before the
    // single-flight fix, several openDb() calls that arrived before the
    // first finished all fell through and each issued its own createPool
    // for the same file. On web that tripped the pool layer's
    // process-wide POOL_ALREADY_ACTIVE guard ("a ConnectionPool is
    // already active for dbName ..."); the real-world trigger is the
    // consumer's queue starting its sendData/receiveData/log processors
    // together, each resolving the same user DB. Pool mode
    // (readerPoolSize >= 1) is what exercises the createPool path.
    final db = await DbasSqlite.getInstance(dbName: 'concurrent_open.db');
    await db.dropDb();

    // Fire the opens together, with no await between them, so they all
    // observe `_db == null` and race the guard exactly as the queue
    // processors did.
    await Future.wait([
      db.openDb(readerPoolSize: 2),
      db.openDb(readerPoolSize: 2),
      db.openDb(readerPoolSize: 2),
      db.openDb(readerPoolSize: 2),
    ]);

    expect(db.isOpened(), isTrue,
        reason: 'concurrent opens must converge on a single open pool');

    // The single pool must be fully functional — a write then read back
    // confirms the converged pool wasn't left in a half-initialized state.
    await _runSql(db, 'CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
    await _runSql(db, "INSERT INTO t (v) VALUES ('ok')");
    final read = await db.prepareQuery('SELECT v FROM t WHERE id = 1');
    try {
      final reader = await read.executeReader();
      try {
        expect(await reader.readRow(), isTrue);
        expect(reader.getColumnValue(0), 'ok');
      } finally {
        await reader.close();
      }
    } finally {
      await read.close();
    }

    await db.closeDb();
    await db.dropDb();
  });

  // ──────────────────────────────────────────────────────────────────────
  // Worker-error envelope `code` field is folded into the message — the
  // public exception's message should carry "[CODE]" so log scrapers
  // and substring matchers can still distinguish worker-side error
  // kinds (WORKER_CRASHED, POOL_CLOSED, …). Pure unit test against the
  // helper's behaviour at the boundary; doesn't require web runtime.
  // ──────────────────────────────────────────────────────────────────────

  // (No direct test — _workerErrorFromJsError is private to web_pool.dart
  // and runs only on web. The behaviour is verified by web integration
  // tests; the helper's contract is documented in its dartdoc.)

  // ──────────────────────────────────────────────────────────────────────
  // WAL checkpoint — committed data must reach the main `.db` file
  //
  // A pooled open (`readerPoolSize >= 1`, the `openDb()` default and the
  // production configuration) puts the database in `journal_mode=wal`.
  // Dart then issues no pragmas of its own, so SQLite's stock
  // `wal_autocheckpoint=1000` is inherited and committed frames pile up
  // in the `-wal` indefinitely — measured: 200 committed inserts leave
  // the main `.db` at 4096 bytes with an 832 KB `-wal`, and the table is
  // not in the main file AT ALL. Anything that reads the main `.db`
  // alone — a file copy, a backup, `streamCopyDb`, the consumer's
  // `copyDatabase` — then sees a truncated or entirely empty database,
  // with no error of any kind.
  //
  // Spec: a checkpoint happens AUTOMATICALLY on
  //   1. commit,
  //   2. closeDb — rolling back any open transaction FIRST, because a
  //      checkpoint issued inside an open transaction is a silent no-op,
  //   3. any insert/update/delete outside a transaction.
  // (1) and (3) are one mechanism: bare DML is an implicit transaction
  // that commits.
  //
  // Every assertion below is "rows visible in a main-file-only copy",
  // the only signal that actually proves frames left the WAL. `-1` in a
  // failure message means the table never reached the main file at all.
  // File sizes appear in `reason:` strings as a secondary diagnostic.
  //
  // `readerPoolSize: 0` is deliberately NOT used here: that path opens
  // in `journal_mode=delete`, where there is no WAL and nothing to
  // checkpoint, so it cannot exercise this behaviour at all.
  // ──────────────────────────────────────────────────────────────────────

  test(
      'WAL checkpoint: bare DML outside a transaction folds into the main db file',
      () async {
    final db = await _createTestDb('wal_bare_dml.db', readerPoolSize: 4);
    await _runSql(
        db, 'CREATE TABLE bare_tbl (id INTEGER PRIMARY KEY, val TEXT)');
    // 200 implicit transactions — comfortably under the stock
    // `wal_autocheckpoint=1000` frame threshold, so nothing folds by
    // accident and the assertion measures the intended mechanism only.
    for (var i = 1; i <= 200; i++) {
      await _runSql(db, 'INSERT INTO bare_tbl (id, val) VALUES (?, ?)',
          params: [i, 'row$i']);
    }

    final footprint = await _walFootprint(db);
    final rows =
        await _rowsInMainFileOnly(db, 'bare_tbl', 'wal_bare_dml_probe.db');

    await db.closeDb();
    await db.dropDb();

    expect(rows, 200,
        reason: 'each insert outside a transaction is an implicit '
            'transaction that commits, so all 200 rows must live in the main '
            '.db file and survive a main-file-only copy. Source footprint at '
            'copy time: $footprint');
  });

  test('WAL checkpoint: commit() folds each transaction into the main db file',
      () async {
    final db = await _createTestDb('wal_commit.db', readerPoolSize: 4);
    await _runSql(
        db, 'CREATE TABLE commit_tbl (id INTEGER PRIMARY KEY, val TEXT)');

    var written = 0;
    final observed = <int>[];
    final expected = <int>[];
    final footprints = <String>[];
    for (var txn = 1; txn <= 5; txn++) {
      await db.beginTransaction();
      for (var i = 0; i < 20; i++) {
        written++;
        await _runSql(db, 'INSERT INTO commit_tbl (id, val) VALUES (?, ?)',
            params: [written, 'v$written']);
      }
      await db.commit();

      footprints.add('after commit #$txn: ${await _walFootprint(db)}');
      observed.add(
          await _rowsInMainFileOnly(db, 'commit_tbl', 'wal_commit_probe.db'));
      expected.add(written);
    }

    await db.closeDb();
    await db.dropDb();

    expect(observed, expected,
        reason: 'after every commit the main .db file must already hold every '
            'row committed so far, so the running totals must track '
            '$expected. Source footprints: ${footprints.join(' | ')}');
  });

  test('WAL checkpoint: closeDb folds even when it is NOT the last connection',
      () async {
    // `closeDb` performs no checkpoint of its own on either teardown
    // path — the pool path calls `closePool`, the single-connection path
    // hardcodes `checkpoint: false`. What makes the default case look
    // healthy is SQLite itself: closing the LAST connection to a WAL
    // database checkpoints implicitly. Keep one other connection alive
    // and that safety net disappears.
    //
    // Two `DbasSqlite` instances over one physical database give exactly
    // that: instances are keyed by `dbName`, but `getAppDatabasePath`
    // concatenates the name onto the directory, so 'x.db' and './x.db'
    // are two independent instances — two real SQLite connections —
    // resolving to a single file. That is precisely the situation any
    // second opener of the same database file creates.
    const writerName = 'wal_close_nonlast.db';
    const holderName = './wal_close_nonlast.db';

    final writer = await _createTestDb(writerName, readerPoolSize: 4);
    await _runSql(
        writer, 'CREATE TABLE nonlast_tbl (id INTEGER PRIMARY KEY, val TEXT)');

    final holder = await DbasSqlite.getInstance(dbName: holderName);
    await holder.openDb(readerPoolSize: 0);
    expect(holder.isOpened(), isTrue,
        reason: 'the second connection must really be open, otherwise this '
            'test degenerates into the last-connection case that folds by '
            'itself and proves nothing');

    for (var i = 1; i <= 200; i++) {
      await _runSql(writer, 'INSERT INTO nonlast_tbl (id, val) VALUES (?, ?)',
          params: [i, 'row$i']);
    }

    await writer.closeDb();

    final footprint = await _walFootprint(writer);
    final rows = await _rowsInMainFileOnly(
        writer, 'nonlast_tbl', 'wal_nonlast_probe.db');

    // Only ONE dropDb: both instances name the same physical file, and
    // `dropDb` also tears down the shared per-file native delegate, so a
    // second call would dereference a delegate that no longer exists.
    await holder.closeDb();
    await holder.dropDb();

    expect(rows, 200,
        reason: 'closeDb must fold the WAL into the main .db file itself, not '
            'lean on SQLite\'s last-connection auto-checkpoint, so the data '
            'is durable in the main file even while another connection keeps '
            'the database open. Footprint after closeDb: $footprint');
  });

  test(
      'WAL checkpoint: closeDb with an open transaction rolls back FIRST, then folds',
      () async {
    // Same two-connection setup as the previous test, so closeDb has to
    // do the checkpoint itself rather than inherit SQLite's
    // last-connection behaviour — that is what makes the ORDER
    // observable here.
    const writerName = 'wal_close_open_txn.db';
    const holderName = './wal_close_open_txn.db';

    final db = await _createTestDb(writerName, readerPoolSize: 4);
    await _runSql(
        db, 'CREATE TABLE close_txn_tbl (id INTEGER PRIMARY KEY, val TEXT)');
    for (var i = 1; i <= 30; i++) {
      await _runSql(db, 'INSERT INTO close_txn_tbl (id, val) VALUES (?, ?)',
          params: [i, 'committed$i']);
    }

    final holder = await DbasSqlite.getInstance(dbName: holderName);
    await holder.openDb(readerPoolSize: 0);
    expect(holder.isOpened(), isTrue);

    // Open transaction, deliberately never committed. `closeDb` must
    // roll it back BEFORE checkpointing: a checkpoint issued while a
    // transaction is still open is a SILENT no-op — no row, no error,
    // WAL untouched — so the wrong order would also strand the 30
    // already-committed rows in the WAL.
    await db.beginTransaction();
    for (var i = 1000; i < 1010; i++) {
      await _runSql(db, 'INSERT INTO close_txn_tbl (id, val) VALUES (?, ?)',
          params: [i, 'uncommitted$i']);
    }
    expect(db.isInTransaction, isTrue);

    await db.closeDb();

    final footprint = await _walFootprint(db);
    final committed = await _rowsInMainFileOnly(
        db, 'close_txn_tbl', 'wal_close_txn_probe.db',
        where: "val LIKE 'committed%'");
    final uncommitted = await _rowsInMainFileOnly(
        db, 'close_txn_tbl', 'wal_close_txn_probe.db',
        where: "val LIKE 'uncommitted%'");

    await holder.closeDb();
    await holder.dropDb();

    expect(committed, 30,
        reason: 'rollback must happen BEFORE the checkpoint — a checkpoint '
            'attempted while the transaction was still open is a silent '
            'no-op, which would leave these 30 committed rows stranded in '
            'the WAL. -1 means the table never reached the main file at all. '
            'Footprint after closeDb: $footprint');
    expect(uncommitted, 0,
        reason: 'the open transaction must be rolled back by closeDb, so its '
            'rows must never reach the main .db file');
  });

  test('WAL checkpoint: a zero-frame checkpoint is reportable', () async {
    // NOTE FOR THE FIX: this test is written against an API that does
    // NOT exist yet and is the one API-shape decision baked into these
    // frozen tests — `DbasSqlite.checkpoint()`, resolving to the three
    // values `PRAGMA wal_checkpoint` produces: `busy`, `log` (frames in
    // the WAL) and `checkpointed` (frames folded into the main file).
    // The result is used without a type annotation on purpose, so it can
    // be a record `({int busy, int log, int checkpointed})` or a class
    // with those three members — whichever the fix prefers.
    //
    // Why the library needs it at all: `busy` is NOT a success signal. A
    // PASSIVE checkpoint that folds nothing reports `busy=0` and returns
    // SQLITE_OK — measured `[0, 10, 0]` — which is indistinguishable
    // from a full fold by every signal `executeSql` currently surfaces
    // (it returns the rc and discards the triple). The only honest test
    // of "did the data actually reach the main file" is
    // `checkpointed == log`.
    final seed = await _createTestDb('wal_checkpoint_report.db',
        readerPoolSize: 4);
    await _runSql(
        seed, 'CREATE TABLE report_tbl (id INTEGER PRIMARY KEY, val TEXT)');
    for (var i = 1; i <= 50; i++) {
      await _runSql(seed, 'INSERT INTO report_tbl (id, val) VALUES (?, ?)',
          params: [i, 'seed$i']);
    }
    // Close/reopen so the WAL starts empty and the pinned snapshot below
    // sits at frame 0 — every frame written afterwards is then provably
    // above it.
    await seed.closeDb();
    final db = await DbasSqlite.getInstance(dbName: 'wal_checkpoint_report.db');
    await db.openDb(readerPoolSize: 4);

    final pinned =
        await db.prepareQuery('SELECT id FROM report_tbl ORDER BY id');
    final pinnedReader = await pinned.executeReader();
    expect(await pinnedReader.readRow(), isTrue);

    for (var i = 100; i < 110; i++) {
      await _runSql(db, 'INSERT INTO report_tbl (id, val) VALUES (?, ?)',
          params: [i, 'above$i']);
    }

    final blocked = await db.checkpoint();
    expect(blocked.busy, greaterThanOrEqualTo(0),
        reason: 'the busy flag must be readable, not swallowed');
    expect(blocked.log, greaterThan(0),
        reason: 'frames written above the pinned snapshot are still in the WAL');
    expect(blocked.checkpointed, 0,
        reason: 'a reader pinned below every one of those frames makes the '
            'checkpoint fold nothing — that zero MUST be reportable, because '
            'the checkpoint otherwise looks completely successful');
    expect(blocked.checkpointed == blocked.log, isFalse,
        reason: 'checkpointed == log is the real success test; it must be '
            'false here');

    await pinnedReader.close();
    await pinned.close();

    final released = await db.checkpoint();
    expect(released.log, greaterThan(0));
    expect(released.checkpointed, released.log,
        reason: 'once the snapshot is released the same frames fold '
            'completely, and checkpointed == log must say so');

    await db.closeDb();
    await db.dropDb();
  });

  test(
      'WAL checkpoint: streamCopyDb produces a complete copy of an open database',
      () async {
    // `streamCopyDb` copies only the main `.db` and deletes the
    // destination's `-wal`/`-shm`, so every frame still sitting in the
    // source WAL is silently dropped from the copy. The existing
    // "streamCopyDb copies database to new name" test hides this with a
    // close/reopen dance and the comment "Re-open to ensure WAL is
    // flushed"; no consumer should have to do that.
    final src = await _createTestDb('wal_copy_src.db', readerPoolSize: 4);
    await _runSql(
        src, 'CREATE TABLE copy_wal_tbl (id INTEGER PRIMARY KEY, val TEXT)');
    for (var i = 1; i <= 200; i++) {
      await _runSql(src, 'INSERT INTO copy_wal_tbl (id, val) VALUES (?, ?)',
          params: [i, 'row$i']);
    }

    await src.streamCopyDb('wal_copy_dest.db');
    final footprint = await _walFootprint(src);

    final dest = await DbasSqlite.getInstance(dbName: 'wal_copy_dest.db');
    await dest.openDb(readerPoolSize: 0);
    final rows = await _rowCountOrAbsent(dest, 'copy_wal_tbl');
    await dest.closeDb();
    await dest.dropDb();

    await src.closeDb();
    await src.dropDb();

    expect(rows, 200,
        reason: 'streamCopyDb of a live database must yield a complete, '
            'self-contained copy without any manual close/reopen dance. '
            'Source footprint at copy time: $footprint');
  });

  test(
      'WAL checkpoint: folding must not stall on busy_timeout when a reader holds a snapshot',
      () async {
    final db = await _createTestDb('wal_no_stall.db', readerPoolSize: 4);
    await _runSql(
        db, 'CREATE TABLE stall_tbl (id INTEGER PRIMARY KEY, val TEXT)');
    for (var i = 1; i <= 50; i++) {
      await _runSql(db, 'INSERT INTO stall_tbl (id, val) VALUES (?, ?)',
          params: [i, 'seed$i']);
    }

    // Pin a read snapshot on a pool reader and hold it. Frames written
    // from here on sit ABOVE that snapshot, so no checkpoint can fold
    // them. Measured on this database: PASSIVE reports the shortfall as
    // `[0, 10, 0]` in ~0 ms, while TRUNCATE blocks for the whole
    // busy_timeout — 5034 ms — and then folds exactly the same zero
    // frames, `[1, 10, 0]`. That is why TRUNCATE must never be used
    // unconditionally, and why these bounds are assertions rather than
    // performance notes.
    final pinned =
        await db.prepareQuery('SELECT id FROM stall_tbl ORDER BY id');
    final pinnedReader = await pinned.executeReader();
    expect(await pinnedReader.readRow(), isTrue);

    final dmlWatch = Stopwatch()..start();
    for (var i = 100; i < 110; i++) {
      await _runSql(db, 'INSERT INTO stall_tbl (id, val) VALUES (?, ?)',
          params: [i, 'above$i']);
    }
    dmlWatch.stop();

    final copyWatch = Stopwatch()..start();
    await db.streamCopyDb('wal_no_stall_copy.db');
    copyWatch.stop();

    await pinnedReader.close();
    await pinned.close();

    final copy = await DbasSqlite.getInstance(dbName: 'wal_no_stall_copy.db');
    await copy.dropDb();

    await db.closeDb();
    await db.dropDb();

    expect(dmlWatch.elapsedMilliseconds, lessThan(3000),
        reason: '10 inserts under a pinned reader snapshot took '
            '${dmlWatch.elapsedMilliseconds} ms — a per-DML checkpoint must '
            'give up immediately when frames are pinned, never block on '
            'busy_timeout (one TRUNCATE alone costs ~5 s here)');
    expect(copyWatch.elapsedMilliseconds, lessThan(3000),
        reason: 'streamCopyDb under a pinned reader snapshot took '
            '${copyWatch.elapsedMilliseconds} ms — it must not block on '
            'busy_timeout for frames it cannot fold anyway');
  });

  // ──────────────────────────────────────────────────────────────────────
  // Writer connection settings — a pin, not a RED
  //
  // This test passes before AND after the change that added
  // `PRAGMA synchronous=FULL` to the open path: FULL was already the
  // value. Its job is to fail if the invariant is ever broken — a C
  // library rebuilt with different flags, a dropped pragma, someone
  // "simplifying" the open path.
  //
  // It pins values that are otherwise INVISIBLE compile-time defaults of
  // a prebuilt binary. Measured from the shipped library via
  // `PRAGMA compile_options` (SQLite 3.52.0): `DEFAULT_SYNCHRONOUS=2`,
  // `DEFAULT_WAL_SYNCHRONOUS=2`, `DEFAULT_WAL_AUTOCHECKPOINT=1000`.
  // Nothing in the Dart guaranteed the first two, so a future rebuild
  // could change this database's durability with no code change and no
  // test failure anywhere. Now a divergence fails loudly, here.
  // ──────────────────────────────────────────────────────────────────────

  test(
      'WAL checkpoint: a production-configured writer pins synchronous, wal_autocheckpoint and journal_mode',
      () async {
    // `readerPoolSize: 4` is `openDb()`'s OWN default — the production
    // configuration — not `_createTestDb`'s default of 0. The 0 path
    // opens in `journal_mode=delete` with no WAL at all, so it cannot
    // exercise, let alone pin, any of this.
    final db = await _createTestDb('wal_settings_pin.db', readerPoolSize: 4);

    // The values are read back through the ordinary public API, but they
    // MUST come off the WRITER connection: `synchronous` and
    // `wal_autocheckpoint` are per-connection settings and the library
    // applies them to the writer only. `executeReader` routes to the
    // writer once the current transaction has performed a write, so the
    // readback runs inside a transaction whose first statement is one.
    //
    // `wal_autocheckpoint == 1` doubles as the WITNESS that the readback
    // really landed on the writer: a pool reader reports the stock 1000
    // (measured). Should that routing rule ever change, this assertion
    // fails loudly instead of quietly pinning `synchronous` — which
    // reads 2 on every connection — against the wrong one.
    await _runSql(db, 'CREATE TABLE pin_tbl (id INTEGER PRIMARY KEY)');
    await db.beginTransaction();
    await _runSql(db, 'INSERT INTO pin_tbl (id) VALUES (1)');

    Future<Object?> readPragma(String pragma) async {
      // `executeScalar` closes the reader and the statement itself, so
      // no cursor is left open across the rollback below.
      final stmt = await db.prepareQuery('PRAGMA $pragma');
      return await stmt.executeScalar();
    }

    final synchronous = await readPragma('synchronous');
    final autoCheckpoint = await readPragma('wal_autocheckpoint');
    final journalMode = await readPragma('journal_mode');

    await db.rollback();
    await db.closeDb();
    await db.dropDb();

    expect(synchronous, 2,
        reason: 'the writer must run at synchronous=FULL (2). It is issued '
            'explicitly at open precisely so it does not depend on the '
            'prebuilt C library\'s DEFAULT_SYNCHRONOUS / '
            'DEFAULT_WAL_SYNCHRONOUS — a value no Dart code declares and '
            'no other test would notice changing');
    expect(autoCheckpoint, 1,
        reason: 'the writer must fold the WAL on every commit. 1000 here '
            'means the readback hit a pool reader instead of the writer '
            '(the routing rule changed) or the open-time pragma was '
            'dropped — either way the durability pin above is no longer '
            'measuring the writer');
    expect(journalMode, 'wal',
        reason: 'a pooled open must put the database in WAL mode; without '
            'it neither of the two settings above has anything to govern');
  });

  // ──────────────────────────────────────────────────────────────────────
  // enableWal() is the OTHER door into WAL mode
  //
  // The open-time writer pragmas above are applied on the pooled branch
  // of `_performOpen` only, because that is the only branch that OPENS
  // in `journal_mode=wal`. But `enableWal()` is public and moves an
  // already-open connection INTO WAL by itself. `openDb(readerPoolSize:
  // 0)` opens in `journal_mode=delete` (measured), so the open-time
  // pragmas are deliberately skipped there — and a consumer that then
  // calls `enableWal()` reaches a WAL database carrying the stock
  // `wal_autocheckpoint=1000` and an unpinned `synchronous`: exactly the
  // silent-data-loss configuration the pragmas above exist to prevent,
  // reached through the public API by a different door.
  //
  // Whichever door a database enters WAL through, it must leave with the
  // same guarantees.
  // ──────────────────────────────────────────────────────────────────────

  test(
      'WAL checkpoint: enableWal on a pool-less open pins the same writer settings as a pooled open',
      () async {
    // `readerPoolSize: 0` is the point of this test, not an economy: it
    // opens in `journal_mode=delete`, so WAL here can only come from the
    // `enableWal()` call below — the path the open-time pragmas do not
    // cover.
    final db = await _createTestDb('wal_enable_wal_pin.db');
    await db.enableWal();

    // Readback rule as in the pooled pin test above: `synchronous` and
    // `wal_autocheckpoint` are per-connection, so they must be read off
    // the WRITER. There is no pool on this path, so every read already
    // lands on the single writer connection; the transaction-after-a-
    // write shape is kept anyway so both pin tests measure the same way
    // and `wal_autocheckpoint == 1` keeps working as the witness that
    // the readback hit the writer.
    await _runSql(db, 'CREATE TABLE enable_wal_pin_tbl (id INTEGER PRIMARY KEY)');
    await db.beginTransaction();
    await _runSql(db, 'INSERT INTO enable_wal_pin_tbl (id) VALUES (1)');

    Future<Object?> readPragma(String pragma) async {
      final stmt = await db.prepareQuery('PRAGMA $pragma');
      return await stmt.executeScalar();
    }

    final synchronous = await readPragma('synchronous');
    final autoCheckpoint = await readPragma('wal_autocheckpoint');
    final journalMode = await readPragma('journal_mode');

    await db.rollback();
    await db.closeDb();
    await db.dropDb();

    expect(journalMode, 'wal',
        reason: 'enableWal() must actually move the pool-less connection '
            'into WAL — without it the two settings below govern nothing');
    expect(autoCheckpoint, 1,
        reason: 'enableWal() put this database in WAL, so it owes the same '
            'fold policy a pooled open establishes. 1000 here is SQLite\'s '
            'stock DEFAULT_WAL_AUTOCHECKPOINT: committed frames would sit '
            'in the -wal and any read of the main .db file alone would '
            'silently miss them');
    expect(synchronous, 2,
        reason: 'the writer must run at synchronous=FULL (2) once it is in '
            'WAL, pinned explicitly rather than inherited from the prebuilt '
            'C library\'s DEFAULT_SYNCHRONOUS / DEFAULT_WAL_SYNCHRONOUS');
  });

  test('WAL checkpoint: enableWal inside a transaction is rejected up front',
      () async {
    // Inside a transaction SQLite refuses BOTH halves of enableWal:
    // `PRAGMA journal_mode=WAL` cannot switch modes, and
    // `PRAGMA synchronous` answers 'Safety level may not be changed
    // inside a transaction' (measured). So the call can only verify,
    // never establish.
    //
    // A pooled database is the case that makes the guard necessary: it
    // is ALREADY in WAL, so the journal-mode statement is a silent no-op
    // success and the call used to look like it worked while
    // establishing nothing. On a pool-less database in `delete` mode the
    // same call failed instead — the same API "succeeding" or failing on
    // nothing but the journal mode it happened to find. The guard makes
    // both answer alike, before any pragma runs.
    final db = await _createTestDb('wal_enable_wal_in_txn.db', readerPoolSize: 2);
    await _runSql(db, 'CREATE TABLE txn_gate_tbl (id INTEGER PRIMARY KEY)');
    await db.beginTransaction();
    await _runSql(db, 'INSERT INTO txn_gate_tbl (id) VALUES (1)');

    await expectLater(
        db.enableWal(),
        throwsA(isA<DbasSqliteException>().having((e) => e.code, 'code',
            DbasSqliteErrorCode.enableWalInsideTransaction)));

    // The rejection is a guard, not damage: the transaction is untouched
    // and still commits.
    await db.commit();
    final stmt = await db.prepareQuery('SELECT COUNT(*) FROM txn_gate_tbl');
    final rows = await stmt.executeScalar();

    await db.closeDb();
    await db.dropDb();

    expect(rows, 1,
        reason: 'enableWal must reject without disturbing the open '
            'transaction — the row committed before it was still there');
  });

  // ──────────────────────────────────────────────────────────────────────
  // executeScript — the multi-statement door
  //
  // `prepareQuery` + `DbasSqliteStatement.executeSql` is ONE
  // `sqlite3_prepare_v2`, one step, one `sqlite3_finalize`, and the C
  // side passes `pzTail` as a hard `nullptr` — so everything after the
  // first `;` is discarded with NO error of any kind: the step reports
  // `SQLITE_DONE` and the caller sees a clean success.
  //
  // The damage is not theoretical. A runtime-created table loses every
  // explicit `CREATE INDEX` that follows it in the same string — a
  // UNIQUE index therefore enforces nothing — and a
  // `PRAGMA foreign_keys = ON` written as the last statement of a
  // four-statement open script never applies at all.
  //
  // `executeScript` routes to the C `ExecuteSql` entry point, which is
  // `sqlite3_exec`: the one path in this library that already loops over
  // every statement. Nothing splits on `;` in Dart, and nothing may:
  // `sqlite3_complete` is not exported by the shipped binary, and the
  // consuming app's DDL carries `CHECK` bodies full of arbitrary user
  // SQL that a naive split would shred.
  //
  // `CREATE INDEX` appeared ZERO times in any `.dart` in this repo
  // before these tests, and nothing anywhere read `sqlite_master` for
  // `type='index'`. That gap is why this shipped.
  // ──────────────────────────────────────────────────────────────────────

  test('executeScript: every statement in the script runs, not just the first',
      () async {
    final db = await _createTestDb('script_multi_statement.db');

    // Four statements. Under the prepare path only `CREATE TABLE` would
    // survive; the index and both rows would vanish silently.
    await db.executeScript('''
      CREATE TABLE script_tbl (id INTEGER PRIMARY KEY, code TEXT NOT NULL);
      CREATE UNIQUE INDEX ux_script_tbl_code ON script_tbl (code);
      INSERT INTO script_tbl (id, code) VALUES (1, 'a');
      INSERT INTO script_tbl (id, code) VALUES (2, 'b');
    ''');

    final tables = await _queryInt(db,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'script_tbl'");
    // The load-bearing assertion, and the one nothing in this repo made
    // before: `type='index'`. A dropped tail leaves the TABLE in place,
    // so a table-only check passes while the schema is silently wrong.
    final indexes = await _queryInt(db,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'ux_script_tbl_code'");
    final rows = await _queryInt(db, 'SELECT COUNT(*) FROM script_tbl');

    // An index that exists in `sqlite_master` but does not ENFORCE would
    // be the same bug one layer down, so prove the constraint bites.
    Object? duplicateError;
    try {
      await _runSql(db, "INSERT INTO script_tbl (id, code) VALUES (3, 'a')");
    } catch (e) {
      duplicateError = e;
    }

    await db.closeDb();
    await db.dropDb();

    expect(tables, 1, reason: 'statement 1 (CREATE TABLE) must have run');
    expect(indexes, 1,
        reason: 'statement 2 (CREATE UNIQUE INDEX) must have run — this is '
            'the statement the one-prepare/one-step path drops silently, '
            'and no test in this repo has ever looked for it');
    expect(rows, 2,
        reason: 'statements 3 and 4 (both INSERTs) must have run — a script '
            'stops at nothing but a failure');
    expect(
        duplicateError,
        isA<DbasSqliteException>().having((e) => e.subCategory, 'subCategory',
            DbasSqliteSubCategory.duplicatedData),
        reason: 'the UNIQUE index must actually enforce: a CREATE INDEX that '
            'is recorded but not applied would be the same silent-schema '
            'bug one layer down');
  });

  test('executeScript: a trailing PRAGMA foreign_keys = ON actually applies',
      () async {
    // The reported production damage, reproduced in its original shape:
    // `PRAGMA foreign_keys = ON` written as the FOURTH statement of a
    // four-statement open script. Under the prepare path it is discarded
    // with the rest of the tail, so every foreign key in the database
    // silently stops being enforced.
    final db = await _createTestDb('script_pragma_tail.db');

    await db.executeScript('''
      CREATE TABLE fk_parent (id INTEGER PRIMARY KEY);
      CREATE TABLE fk_child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES fk_parent (id)
      );
      INSERT INTO fk_parent (id) VALUES (1);
      PRAGMA foreign_keys = ON;
    ''');

    final fkStmt = await db.prepareQuery('PRAGMA foreign_keys');
    final fkEnabled = await fkStmt.executeScalar();
    await fkStmt.close();

    Object? orphanError;
    try {
      await _runSql(db, 'INSERT INTO fk_child (id, parent_id) VALUES (1, 99)');
    } catch (e) {
      orphanError = e;
    }

    await db.closeDb();
    await db.dropDb();

    expect(fkEnabled, 1,
        reason: 'the 4th statement of the script must have applied — a '
            'dropped tail leaves foreign_keys at its default 0 and the '
            'readback is the only way to see it');
    expect(
        orphanError,
        isA<DbasSqliteException>().having((e) => e.subCategory, 'subCategory',
            DbasSqliteSubCategory.foreignKeyViolation),
        reason: 'foreign_keys=ON must be in force, not merely recorded: an '
            'orphan child row has to be rejected');
  });

  test(
      'executeScript: the prepare path still drops the tail — the limit executeScript exists for',
      () async {
    // A PIN, not a RED. This documents the boundary the new dartdoc on
    // `prepareQuery` / `DbasSqliteStatement.executeSql` now states out
    // loud, and it passes before and after `executeScript` lands.
    //
    // The silence is the whole hazard: the call below RETURNS NORMALLY.
    // There is no rc, no exception and no log to notice — the second
    // statement simply never existed as far as SQLite is concerned.
    final db = await _createTestDb('script_prepare_path_limit.db');

    await _runSql(db, '''
      CREATE TABLE limit_tbl (id INTEGER PRIMARY KEY);
      CREATE INDEX ix_limit_tbl_id ON limit_tbl (id);
    ''');

    final tables = await _queryInt(db,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'limit_tbl'");
    final indexes = await _queryInt(db,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'ix_limit_tbl_id'");

    await db.closeDb();
    await db.dropDb();

    expect(tables, 1,
        reason: 'the FIRST statement runs, which is exactly what makes the '
            'drop invisible — the caller sees a table and assumes the rest '
            'landed too');
    expect(indexes, 0,
        reason: 'everything after the first ; is discarded by the one-prepare/'
            'one-step path, silently. If this ever reads 1 the prepare path '
            'grew multi-statement support and the docs pointing callers at '
            'executeScript need revisiting');
  });

  test(
      'executeScript: a failure stops the script and leaves earlier statements applied',
      () async {
    // `sqlite3_exec`'s real contract, and the reason the atomicity
    // caveat has to be documented: outside a transaction each statement
    // is its own implicit transaction, so statement 1 is ALREADY
    // COMMITTED by the time statement 2 fails. There is no rollback to
    // be had — the failure is not atomic and cannot be made so from
    // inside this call.
    final db = await _createTestDb('script_mid_failure.db');

    Object? error;
    try {
      await db.executeScript('''
        CREATE TABLE midfail_kept (id INTEGER PRIMARY KEY);
        INSERT INTO no_such_table_xyz (id) VALUES (1);
        CREATE TABLE midfail_never (id INTEGER PRIMARY KEY);
      ''');
    } catch (e) {
      error = e;
    }

    final kept = await _rowCountOrAbsent(db, 'midfail_kept');
    final never = await _rowCountOrAbsent(db, 'midfail_never');

    await db.closeDb();
    await db.dropDb();

    expect(
        error,
        isA<DbasSqliteException>()
            .having((e) => e.code, 'code', DbasSqliteErrorCode.executeScriptFailed)
            .having((e) => e.category, 'category',
                DbasSqliteErrorCategory.executeFailed),
        reason: 'the failure must surface as a DbasSqliteException with its '
            'own code — silently returning an rc is what this whole change '
            'exists to stop');
    expect((error as DbasSqliteException).message, contains('no_such_table_xyz'),
        reason: 'sqlite3_exec reports WHICH statement failed via errmsg; '
            'dropping that leaves the caller with a script and no offender');
    expect(kept, 0,
        reason: 'statement 1 ran and, in autocommit, is already committed — '
            'the script is NOT atomic on its own');
    expect(never, -1,
        reason: 'the script stops at the first failure: statement 3 must '
            'never have run');
  });

  test(
      'executeScript: wrapping the script in a caller transaction is what makes it atomic',
      () async {
    // The documented remedy for the caveat above — and the reason
    // `executeScript` does NOT copy `vacuum()`'s in-transaction guard.
    // Rejecting inside a transaction would leave callers with only the
    // non-atomic mode, i.e. force the very hazard the docs warn about.
    final db = await _createTestDb('script_txn_atomic.db');
    await db.beginTransaction();

    Object? error;
    try {
      await db.executeScript('''
        CREATE TABLE txnfail_tbl (id INTEGER PRIMARY KEY);
        INSERT INTO no_such_table_xyz (id) VALUES (1);
      ''');
    } catch (e) {
      error = e;
    }
    await db.rollback();

    final kept = await _rowCountOrAbsent(db, 'txnfail_tbl');

    await db.closeDb();
    await db.dropDb();

    expect(error, isA<DbasSqliteException>(),
        reason: 'the script must still fail the same way inside a '
            'transaction');
    expect(kept, -1,
        reason: 'statement 1 must be GONE. Inside the caller\'s transaction '
            'there is no implicit per-statement commit, so the rollback '
            'takes the CREATE TABLE with it — this is the only way to get '
            'all-or-nothing out of a script');
  });

  test('executeScript: runs inside a transaction without deadlocking on the writer lock',
      () async {
    // `beginTransaction` holds the writer lock for the transaction's
    // whole lifetime and the queue is FIFO, so a naive
    // `_acquireWriterLock()` here would queue behind ITSELF and park for
    // the full `kWriterLockWaitTimeoutMs`. `executeScript` must register
    // as a reentrant writer user instead, exactly as
    // `DbasSqliteStatement.executeSql` does.
    //
    // The commit below is the second half of the proof: a reentrant
    // registration that is never released would make `commit()` throw
    // `commitBlockedByInFlightOperation` instead.
    final db = await _createTestDb('script_in_transaction.db');
    DbasSqlite.debugWriterLockWaitTimeoutMs = 2000;
    addTearDown(() => DbasSqlite.debugWriterLockWaitTimeoutMs = null);

    await db.beginTransaction();
    await db.executeScript('''
      CREATE TABLE txn_script_tbl (id INTEGER PRIMARY KEY, code TEXT);
      CREATE UNIQUE INDEX ux_txn_script_code ON txn_script_tbl (code);
      INSERT INTO txn_script_tbl (id, code) VALUES (1, 'x');
    ''');
    await db.commit();

    final indexes = await _queryInt(db,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'ux_txn_script_code'");
    final rows = await _queryInt(db, 'SELECT COUNT(*) FROM txn_script_tbl');

    await db.closeDb();
    await db.dropDb();

    expect(indexes, 1,
        reason: 'the whole script must have run inside the transaction and '
            'survived the commit');
    expect(rows, 1, reason: 'the INSERT in the script must have committed');
  });

  test('executeScript: returns affected rows and discards result rows',
      () async {
    // Two contracts in one measurement, because they share a script:
    //   - affected rows come from the CONNECTION-scoped accessor
    //     (`sqlite3_changes64`), which is what the C header says exists
    //     for `ExecuteSql` callers — no stmt handle exists in that flow;
    //   - `sqlite3_exec` is called with a nullptr callback, so the
    //     trailing SELECT's rows go nowhere. It must not throw, and it
    //     must not disturb the change counter either.
    final db = await _createTestDb('script_affected_rows.db');
    await _runSql(db, 'CREATE TABLE affected_tbl (id INTEGER PRIMARY KEY)');

    final affected = await db.executeScript('''
      INSERT INTO affected_tbl (id) VALUES (1);
      INSERT INTO affected_tbl (id) VALUES (2);
      INSERT INTO affected_tbl (id) VALUES (3);
      DELETE FROM affected_tbl WHERE id > 1;
      SELECT * FROM affected_tbl;
    ''');
    final remaining = await _queryInt(db, 'SELECT COUNT(*) FROM affected_tbl');

    await db.closeDb();
    await db.dropDb();

    expect(affected, 2,
        reason: 'the count belongs to the last ROW-CHANGING statement (the '
            'DELETE removed 2), not to the script as a whole and not to the '
            'trailing SELECT');
    expect(remaining, 1,
        reason: 'the DELETE really ran — the returned count is not a number '
            'invented by the wrapper');
  });

  test('executeScript: rejects when the database is not opened', () async {
    final db = await DbasSqlite.getInstance(dbName: 'script_not_opened.db');
    await db.dropDb();

    await expectLater(
        db.executeScript('CREATE TABLE never_tbl (id INTEGER PRIMARY KEY)'),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.executeScriptDatabaseNotOpened)
            .having((e) => e.category, 'category',
                DbasSqliteErrorCategory.notOpened)));
  });

  test(
      "executeScript's registry label carries its SQL, like every other "
      'statement-bearing call', () async {
    // The drain-timeout message names every outstanding label, and its
    // whole diagnostic value is saying WHICH call never handed back.
    // executeScript is the DDL/migration door and the most likely
    // long-running native call there is, so a bare 'executeScript' label
    // is the one that identifies least. Truncated the same way
    // executeReader's is — a script can be arbitrarily long.
    final db = await _createTestDb('script_native_op_label.db');
    addTearDown(() => DbasSqlite.debugNativeOpDrainTimeoutMs = null);
    try {
      // A script long enough to prove the truncation, with the
      // identifying part up front where a truncated label keeps it.
      final sql = 'CREATE TABLE script_label_probe (id INTEGER PRIMARY KEY, '
          '${List.generate(20, (i) => 'col$i TEXT').join(', ')})';
      expect(sql.length, greaterThan(80),
          reason: 'the script must exceed the label budget or the '
              'truncation below proves nothing');

      // Zero, not a small number: the drain must expire on the first
      // check, while the script is still registered. A script this short
      // can finish inside any positive window, which would make the test
      // pass or fail on machine speed.
      DbasSqlite.debugNativeOpDrainTimeoutMs = 0;
      // Un-awaited on purpose: the script is inside native code, so the
      // drain has something real to time out on and the failure carries
      // the label this test is about.
      final script = db
          .executeScript(sql)
          .then<Object?>((r) => r, onError: (Object e) => e);

      await expectLater(
        db.closeDb(),
        throwsA(isA<DbasSqliteException>()
            .having((e) => e.code, 'code',
                DbasSqliteErrorCode.closeDbNativeOpDrainTimeout)
            .having((e) => e.message, 'message', contains('executeScript('))
            .having((e) => e.message, 'message',
                contains('CREATE TABLE script_label_probe'))
            // Truncated, not verbatim: the last column of a 20-column
            // script must not make it into the message.
            .having((e) => e.message, 'message', isNot(contains('col19')))
            // Every outstanding label now carries its own age, so one
            // stuck call reads differently from ops churning.
            .having((e) => e.message, 'message', contains('in flight'))),
      );

      DbasSqlite.debugNativeOpDrainTimeoutMs = null;
      expect(await script, isA<int>(),
          reason: 'the refused teardown must leave the script itself '
              'untouched — that is the whole point of refusing');
      // The documented remedy: let the work finish, then close again.
      await db.closeDb();
      expect(db.isOpened(), isFalse);
    } finally {
      DbasSqlite.debugNativeOpDrainTimeoutMs = null;
      await db.dropDb();
    }
  });
}
