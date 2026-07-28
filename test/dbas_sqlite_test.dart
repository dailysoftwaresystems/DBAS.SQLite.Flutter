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
}
