#ifdef _WIN32
#define DLL_EXPORT __declspec(dllexport)
#else
#define DLL_EXPORT __attribute__((visibility("default")))
#endif

#pragma once
#include "sqlite/sqlite3.h"
#include <stdint.h>

#ifndef __EMSCRIPTEN__
#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

    /*
     * ABI version. Bump on any change to the binary shape of the public
     * surface that a consumer can OBSERVE — a field reorder/addition in
     * a struct the consumer allocates, sizes, or dereferences
     * (`SQLiteDb` / `SQLiteStmt`), a function signature change, or the
     * removal of an exported symbol. Adding a new DLL_EXPORT function is
     * NOT a bump (additive). FFI consumers (Dart, .NET, Flutter, etc.)
     * compile against this header value and assert at startup against
     * GetAbiVersion() — a mismatch fails fast with a clear error
     * instead of a `<missing-symbol> is not a function` six frames deep,
     * or worse, a struct misread that surfaces as silent corruption.
     *
     * `SQLitePool` is deliberately excluded from the struct list above:
     * it is fully opaque at every FFI boundary — no consumer allocates
     * one, takes its `sizeof`, or reads a field; it is only ever held as
     * an opaque pointer minted and freed by this library. Adding fields
     * to it therefore changes no binary contract any consumer can
     * observe, so additive `SQLitePool` fields do NOT bump. (The
     * invariant is "no consumer depends on the layout," not the weaker
     * "no consumer happens to read it today" — if `SQLitePool` ever
     * becomes consumer-allocatable, it joins the bump list.)
     *
     * Version registry (terse — full notes in CHANGELOG):
     *   1 → introduced 2026-05 alongside `GetSqliteVersion`,
     *       `GetAbiVersion`, and `PoolAcquireReaderBlocking`. The
     *       `closing` / `activeOps` drain fields and the additive
     *       `PoolLastAcquireStatus` accessor landed under this version
     *       (opaque-struct fields + a new export — neither bumps).
     */
    #define DBAS_ABI_VERSION ((uint32_t)1)

    /*
     * Statement handle.
     *
     * A 64-bit per-connection token returned by `PrepareQuery`. The
     * underlying `SQLiteStmt` (the prepared sqlite3_stmt + per-execution
     * state) is owned by the connection and looked up through the
     * connection's live-handle map on every per-stmt call. The handle
     * itself is opaque — callers MUST NOT decode, dereference, or compare
     * across connections; the only valid operations are passing it to
     * the per-stmt API or comparing against `SQLITE_INVALID_STMT_HANDLE`.
     *
     * Lifetime safety:
     *   - Per-connection monotonic counter; never reused within a
     *     connection lifetime (64 bits = practically infinite).
     *   - Stale handles (already-finalized / never-belonged-to-this-db)
     *     resolve to nullptr in the live map → the call returns
     *     SQLITE_MISUSE (or a typed sentinel for column getters) instead
     *     of dereferencing freed memory.
     *   - Idempotent `FinalizeStmt`: a second call on a stale handle is
     *     a silent no-op returning SQLITE_OK.
     *   - Strict `CloseDb` refuses to close while any handle is live; the
     *     map size IS the leak count, computed in O(1).
     *   - Memory tracks live count exactly: each Finalize frees both the
     *     map entry AND the SQLiteStmt heap allocation.
     */
    typedef uint64_t SQLiteStmtHandle;
    #define SQLITE_INVALID_STMT_HANDLE ((SQLiteStmtHandle)0)

    /*
     * Connection handle.
     *
     * Holds connection-scoped state. Per-statement state lives on
     * SQLiteStmt (internal-only struct in dbas_sqlite.cpp), keyed by
     * SQLiteStmtHandle in `liveStmts`.
     *
     * `lastError` here only ever holds connection-scoped errors:
     * Open failures, ExecuteSql failures, and FAILED PrepareQuery (which
     * fails before any SQLiteStmt is allocated). Once a SQLiteStmt
     * exists, every Bind/Step/column-decode error is recorded on the
     * stmt's own `lastError` instead — see `GetLastStmtError`.
     *
     * `liveStmts` is a `std::unordered_map<SQLiteStmtHandle, SQLiteStmt*>*`
     * (held as `void*` to keep the struct C-compatible). Each PrepareQuery
     * inserts; each FinalizeStmt erases AND frees the SQLiteStmt. CloseDb
     * checks size to enforce the lifetime contract: every handle MUST be
     * FinalizeStmt'd before CloseDb.
     *
     * `nextStmtTag` is the per-connection monotonic counter for handle
     * allocation. Starts at 1; 0 is `SQLITE_INVALID_STMT_HANDLE`.
     *
     * ABI / layout policy: this is the PUBLIC header (the distribution
     * copy at `src/public_headers/dbas_sqlite.h`). The struct fields
     * below ARE visible to external consumers (Dart FFI, iOS / Android
     * framework binders, etc.) — but only the public function ABI is
     * stable. Treat `SQLiteDb` as opaque: route all state access
     * through the accessor functions (`GetLastDbError`, `GetDbFileName`,
     * `IsOpened`, etc.). Field reorder, removal, or addition is NOT
     * an ABI break for accessor-only consumers and IS expected on
     * minor versions; consumers that dereference these fields directly
     * are taking on a private contract that may break at any release.
     */
    typedef struct SQLiteDb {
        sqlite3* db;
        char* lastError;
        char* fileName;
        void* liveStmts;
        uint64_t nextStmtTag;
        /* Mutex protecting `liveStmts` and `nextStmtTag`. Pool readers
         * and writers are separate SQLiteDb instances, so each connection
         * has its own lock; concurrent ops across different connections
         * never contend. Within a single connection, concurrent
         * Prepare/Finalize/resolve ops are serialized through this lock.
         * The lock is held only briefly (never across SQLite step/bind
         * calls), so it does not serialize the actual SQL work. */
#ifndef __EMSCRIPTEN__
#ifdef _WIN32
        CRITICAL_SECTION stmtsLock;
#else
        pthread_mutex_t stmtsLock;
#endif
#endif
    } SQLiteDb;

    /* ── Library-scoped ───────────────────────────────────────────── */

    /*
     * Vendored SQLite version (e.g. "3.52.0"). Returns
     * `sqlite3_libversion()` — the runtime version string baked into
     * the .c amalgamation we ship. Useful for crash reports,
     * diagnostic logs, and "is this the build I think it is?" checks
     * from FFI consumers. Buffer is owned by SQLite (static literal);
     * do not free.
     */
    DLL_EXPORT const char* GetSqliteVersion(void);

    /*
     * ABI version of THIS binary. Returns `DBAS_ABI_VERSION` as it
     * was at compile time. FFI consumers should assert
     *
     *     assert(GetAbiVersion() == DBAS_ABI_VERSION)
     *
     * at startup. A mismatch means the consumer's bindings were
     * generated against a different header than the binary they're
     * actually loading — a class of bug that otherwise surfaces as
     * silent struct misreads or symbol-not-found errors deep inside
     * a call. See `DBAS_ABI_VERSION` for the bump policy and registry.
     */
    DLL_EXPORT uint32_t GetAbiVersion(void);

    /* ── Connection-scoped ────────────────────────────────────────── */

    /*
     * Open (or create) a database file. See the public header for the
     * full contract. Quick recap:
     *   - On success, returns a SQLiteDb* with `inst->db != NULL`.
     *   - On `sqlite3_open_v2` failure, ALSO returns a non-NULL inst,
     *     but with `inst->db == NULL` and `inst->lastError` set. Caller
     *     MUST check `IsOpened(inst)` (or `inst->db != NULL`) before
     *     using and call `CloseDb(inst, 0)` to release the partial state.
     *   - `OpenDb(NULL)` returns NULL.
     */
    DLL_EXPORT SQLiteDb* OpenDb(const char* fileName);
    DLL_EXPORT bool IsOpened(SQLiteDb* inst);

    /* One-shot SQL execution (DDL / write without bindings). Errors land
     * on inst->lastError. */
    DLL_EXPORT int ExecuteSql(SQLiteDb* inst, const char* sql);

    /* Connection-level error message. NULL when none. Buffer is owned by
     * the connection — do not free. */
    DLL_EXPORT const char* GetLastDbError(SQLiteDb* inst);

    /* Extended SQLite return code on the connection
     * (`sqlite3_extended_errcode`). Distinguishes e.g.
     * SQLITE_IOERR_READ (266) from SQLITE_IOERR_SHORT_READ (522) —
     * both are extended forms of SQLITE_IOERR (10). Returns -1 on a
     * NULL or unopened connection (cannot be a legitimate SQLite rc,
     * so no overlap). The primary rc returned by ReadRow / ExecuteSql
     * / bind APIs is sufficient for most callers; extendedRc is for
     * targeted recovery / diagnostic logging. */
    DLL_EXPORT int GetExtendedErrorCode(SQLiteDb* inst);

    /* File name used to open the connection. NULL after CloseDb. Buffer
     * is owned by the connection — do not free. */
    DLL_EXPORT const char* GetDbFileName(SQLiteDb* inst);

    /* `sqlite3_changes64` / `sqlite3_last_insert_rowid` for the most
     * recent operation on this connection. Race-prone if other stmts
     * step concurrently on the same connection — when you have a stmt
     * handle, prefer `GetStmtAffectedRows` / `GetStmtLastInsertedId`,
     * which are captured at step time and survive concurrent stepping.
     * The connection-scoped variants are intended for callers of
     * ExecuteSql (no stmt handle exists in that flow). All return int64
     * (long long) to match the int64 column read/write surface and
     * sqlite3_changes64; -1 on a NULL or closed connection. */
    DLL_EXPORT long long GetAffectedRows(SQLiteDb* inst);
    DLL_EXPORT long long GetLastInsertedId(SQLiteDb* inst);

    /* Connection-lifetime cumulative change counter
     * (sqlite3_total_changes64). Returns the total number of row
     * changes seen by this connection, including those overwritten
     * or rolled back. -1 on a NULL or closed connection. */
    DLL_EXPORT long long GetTotalChanges(SQLiteDb* inst);

    /* Close the connection. ENFORCED lifetime contract: every handle
     * produced by this connection MUST be `FinalizeStmt`'d before this
     * call. If any are still alive, CloseDb returns SQLITE_BUSY (5),
     * writes a descriptive message to inst->lastError (read it with
     * GetLastDbError), and the connection stays alive and fully usable —
     * finalize the leaked handles, then call CloseDb again. SQLITE_OK
     * (0) means the connection was closed and the SQLiteDb pointer is
     * now invalid. Returns SQLITE_OK on a NULL input (no-op).
     *
     * `checkpoint`: if non-zero, runs sqlite3_wal_checkpoint_v2(TRUNCATE)
     * before the close. Pass non-zero when the next operation reads the
     * .db file at the file-system level (e.g. file copy, raw read,
     * upload) — this guarantees the main file reflects every committed
     * write. No-op for non-WAL databases. Pass zero for ordinary closes;
     * the cost is small but non-zero (a checkpoint walks the WAL).
     *
     * Mirrors `sqlite3_close`'s SQLITE_BUSY-on-leak contract. Use the
     * pool's ClosePool for defensive end-of-life cleanup that
     * auto-finalizes any leftover handles on its connections. */
    DLL_EXPORT int CloseDb(SQLiteDb* inst, int checkpoint);

    /* Set the SQLite busy-timeout (ms) on an open connection. Default
     * for any connection returned by OpenDb is 5000 ms — call this only
     * when a different value is needed (e.g. a long-running batch in a
     * single-tenant scenario where instant-fail is preferable). Returns
     * SQLITE_OK on success, SQLITE_MISUSE on a closed/null connection. */
    DLL_EXPORT int SetBusyTimeout(SQLiteDb* inst, int ms);

    /* Switch the connection to WAL journal mode and verify the readback.
     * Returns SQLITE_OK if WAL is active after the call. On failure
     * populates inst->lastError (read via GetLastDbError) and returns
     * the driver rc — preserved as-is so callers can react meaningfully
     * (SQLITE_BUSY is retryable; SQLITE_NOTADB / SQLITE_CORRUPT are
     * permanent; SQLITE_IOERR_* surfaces filesystem issues). Returns
     * SQLITE_ERROR specifically when the step succeeded but the
     * readback mode is not 'wal' — i.e. PRAGMA silently no-op'd on
     * read-only media or unsupported VFS, the case the verify exists
     * to catch. SQLITE_MISUSE on a closed/null connection. Both native
     * and web pool paths route through this single recipe. */
    DLL_EXPORT int EnableWal(SQLiteDb* inst);

    /* ── Statement-scoped ─────────────────────────────────────────── */

    /*
     * Prepare a SQL statement. Returns a non-zero `SQLiteStmtHandle` on
     * success, or `SQLITE_INVALID_STMT_HANDLE` (0) on failure. On
     * failure, inspect `GetLastDbError(inst)` — the error originated at
     * the connection before any statement was allocated.
     *
     * Caller MUST `FinalizeStmt` the returned handle. Multiple handles
     * may be alive on the same connection at the same time; each carries
     * fully independent state.
     */
    DLL_EXPORT SQLiteStmtHandle PrepareQuery(SQLiteDb* inst, const char* sql);

    /*
     * Per-statement bind/step/read API. All take (db, handle).
     *
     * Stale-handle behavior: every per-stmt entry point starts with an
     * O(1) live-map lookup. If the handle is unknown to the connection
     * (already finalized, never belonged here, garbage value), the call
     * is a safe no-op:
     *   - Bind / Read / int-returning Get      -> SQLITE_MISUSE (21)
     *   - GetColumn that return pointers       -> nullptr / 0
     *   - GetLastStmtError                     -> nullptr
     *   - FinalizeStmt                         -> SQLITE_OK (idempotent)
     *
     * No call ever dereferences the handle as a pointer; there is no
     * use-after-free surface from a stale handle.
     */

    DLL_EXPORT int BindText(SQLiteDb* db, SQLiteStmtHandle h, int index, const char* value);
    DLL_EXPORT int BindInt(SQLiteDb* db, SQLiteStmtHandle h, int index, int value);
    /* 64-bit integer bind. Symmetric with GetStmtLastInsertedId on
     * the read side: row IDs and large counters that overflow 32
     * bits would silently truncate via BindInt. */
    DLL_EXPORT int BindInt64(SQLiteDb* db, SQLiteStmtHandle h, int index, long long value);
    DLL_EXPORT int BindFloat(SQLiteDb* db, SQLiteStmtHandle h, int index, float value);
    DLL_EXPORT int BindDouble(SQLiteDb* db, SQLiteStmtHandle h, int index, double value);
    DLL_EXPORT int BindNull(SQLiteDb* db, SQLiteStmtHandle h, int index);
    DLL_EXPORT int BindBlob(SQLiteDb* db, SQLiteStmtHandle h, int index, const void* data, int length);

    DLL_EXPORT int BindNameText(SQLiteDb* db, SQLiteStmtHandle h, const char* name, const char* value);
    DLL_EXPORT int BindNameInt(SQLiteDb* db, SQLiteStmtHandle h, const char* name, int value);
    DLL_EXPORT int BindNameInt64(SQLiteDb* db, SQLiteStmtHandle h, const char* name, long long value);
    DLL_EXPORT int BindNameFloat(SQLiteDb* db, SQLiteStmtHandle h, const char* name, float value);
    DLL_EXPORT int BindNameDouble(SQLiteDb* db, SQLiteStmtHandle h, const char* name, double value);
    DLL_EXPORT int BindNameNull(SQLiteDb* db, SQLiteStmtHandle h, const char* name);
    DLL_EXPORT int BindNameBlob(SQLiteDb* db, SQLiteStmtHandle h, const char* name, const void* data, int length);

    /* Type-dispatched bind for native callers. `type` is one of:
     *   0=NULL, 1=INT64 (uses intValue), 2=DOUBLE (uses doubleValue),
     *   3=TEXT (UTF-8 at `data`, `length` bytes; -1 = strlen, NULL data
     *          binds NULL),
     *   4=BLOB (`data`, `length` bytes; NULL data binds NULL, length=0
     *          with non-NULL data preserves a real zero-byte BLOB).
     * Mirrors the JS-side bindParams type routing so a native caller
     * doesn't have to duplicate the Number.isInteger / Uint8Array dispatch.
     * Returns the raw sqlite3_bind_* return code; SQLITE_MISUSE on a
     * stale handle or unknown `type`. On rcs other than SQLITE_OK,
     * SQLITE_RANGE, and SQLITE_MISUSE (e.g. SQLITE_TOOBIG, SQLITE_NOMEM)
     * a descriptive message is captured on the stmt — read it via
     * GetLastStmtError(db, h). */
    DLL_EXPORT int BindValue(SQLiteDb* db, SQLiteStmtHandle h, int index,
                             int type, const void* data, long long intValue,
                             double doubleValue, int length);

    /* Step the statement. Returns SQLITE_ROW (100), SQLITE_DONE (101),
     * an error code, or SQLITE_MISUSE (21) on a stale handle. On error,
     * populates the stmt's lastError; read it via GetLastStmtError. On a
     * SUCCESSFUL step (ROW or DONE), captures sqlite3_changes64 /
     * sqlite3_last_insert_rowid into stmt-scoped slots so the caller can
     * read them later via GetStmtAffectedRows / GetStmtLastInsertedId
     * without being raced by concurrent stmts on the same connection. An
     * error step intentionally does NOT overwrite the captured counters
     * — the most recent successful snapshot survives. */
    DLL_EXPORT int ReadRow(SQLiteDb* db, SQLiteStmtHandle h);

    DLL_EXPORT int IsNull(SQLiteDb* db, SQLiteStmtHandle h, int col_index);
    /* GetColumnText / GetColumnBlob return pointers owned by SQLite that
     * are valid until the next ReadRow or FinalizeStmt ON THIS SAME
     * stmt. Concurrent stmts on the same connection do NOT invalidate
     * them. */
    DLL_EXPORT char* GetColumnText(SQLiteDb* db, SQLiteStmtHandle h, int col_index);
    DLL_EXPORT int GetColumnInt(SQLiteDb* db, SQLiteStmtHandle h, int col_index);
    /* 64-bit column read. Symmetric with BindInt64. */
    DLL_EXPORT long long GetColumnInt64(SQLiteDb* db, SQLiteStmtHandle h, int col_index);
    DLL_EXPORT float GetColumnFloat(SQLiteDb* db, SQLiteStmtHandle h, int col_index);
    DLL_EXPORT double GetColumnDouble(SQLiteDb* db, SQLiteStmtHandle h, int col_index);
    DLL_EXPORT int GetColumnType(SQLiteDb* db, SQLiteStmtHandle h, int col_index);
    DLL_EXPORT const void* GetColumnBlob(SQLiteDb* db, SQLiteStmtHandle h, int index);
    DLL_EXPORT int GetColumnBytes(SQLiteDb* db, SQLiteStmtHandle h, int index);
    DLL_EXPORT char* GetColumnName(SQLiteDb* db, SQLiteStmtHandle h, int index);
    DLL_EXPORT int GetColumnCount(SQLiteDb* db, SQLiteStmtHandle h);

    /* Non-zero (1) if the prepared statement makes no direct changes to
     * the database (sqlite3_stmt_readonly). Web-pool writer worker uses
     * it to choose a streaming cursor's cross-handle fence (SHARED for
     * reads, EXCLUSIVE for writes). Returns 1 on a stale handle. */
    DLL_EXPORT int GetStmtReadonly(SQLiteDb* db, SQLiteStmtHandle h);

    /* Statement-level error message. NULL when none, also NULL on a
     * stale handle. Buffer is owned by the statement — do not free. */
    DLL_EXPORT const char* GetLastStmtError(SQLiteDb* db, SQLiteStmtHandle h);

    /* Stmt-scoped post-step counters. Captured by ReadRow on a
     * SUCCESSFUL step (ROW or DONE), so they're not raced by concurrent
     * stmts on the same connection and not corrupted by a later error
     * step on this stmt. Returns -1 if the stmt has never been
     * successfully stepped, or on a stale handle. Both int64 to match
     * sqlite3_changes64 / sqlite3_last_insert_rowid. */
    DLL_EXPORT long long GetStmtAffectedRows(SQLiteDb* db, SQLiteStmtHandle h);
    DLL_EXPORT long long GetStmtLastInsertedId(SQLiteDb* db, SQLiteStmtHandle h);

    /* Finalize the statement. Idempotent: returns SQLITE_OK whether the
     * handle was alive (resources finalized + heap freed + map entry
     * erased) or stale (no-op). Safe to call with a NULL db, a 0
     * handle, or a handle that was never produced by this db. */
    DLL_EXPORT int FinalizeStmt(SQLiteDb* db, SQLiteStmtHandle h);

    /* ── Connection Pool ──────────────────────────────────────────── */

    typedef struct SQLitePool {
        SQLiteDb* writer;
        SQLiteDb** readers;
        bool* readerBusy;
        int readerCount;
        char* fileName;
        /* Latched true by `ClosePool` before it broadcasts the
         * shutdown signal. While set, `PoolAcquireReader[Blocking]`
         * bail under the lock instead of returning a soon-to-be-freed
         * reader, and any thread parked in `pool_cond_wait` sees the
         * flag on its next wake and exits without touching pool
         * state. The flag plus `activeOps` together let `ClosePool`
         * wait until no operation is in flight before destroying
         * the lock / condvar. */
        bool closing;
        /* Count of in-flight pool operations that must finish before
         * teardown is safe. Two contributors, both tracked under the
         * lock:
         *   1. A blocking acquire parked in `pool_cond_wait` /
         *      `pool_cond_timedwait_ms` — bumped on entry so `ClosePool`
         *      can't destroy the condvar it's blocked on.
         *   2. A CHECKED-OUT reader — the bump from a successful
         *      acquire is NOT released on return; it persists for the
         *      whole acquire→`PoolReleaseReader` interval so `ClosePool`
         *      can't `closeDbCore` a reader (or destroy the lock that
         *      `PoolReleaseReader` will take) while a caller still holds
         *      and may be stepping it.
         * For a blocking acquire the single entry-bump serves both
         * phases with no gap: it covers the park, then transfers to
         * covering the held reader on success. Decremented (and the
         * condvar broadcast) on every acquire-failure path and inside
         * `PoolReleaseReader`, so a draining `ClosePool` observes zero
         * once every reader is back and every waiter has bailed. */
        int activeOps;
#ifndef __EMSCRIPTEN__
#ifdef _WIN32
        CRITICAL_SECTION lock;
        /* Condition variable signalled whenever a reader is released
         * back to the pool, when `ClosePool` latches `closing`, and
         * whenever `activeOps` is decremented. `PoolAcquireReaderBlocking`
         * waits on this to avoid the spin-and-retry pattern FFI
         * consumers would otherwise have to write at the Dart /
         * language layer; `ClosePool` waits on it to drain in-flight
         * acquires before tearing the pool down. */
        CONDITION_VARIABLE readerAvailable;
#else
        pthread_mutex_t lock;
        pthread_cond_t readerAvailable;
#endif
#endif
    } SQLitePool;

    DLL_EXPORT SQLitePool* CreatePool(const char* fileName, int readerCount);
    DLL_EXPORT SQLiteDb* PoolGetWriter(SQLitePool* pool);

    /* Why the last `PoolAcquireReader[Blocking]` returned NULL. Both
     * acquire functions return NULL for several distinct reasons that
     * the SQLiteDb*\/NULL contract alone can't tell apart — most
     * importantly, "the pool is closing" (terminal; retrying is
     * pointless) versus "no reader was free in time" (transient;
     * retrying may succeed). After a NULL return, call this to learn
     * which. The value is stored per-thread (like `errno`): it
     * reflects THIS thread's most recent acquire call only, is not
     * raced by acquires on other threads, and is meaningful only
     * immediately after a NULL — a successful acquire sets it to
     * `POOL_ACQUIRE_OK`. */
    typedef enum PoolAcquireStatus {
        POOL_ACQUIRE_OK       = 0, /* a reader was returned */
        POOL_ACQUIRE_NO_SLOT  = 1, /* non-blocking / timeout==0: all busy */
        POOL_ACQUIRE_TIMEOUT  = 2, /* blocking: deadline elapsed, none free */
        POOL_ACQUIRE_CLOSING  = 3, /* pool is closing or closed (terminal) */
        POOL_ACQUIRE_INVALID  = 4  /* NULL pool / readers already destroyed */
    } PoolAcquireStatus;
    DLL_EXPORT int PoolLastAcquireStatus(void);

    /* Non-blocking reader acquire. Returns the first idle reader's
     * SQLiteDb*, or NULL when every reader is busy (or the pool is
     * closing — see `PoolLastAcquireStatus`). Caller is responsible
     * for retrying / queueing. Use `PoolAcquireReaderBlocking` when you
     * want the kernel to wake you on release rather than spinning.
     * A returned reader is "checked out" until `PoolReleaseReader`;
     * see ClosePool for the lifetime contract that pins. */
    DLL_EXPORT SQLiteDb* PoolAcquireReader(SQLitePool* pool);
    /* Blocking reader acquire with optional timeout.
     *
     *   timeout_ms <  0  : wait forever (returns only on success or
     *                      when the pool starts closing)
     *   timeout_ms == 0  : equivalent to PoolAcquireReader (no wait)
     *   timeout_ms >  0  : wait up to that many milliseconds; returns
     *                      NULL if no reader becomes available in
     *                      that window
     *
     * Spurious wakeups are handled internally; callers see a clean
     * "got a reader, hit the deadline, or the pool is closing"
     * contract — call `PoolLastAcquireStatus` to disambiguate a NULL.
     * Returns NULL on a NULL pool or one whose readers were destroyed.
     * On WASM (single-threaded) this degrades to PoolAcquireReader —
     * there's no peer thread to release a reader while we wait. */
    DLL_EXPORT SQLiteDb* PoolAcquireReaderBlocking(SQLitePool* pool, int timeout_ms);
    /* Return a checked-out reader to the pool. Idempotent for a given
     * checkout: a second release of an already-returned reader (or of a
     * pointer this pool never handed out) is a no-op. Releasing is what
     * lets a concurrent `ClosePool` finish — see its contract. */
    DLL_EXPORT void PoolReleaseReader(SQLitePool* pool, SQLiteDb* reader);
    /* End-of-life cleanup. Force-closes the writer and all readers,
     * draining any handles the caller forgot to finalize.
     *
     * ClosePool BLOCKS until every reader checked out via
     * `PoolAcquireReader[Blocking]` has been returned with
     * `PoolReleaseReader` and every parked blocking-acquire has woken
     * and bailed. This is what makes teardown safe: a reader cannot be
     * force-closed (nor the lock/condvar destroyed) while another
     * thread still holds and may be stepping it. New acquires after
     * close begins return NULL with status `POOL_ACQUIRE_CLOSING`.
     *
     * Corollary — calling ClosePool from a thread that is ITSELF still
     * holding a reader deadlocks (nothing can release it). The
     * supported usage is "quiesce, release every reader, then close,"
     * which the wrapper layer already enforces. The writer is NOT
     * checkout-tracked (it has no release call); the same quiesce
     * contract covers it.
     *
     * After this call returns:
     *   - The pool pointer is invalid.
     *   - Every SQLiteDb pointer the pool produced (writer + readers) is
     *     invalid.
     *   - Every SQLiteStmtHandle ever produced by this pool's
     *     connections is invalid; any SUBSEQUENT per-stmt API call with
     *     such a handle is undefined behavior (the db pointer is freed).
     * The wrapper-level pool object that issued the handles is
     * responsible for not re-using them after close. */
    DLL_EXPORT void ClosePool(SQLitePool* pool);

#ifdef __cplusplus
}
#endif
