/// Outcome of one WAL checkpoint — the three values SQLite's
/// `PRAGMA wal_checkpoint` reports, kept together because none of them
/// means anything on its own.
///
/// Returned by `DbasSqlite.checkpoint`. Read [isComplete] to decide
/// whether committed data actually reached the main `.db` file; see the
/// warning on [busy] for why that flag must **not** be used for it.
class DbasSqliteCheckpointResult {
  const DbasSqliteCheckpointResult({
    required this.busy,
    required this.log,
    required this.checkpointed,
  });

  /// `1` when SQLite could not take the checkpoint lock because another
  /// connection was already checkpointing (or, for the blocking modes,
  /// the `busy_timeout` elapsed); `0` otherwise.
  ///
  /// **Not a success signal.** A `PASSIVE` checkpoint that folds nothing
  /// because a reader pins the WAL still reports `busy == 0` and
  /// `SQLITE_OK` — measured `(busy: 0, log: 10, checkpointed: 0)` —
  /// which is indistinguishable from a full fold by this flag alone.
  /// Use [isComplete].
  final int busy;

  /// Frames in the `-wal` file at the end of the checkpoint, or `-1`
  /// when the database is **not** in WAL mode (there is no WAL, so
  /// there is nothing to fold — see [isComplete]).
  final int log;

  /// Frames this checkpoint copied into the main `.db` file, or `-1`
  /// when the database is **not** in WAL mode.
  final int checkpointed;

  /// `true` when every frame that was in the WAL reached the main `.db`
  /// file — `checkpointed == log`.
  ///
  /// **This is the only honest test** of "the committed data is in the
  /// `.db` file". [busy] is not: see its documentation. Non-WAL
  /// databases report `log == checkpointed == -1`, which is `true` here
  /// and correctly means "nothing was left behind".
  ///
  /// `false` is a **recoverable** state, not an error: a reader holding
  /// a WAL snapshot pins every frame above it, and no checkpoint mode
  /// can fold those — they fold at the next opportunity once the
  /// snapshot is released. What it does mean is that a copy of the main
  /// `.db` file **alone**, taken right now, would be missing them.
  bool get isComplete => checkpointed == log;

  @override
  String toString() => 'DbasSqliteCheckpointResult(busy: $busy, log: $log, '
      'checkpointed: $checkpointed, isComplete: $isComplete)';
}
