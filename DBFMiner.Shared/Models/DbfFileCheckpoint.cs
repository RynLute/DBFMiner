namespace DBFMiner.Shared.Models;

public sealed class DbfFileCheckpoint
{
    public required string FilePath { get; init; }
    public required long FileLastWriteTicks { get; init; }
    public required long FileSize { get; init; }
    public required string Status { get; init; }
    public required int HeaderLength { get; init; }
    public required int RecordLength { get; init; }
    public required long NextByteOffset { get; init; }
    public required long NextRecordIndex { get; init; }
    public required long LastSeenRecordCount { get; init; }
    public required string? HeaderHash { get; init; }
    public required long RowsProcessed { get; init; }
    public required long RowsInserted { get; init; }
    public required string? ErrorText { get; init; }
    public required string? ResetReason { get; init; }
}