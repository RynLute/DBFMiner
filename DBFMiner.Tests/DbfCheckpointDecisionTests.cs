using DBFMiner.Service;
using DBFMiner.Shared.Models;

namespace DBFMiner.Tests;

public sealed class DbfCheckpointDecisionTests
{
    [Fact]
    public void Create_ResumesFromCheckpoint_WhenFileRemainsAppendOnly()
    {
        var header = CreateHeader();
        var checkpoint = new DbfFileCheckpoint
        {
            FilePath = "sample.dbf",
            FileLastWriteTicks = 100,
            FileSize = 517,
            Status = "done",
            HeaderLength = 417,
            RecordLength = 37,
            NextByteOffset = 491,
            NextRecordIndex = 2,
            LastSeenRecordCount = 2,
            HeaderHash = header.HeaderHash,
            RowsProcessed = 2,
            RowsInserted = 2,
            ErrorText = null,
            ResetReason = null
        };

        var decision = DbfCheckpointDecision.Create("sample.dbf", checkpoint, header, 628, 200);

        Assert.Equal(491, decision.StartByteOffset);
        Assert.Equal(2, decision.StartRecordIndex);
        Assert.Equal(4, decision.FullRecordCount);
        Assert.True(decision.HasNewRecords);
    }

    [Fact]
    public void Create_Throws_WhenFileShrinks()
    {
        var header = CreateHeader();
        var checkpoint = new DbfFileCheckpoint
        {
            FilePath = "sample.dbf",
            FileLastWriteTicks = 100,
            FileSize = 628,
            Status = "done",
            HeaderLength = 417,
            RecordLength = 37,
            NextByteOffset = 565,
            NextRecordIndex = 4,
            LastSeenRecordCount = 4,
            HeaderHash = header.HeaderHash,
            RowsProcessed = 4,
            RowsInserted = 4,
            ErrorText = null,
            ResetReason = null
        };

        var ex = Assert.Throws<InvalidDataException>(() =>
            DbfCheckpointDecision.Create("sample.dbf", checkpoint, header, 517, 200));

        Assert.Contains("file size shrank", ex.Message, StringComparison.Ordinal);
    }

    private static DbfHeaderInfo CreateHeader()
    {
        return new DbfHeaderInfo
        {
            Version = 3,
            LastUpdatedOn = new DateOnly(2026, 2, 25),
            RecordCount = 4,
            HeaderLength = 417,
            RecordLength = 37,
            HeaderHash = "HEADER",
            Fields = Array.Empty<DbfFieldDescriptor>()
        };
    }
}
