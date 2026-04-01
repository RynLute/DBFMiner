using DBFMiner.Shared.Dbf;

namespace DBFMiner.Tests;

public sealed class DbfBinaryReaderTests
{
    [Fact]
    public async Task ReadHeaderAsync_ParsesRealDbfHeader()
    {
        await using var stream = File.Open(
            TestPaths.GetBaseFilePath("00012026.dbf"),
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        var header = await DbfBinaryReader.ReadHeaderAsync(stream, CancellationToken.None);

        Assert.Equal(3, header.Version);
        Assert.Equal(new DateOnly(2026, 2, 25), header.LastUpdatedOn);
        Assert.Equal((uint)80640, header.RecordCount);
        Assert.Equal(417, header.HeaderLength);
        Assert.Equal(37, header.RecordLength);
        Assert.Equal(12, header.Fields.Count);
        Assert.Equal("DATE", header.Fields[0].Name);
        Assert.Equal("FS", header.Fields[^1].Name);
    }
}