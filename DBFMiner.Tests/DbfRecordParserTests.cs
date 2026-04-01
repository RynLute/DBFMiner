using DBFMiner.Shared.Dbf;

namespace DBFMiner.Tests;

public sealed class DbfRecordParserTests
{
    [Fact]
    public async Task Parse_FirstRecord_MapsFsAndNumericFields()
    {
        await using var stream = File.Open(
            TestPaths.GetBaseFilePath("00012026.dbf"),
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        var header = await DbfBinaryReader.ReadHeaderAsync(stream, CancellationToken.None);
        var parser = new DbfRecordParser(header);

        stream.Position = header.HeaderLength;
        var buffer = new byte[header.RecordLength];
        await stream.ReadExactlyAsync(buffer, CancellationToken.None);

        var row = parser.Parse(buffer);

        Assert.NotNull(row);
        Assert.Equal(new DateOnly(2026, 1, 1), row!.Date);
        Assert.Equal(new TimeOnly(0, 0), row.Time);
        Assert.Equal(new DateTime(2026, 1, 1, 0, 0, 0), row.Timestamp);
        Assert.Equal(0, row.Count);
        Assert.Equal(4, row.Kod);
        Assert.Equal(1, row.Key);
        Assert.False(row.Avr);
        Assert.False(row.Dna);
        Assert.False(row.Pit);
        Assert.False(row.Ktime);
        Assert.Equal(0, row.AvrTime);
        Assert.Equal(0, row.PitTime);
        Assert.Equal(7, row.Fs);
        Assert.Equal(3, row.Shift);
        Assert.True(row.ShiftEnd);
        Assert.False(row.Sensor1Alarm);
        Assert.False(row.Sensor2Alarm);
        Assert.False(row.TimeSync);
        Assert.False(row.ReadAttempt);
    }

    [Fact]
    public async Task Parse_BlankPayloadRecord_ReturnsNull()
    {
        await using var stream = File.Open(
            TestPaths.GetBaseFilePath("00022026.dbf"),
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        var header = await DbfBinaryReader.ReadHeaderAsync(stream, CancellationToken.None);
        var parser = new DbfRecordParser(header);

        var recordOffset = header.HeaderLength + ((29571 - 1) * header.RecordLength);
        stream.Position = recordOffset;

        var buffer = new byte[header.RecordLength];
        await stream.ReadExactlyAsync(buffer, CancellationToken.None);

        var row = parser.Parse(buffer);

        Assert.Null(row);
    }
}
