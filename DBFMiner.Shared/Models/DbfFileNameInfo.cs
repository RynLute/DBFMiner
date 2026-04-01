namespace DBFMiner.Shared.Models;

public sealed class DbfFileNameInfo
{
    public required string FilePath { get; init; }
    public required string FileName { get; init; }
    public required int MeterNumber { get; init; }
    public required int Year { get; init; }
}