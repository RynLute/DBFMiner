using DBFMiner.Shared.Models;

namespace DBFMiner.Service;

public sealed class DbfRowWriteModel
{
    public required long RowIndex { get; init; }
    public required long FileOffset { get; init; }
    public required string RowHash { get; init; }
    public required int MeterNumber { get; init; }
    public required int SourceYear { get; init; }
    public required ParsedDbfRow Row { get; init; }
}