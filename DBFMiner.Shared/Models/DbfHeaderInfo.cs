namespace DBFMiner.Shared.Models;

public sealed class DbfHeaderInfo
{
    public required byte Version { get; init; }
    public required DateOnly LastUpdatedOn { get; init; }
    public required uint RecordCount { get; init; }
    public required int HeaderLength { get; init; }
    public required int RecordLength { get; init; }
    public required string HeaderHash { get; init; }
    public required IReadOnlyList<DbfFieldDescriptor> Fields { get; init; }
}