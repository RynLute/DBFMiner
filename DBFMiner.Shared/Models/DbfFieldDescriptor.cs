namespace DBFMiner.Shared.Models;

public sealed class DbfFieldDescriptor
{
    public required string Name { get; init; }
    public required char Type { get; init; }
    public required int Length { get; init; }
    public required int DecimalCount { get; init; }
}