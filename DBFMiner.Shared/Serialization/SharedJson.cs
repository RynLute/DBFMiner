using System.Text.Json;

namespace DBFMiner.Shared.Serialization;

public static class SharedJson
{
    public static JsonSerializerOptions Default { get; } = Create(writeIndented: false);
    public static JsonSerializerOptions Indented { get; } = Create(writeIndented: true);

    private static JsonSerializerOptions Create(bool writeIndented)
    {
        return new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = writeIndented,
            TypeInfoResolver = AppJsonSerializerContext.Default
        };
    }
}