namespace DBFMiner.Shared.Serialization;

public static class SharedJson
{
    public static AppJsonSerializerContext DefaultContext { get; } = Create(writeIndented: false);
    public static AppJsonSerializerContext IndentedContext { get; } = Create(writeIndented: true);

    private static AppJsonSerializerContext Create(bool writeIndented)
    {
        return new AppJsonSerializerContext(new System.Text.Json.JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = writeIndented
        });
    }
}
