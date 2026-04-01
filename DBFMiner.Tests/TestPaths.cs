namespace DBFMiner.Tests;

internal static class TestPaths
{
    public static string GetBaseFilePath(string fileName)
    {
        var root = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory,
            "..",
            "..",
            "..",
            ".."));

        return Path.Combine(root, "Base", fileName);
    }
}