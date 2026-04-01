namespace DBFMiner.Tests;

internal static class TestPaths
{
    public static string GetBaseFilePath(string fileName)
    {
        return TestDbfFixtureFactory.EnsureFixture(fileName);
    }
}
