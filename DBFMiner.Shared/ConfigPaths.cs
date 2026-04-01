namespace DBFMiner.Shared;

public static class ConfigPaths
{
    // Общий каталог для сервиса и пользовательского tray-приложения.
    public static string DefaultConfigPath
        => Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
            "DbfMiner",
            "config.json");
}