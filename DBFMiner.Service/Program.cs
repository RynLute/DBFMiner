using DBFMiner.Service;
using DBFMiner.Shared;

static string GetConfigPath(string[] args)
{
    var configArg = args.FirstOrDefault(a =>
        a.StartsWith("--configPath=", StringComparison.OrdinalIgnoreCase) ||
        a.StartsWith("--config=", StringComparison.OrdinalIgnoreCase));

    if (!string.IsNullOrWhiteSpace(configArg))
    {
        var idx = configArg.IndexOf('=');
        if (idx >= 0 && idx < configArg.Length - 1)
            return configArg[(idx + 1)..];
    }

    return ConfigPaths.DefaultConfigPath;
}

var configPath = GetConfigPath(args);

var builder = Host.CreateDefaultBuilder(args)
    .UseWindowsService(options =>
    {
        options.ServiceName = "DbfMiner";
    })
    .ConfigureServices((_, services) =>
    {
        services.AddSingleton(new ConfigProvider(configPath));
        services.AddSingleton<StatusStore>();
        services.AddSingleton<PostgresRepository>();
        services.AddHostedService<ApiServerHostedService>();
        services.AddHostedService<Worker>();
    });

var host = builder.Build();

var configProvider = host.Services.GetRequiredService<ConfigProvider>();
var statusStore = host.Services.GetRequiredService<StatusStore>();

await configProvider.InitializeAsync(CancellationToken.None).ConfigureAwait(false);
statusStore.SetServiceStarted(DateTimeOffset.UtcNow, configProvider.ConfigPath);
statusStore.MarkReload(configProvider.LastReloadAt, configProvider.Current);

await host.RunAsync().ConfigureAwait(false);
