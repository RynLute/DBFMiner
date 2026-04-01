using System.Text.Json;
using DBFMiner.Shared.Models;

namespace DBFMiner.Service;

public sealed class ConfigProvider
{
    private readonly string _configPath;
    private readonly SemaphoreSlim _reloadGate = new(1, 1);
    private TaskCompletionSource<bool> _reloadTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private DbfMinerConfig _current = new();

    public string ConfigPath => _configPath;
    public DateTimeOffset LastReloadAt { get; private set; } = DateTimeOffset.MinValue;

    public DbfMinerConfig Current => Volatile.Read(ref _current);

    public ConfigProvider(string configPath)
    {
        _configPath = configPath;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await ReloadInternalAsync(cancellationToken, createIfMissing: true).ConfigureAwait(false);
    }

    public async Task ReloadAsync(CancellationToken cancellationToken)
    {
        await _reloadGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await ReloadInternalAsync(cancellationToken, createIfMissing: false).ConfigureAwait(false);
        }
        finally
        {
            _reloadGate.Release();
        }

        _reloadTcs.TrySetResult(true);
        _reloadTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public Task WaitForReloadAsync()
    {
        return _reloadTcs.Task;
    }

    private async Task ReloadInternalAsync(
        CancellationToken cancellationToken,
        bool createIfMissing)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_configPath)!);

        if (!File.Exists(_configPath))
        {
            if (!createIfMissing)
                throw new FileNotFoundException("config.json not found", _configPath);

            var defaultConfig = new DbfMinerConfig
            {
                DbfFolder = "",
                DbfSearchPattern = "*.dbf",
                PollIntervalSeconds = 10,
                Api = new DBFMiner.Shared.Models.ApiConfig(),
                Postgres = new DBFMiner.Shared.Models.PostgresConfig(),
                Ingestion = new DBFMiner.Shared.Models.IngestionConfig()
            };

            var json = JsonSerializer.Serialize(
                defaultConfig,
                new JsonSerializerOptions { WriteIndented = true });

            await File.WriteAllTextAsync(_configPath, json, cancellationToken).ConfigureAwait(false);
            _current = defaultConfig;
            LastReloadAt = DateTimeOffset.UtcNow;
            return;
        }

        var text = await File.ReadAllTextAsync(_configPath, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(text))
            throw new InvalidDataException($"Config file is empty: {_configPath}");

        var cfg = JsonSerializer.Deserialize<DbfMinerConfig>(
            text,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (cfg is null)
            throw new InvalidDataException($"Failed to deserialize config: {_configPath}");

        _current = cfg;
        LastReloadAt = DateTimeOffset.UtcNow;
    }
}
