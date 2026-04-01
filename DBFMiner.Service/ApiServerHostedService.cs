using DBFMiner.Shared.Dto;
using DBFMiner.Shared.Serialization;
using Microsoft.AspNetCore.Http;

namespace DBFMiner.Service;

public sealed class ApiServerHostedService : BackgroundService
{
    private readonly ConfigProvider _configProvider;
    private readonly StatusStore _statusStore;
    private readonly ILogger<ApiServerHostedService> _logger;

    public ApiServerHostedService(
        ConfigProvider configProvider,
        StatusStore statusStore,
        ILogger<ApiServerHostedService> logger)
    {
        _configProvider = configProvider;
        _statusStore = statusStore;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var cfg = _configProvider.Current;

        var url = $"http://{cfg.Api.Host}:{cfg.Api.Port}";
        _logger.LogInformation("Local HTTP API listening on {Url}", url);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls(url);
        var app = builder.Build();

        app.MapGet("/api/status", () =>
        {
            return Results.Json(_statusStore.Snapshot(), SharedJson.Default);
        });

        app.MapPost("/api/config/reload", async (CancellationToken ct) =>
        {
            try
            {
                await _configProvider.ReloadAsync(ct).ConfigureAwait(false);
                _statusStore.MarkReload(_configProvider.LastReloadAt, _configProvider.Current);

                var resp = new ReloadResponseDto
                {
                    Ok = true,
                    Message = "Config reloaded",
                    Status = _statusStore.Snapshot()
                };

                return Results.Json(resp, SharedJson.Default);
            }
            catch (Exception ex)
            {
                var resp = new ReloadResponseDto
                {
                    Ok = false,
                    Message = ex.Message,
                    Status = _statusStore.Snapshot()
                };

                _logger.LogError(ex, "Failed to reload config");
                return Results.Json(resp, SharedJson.Default, statusCode: 500);
            }
        });

        await app.RunAsync(stoppingToken).ConfigureAwait(false);
    }
}

