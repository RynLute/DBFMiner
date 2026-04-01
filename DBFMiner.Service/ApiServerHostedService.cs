using DBFMiner.Shared.Dto;
using DBFMiner.Shared.Models;
using DBFMiner.Shared.Serialization;
using Microsoft.AspNetCore.Http;

namespace DBFMiner.Service;

public sealed class ApiServerHostedService : BackgroundService
{
    private readonly ConfigProvider _configProvider;
    private readonly StatusStore _statusStore;
    private readonly ILogger<ApiServerHostedService> _logger;

    private TaskCompletionSource<bool> _restartTcs = CreateRestartSignal();

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
        while (!stoppingToken.IsCancellationRequested)
        {
            var url = BuildUrl(_configProvider.Current.Api);
            _logger.LogInformation("Local HTTP API listening on {Url}", url);

            await using var app = BuildApp(url);
            await app.StartAsync(stoppingToken).ConfigureAwait(false);

            var restartRequested = await WaitForRestartOrStopAsync(stoppingToken).ConfigureAwait(false);

            await app.StopAsync(CancellationToken.None).ConfigureAwait(false);

            if (!restartRequested || stoppingToken.IsCancellationRequested)
                break;

            _restartTcs = CreateRestartSignal();
        }
    }

    private WebApplication BuildApp(string url)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls(url);

        var app = builder.Build();

        app.MapGet("/api/status", () =>
        {
            return Results.Json(
                _statusStore.Snapshot(),
                SharedJson.DefaultContext.ServiceStatusDto);
        });

        app.MapPost("/api/config/reload", async (HttpContext httpContext, CancellationToken ct) =>
        {
            var previousUrl = BuildUrl(_configProvider.Current.Api);

            try
            {
                await _configProvider.ReloadAsync(ct).ConfigureAwait(false);
                _statusStore.MarkReload(_configProvider.LastReloadAt, _configProvider.Current);

                var updatedUrl = BuildUrl(_configProvider.Current.Api);
                if (!string.Equals(previousUrl, updatedUrl, StringComparison.OrdinalIgnoreCase))
                {
                    httpContext.Response.OnCompleted(() =>
                    {
                        RequestRestart();
                        return Task.CompletedTask;
                    });
                }

                var resp = new ReloadResponseDto
                {
                    Ok = true,
                    Message = string.Equals(previousUrl, updatedUrl, StringComparison.OrdinalIgnoreCase)
                        ? "Config reloaded"
                        : $"Config reloaded. API listener will restart on {updatedUrl}",
                    Status = _statusStore.Snapshot()
                };

                return Results.Json(resp, SharedJson.DefaultContext.ReloadResponseDto);
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
                return Results.Json(
                    resp,
                    SharedJson.DefaultContext.ReloadResponseDto,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        });

        return app;
    }

    private static string BuildUrl(ApiConfig config)
    {
        return $"http://{config.Host}:{config.Port}";
    }

    private void RequestRestart()
    {
        _restartTcs.TrySetResult(true);
    }

    private async Task<bool> WaitForRestartOrStopAsync(CancellationToken stoppingToken)
    {
        var restartTask = _restartTcs.Task;
        var stopTask = Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);

        try
        {
            var completedTask = await Task.WhenAny(restartTask, stopTask).ConfigureAwait(false);
            return completedTask == restartTask;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private static TaskCompletionSource<bool> CreateRestartSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
