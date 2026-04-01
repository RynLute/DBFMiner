using System.Security.Cryptography;
using DBFMiner.Service;
using DBFMiner.Shared;
using DBFMiner.Shared.Dbf;
using DBFMiner.Shared.Models;

namespace DbfMiner.Service;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ConfigProvider _configProvider;
    private readonly StatusStore _statusStore;
    private readonly PostgresRepository _repository;

    private long _totalRowsProcessed;
    private long _totalRowsInserted;

    public Worker(
        ILogger<Worker> logger,
        ConfigProvider configProvider,
        StatusStore statusStore,
        PostgresRepository repository)
    {
        _logger = logger;
        _configProvider = configProvider;
        _statusStore = statusStore;
        _repository = repository;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _statusStore.ResetCounters();
        _statusStore.SetState("Idle");

        await _repository.EnsureTablesAsync(stoppingToken).ConfigureAwait(false);

        while (!stoppingToken.IsCancellationRequested)
        {
            var cfg = _configProvider.Current;
            var dbfFolder = cfg.DbfFolder;

            if (string.IsNullOrWhiteSpace(dbfFolder) || !Directory.Exists(dbfFolder))
            {
                _statusStore.SetState("Idle");
                _statusStore.SetCurrentFile(null, null);
                _statusStore.BeginFileScan(0);
                _statusStore.SetLastError(null);

                await WaitForReloadOrDelay(cfg.PollIntervalSeconds, stoppingToken).ConfigureAwait(false);
                continue;
            }

            try
            {
                _statusStore.SetState("Processing");
                _statusStore.SetLastError(null);
                await ScanAndProcessAsync(stoppingToken).ConfigureAwait(false);
                _statusStore.SetState(
                    string.IsNullOrWhiteSpace(_statusStore.Snapshot().LastError)
                        ? "Idle"
                        : "Error");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Processing loop failed");
                _statusStore.SetLastError(ex.Message);
            }

            await WaitForReloadOrDelay(cfg.PollIntervalSeconds, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task WaitForReloadOrDelay(int pollIntervalSeconds, CancellationToken cancellationToken)
    {
        var delayTask = Task.Delay(TimeSpan.FromSeconds(Math.Max(1, pollIntervalSeconds)), cancellationToken);
        var reloadTask = _configProvider.WaitForReloadAsync();

        try
        {
            await Task.WhenAny(delayTask, reloadTask).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ScanAndProcessAsync(CancellationToken cancellationToken)
    {
        var cfg = _configProvider.Current;
        var dbfFolder = cfg.DbfFolder;
        var searchPattern = string.IsNullOrWhiteSpace(cfg.DbfSearchPattern) ? "*.dbf" : cfg.DbfSearchPattern;

        List<string> files;
        try
        {
            files = Directory
                .EnumerateFiles(dbfFolder, searchPattern, SearchOption.TopDirectoryOnly)
                .Where(cfg.IsDbfFileSelected)
                .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cannot enumerate DBF folder: {Folder}", dbfFolder);
            _statusStore.BeginFileScan(0);
            _statusStore.SetLastError(ex.Message);
            return;
        }

        _statusStore.BeginFileScan(files.Count);

        foreach (var filePath in files)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await ProcessFileAsync(filePath, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessFileAsync(string filePath, CancellationToken cancellationToken)
    {
        var cfg = _configProvider.Current;
        var batchSize = Math.Max(1, cfg.Ingestion.BatchSize);
        var fileLastWriteTicks = new FileInfo(filePath).LastWriteTimeUtc.Ticks;

        _statusStore.SetCurrentFile(filePath, "processing");

        DbfHeaderInfo? header = null;
        long fileSize = 0;

        try
        {
            if (!DbfMinerConfig.TryParseDbfFilePath(filePath, out var fileInfo))
                throw new InvalidDataException(
                    $"DBF file name '{Path.GetFileName(filePath)}' must match pattern nnnndddd.dbf, where nnnn is meter number and dddd is year.");

            await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            fileSize = stream.Length;
            header = await DbfBinaryReader.ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);

            var checkpoint = await _repository.GetFileCheckpointAsync(filePath, cancellationToken).ConfigureAwait(false);
            var decision = DbfCheckpointDecision.Create(filePath, checkpoint, header, fileSize, fileLastWriteTicks);
            var parser = new DbfRecordParser(header);

            var fileRowsProcessed = checkpoint?.RowsProcessed ?? 0;
            var fileRowsInserted = checkpoint?.RowsInserted ?? 0;
            long passRowsProcessed = 0;
            long passRowsInserted = 0;

            _statusStore.SetCurrentPosition(decision.StartByteOffset, decision.StartRecordIndex, decision.ResetReason);

            await _repository.MarkFileProcessingAsync(
                    filePath,
                    fileLastWriteTicks,
                    fileSize,
                    header,
                    decision.StartByteOffset,
                    decision.StartRecordIndex,
                    decision.FullRecordCount,
                    fileRowsProcessed,
                    fileRowsInserted,
                    decision.ResetReason,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!decision.HasNewRecords)
            {
                await _repository.MarkFileCompletedAsync(
                        filePath,
                        fileLastWriteTicks,
                        fileSize,
                        header,
                        decision.StartByteOffset,
                        decision.StartRecordIndex,
                        decision.FullRecordCount,
                        fileRowsProcessed,
                        fileRowsInserted,
                        cancellationToken)
                    .ConfigureAwait(false);

                _statusStore.SetCurrentFile(filePath, "done");
                _statusStore.SetCurrentPosition(decision.StartByteOffset, decision.StartRecordIndex, decision.ResetReason);
                _statusStore.IncrementFilesProcessed();
                return;
            }

            stream.Position = decision.StartByteOffset;

            var nextByteOffset = decision.StartByteOffset;
            var nextRecordIndex = decision.StartRecordIndex;
            var buffer = new byte[header.RecordLength];
            var batch = new List<DbfRowWriteModel>(batchSize);

            while (nextRecordIndex < decision.FullRecordCount && !cancellationToken.IsCancellationRequested)
            {
                await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);

                var recordOffset = nextByteOffset;
                nextByteOffset += header.RecordLength;
                nextRecordIndex++;
                fileRowsProcessed++;
                passRowsProcessed++;

                var row = parser.Parse(buffer);
                if (row is not null)
                {
                    batch.Add(new DbfRowWriteModel
                    {
                        RowIndex = nextRecordIndex,
                        FileOffset = recordOffset,
                        RowHash = ComputeRowHash(buffer),
                        MeterNumber = fileInfo.MeterNumber,
                        SourceYear = fileInfo.Year,
                        Row = row
                    });
                }

                if (batch.Count >= batchSize)
                {
                    var inserted = await FlushBatchAsync(
                            filePath,
                            fileLastWriteTicks,
                            fileSize,
                            header,
                            batch,
                            nextByteOffset,
                            nextRecordIndex,
                            decision.FullRecordCount,
                            fileRowsProcessed,
                            fileRowsInserted,
                            cancellationToken)
                        .ConfigureAwait(false);

                    fileRowsInserted += inserted;
                    passRowsInserted += inserted;
                    batch.Clear();
                }

                if (passRowsProcessed % 200 == 0)
                {
                    _statusStore.SetCurrentPosition(nextByteOffset, nextRecordIndex, decision.ResetReason);
                    _statusStore.UpdateRowProgress(
                        _totalRowsProcessed + passRowsProcessed,
                        _totalRowsInserted + passRowsInserted);
                }
            }

            if (batch.Count > 0)
            {
                var inserted = await FlushBatchAsync(
                        filePath,
                        fileLastWriteTicks,
                        fileSize,
                        header,
                        batch,
                        nextByteOffset,
                        nextRecordIndex,
                        decision.FullRecordCount,
                        fileRowsProcessed,
                        fileRowsInserted,
                        cancellationToken)
                    .ConfigureAwait(false);

                fileRowsInserted += inserted;
                passRowsInserted += inserted;
            }

            _totalRowsProcessed += passRowsProcessed;
            _totalRowsInserted += passRowsInserted;
            _statusStore.UpdateRowProgress(_totalRowsProcessed, _totalRowsInserted);
            _statusStore.SetCurrentPosition(nextByteOffset, nextRecordIndex, decision.ResetReason);

            await _repository.MarkFileCompletedAsync(
                    filePath,
                    fileLastWriteTicks,
                    fileSize,
                    header,
                    nextByteOffset,
                    nextRecordIndex,
                    decision.FullRecordCount,
                    fileRowsProcessed,
                    fileRowsInserted,
                    cancellationToken)
                .ConfigureAwait(false);

            _statusStore.SetCurrentFile(filePath, "done");
            _statusStore.IncrementFilesProcessed();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process DBF file {File}", filePath);
            _statusStore.SetCurrentFile(filePath, "error");
            _statusStore.SetLastError(ex.Message);

            try
            {
                await _repository.UpdateFileErrorAsync(
                        filePath,
                        fileLastWriteTicks,
                        fileSize,
                        header,
                        ex.ToString(),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception dbEx)
            {
                _logger.LogError(dbEx, "Also failed to update file error status");
            }
        }
    }

    private async Task<int> FlushBatchAsync(
        string filePath,
        long fileLastWriteTicks,
        long fileSize,
        DbfHeaderInfo header,
        IReadOnlyList<DbfRowWriteModel> batch,
        long nextByteOffset,
        long nextRecordIndex,
        long lastSeenRecordCount,
        long fileRowsProcessed,
        long fileRowsInserted,
        CancellationToken cancellationToken)
    {
        var inserted = await _repository.PersistBatchAsync(
                filePath,
                fileLastWriteTicks,
                fileSize,
                header,
                batch,
                nextByteOffset,
                nextRecordIndex,
                lastSeenRecordCount,
                fileRowsProcessed,
                fileRowsInserted,
                cancellationToken)
            .ConfigureAwait(false);

        _statusStore.SetCurrentPosition(nextByteOffset, nextRecordIndex);
        return inserted;
    }

    private static string ComputeRowHash(byte[] recordBytes)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(recordBytes);
        return Convert.ToHexString(hash);
    }
}