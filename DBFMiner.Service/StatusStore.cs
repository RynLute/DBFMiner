using DBFMiner.Shared.Dto;
using DBFMiner.Shared.Models;

namespace DBFMiner.Service;

public sealed class StatusStore
{
    private readonly object _gate = new();
    private readonly ServiceStatusDto _status = new();

    public void SetServiceStarted(DateTimeOffset startedAt, string configPath)
    {
        lock (_gate)
        {
            _status.ServiceStartedAt = startedAt;
            _status.ConfigPath = configPath;
        }
    }

    public void MarkReload(DateTimeOffset reloadAt, DbfMinerConfig config)
    {
        lock (_gate)
        {
            _status.LastReloadAt = reloadAt;
            _status.PollIntervalSeconds = config.PollIntervalSeconds;
        }
    }

    public void SetState(string serviceState)
    {
        lock (_gate)
        {
            _status.ServiceState = serviceState;
        }
    }

    public void SetCurrentFile(string? filePath, string? fileStatus)
    {
        lock (_gate)
        {
            _status.CurrentFile = filePath;
            _status.CurrentFileStatus = fileStatus;

            if (filePath is null)
            {
                _status.CurrentFileOffset = null;
                _status.CurrentRecordIndex = null;
                _status.LastCheckpointResetReason = null;
            }
        }
    }

    public void SetCurrentPosition(long? fileOffset, long? recordIndex, string? resetReason = null)
    {
        lock (_gate)
        {
            _status.CurrentFileOffset = fileOffset;
            _status.CurrentRecordIndex = recordIndex;
            _status.LastCheckpointResetReason = resetReason;
        }
    }

    public void ResetCounters()
    {
        lock (_gate)
        {
            _status.RowsProcessed = 0;
            _status.RowsInserted = 0;
            _status.FilesProcessed = 0;
            _status.FilesDiscovered = 0;
        }
    }

    public void BeginFileScan(int filesDiscovered)
    {
        lock (_gate)
        {
            _status.FilesDiscovered = filesDiscovered;
            _status.FilesProcessed = 0;
        }
    }

    public void IncrementFilesProcessed()
    {
        lock (_gate)
        {
            _status.FilesProcessed++;
        }
    }

    public void UpdateRowProgress(long rowsProcessed, long rowsInserted)
    {
        lock (_gate)
        {
            _status.RowsProcessed = rowsProcessed;
            _status.RowsInserted = rowsInserted;
        }
    }

    public void SetLastError(string? error)
    {
        lock (_gate)
        {
            _status.LastError = error;
            if (!string.IsNullOrWhiteSpace(error))
                _status.ServiceState = "Error";
        }
    }

    public ServiceStatusDto Snapshot()
    {
        lock (_gate)
        {
            return new ServiceStatusDto
            {
                ServiceState = _status.ServiceState,
                ServiceStartedAt = _status.ServiceStartedAt,
                LastReloadAt = _status.LastReloadAt,
                CurrentFile = _status.CurrentFile,
                CurrentFileStatus = _status.CurrentFileStatus,
                CurrentFileOffset = _status.CurrentFileOffset,
                CurrentRecordIndex = _status.CurrentRecordIndex,
                LastCheckpointResetReason = _status.LastCheckpointResetReason,
                RowsProcessed = _status.RowsProcessed,
                RowsInserted = _status.RowsInserted,
                FilesProcessed = _status.FilesProcessed,
                FilesDiscovered = _status.FilesDiscovered,
                LastError = _status.LastError,
                ConfigPath = _status.ConfigPath,
                PollIntervalSeconds = _status.PollIntervalSeconds
            };
        }
    }
}
