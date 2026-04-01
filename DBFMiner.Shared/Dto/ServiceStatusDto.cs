namespace DBFMiner.Shared.Dto;

public sealed class ServiceStatusDto
{
    public string ServiceState { get; set; } = "Idle"; // Idle | Processing | Error

    public DateTimeOffset ServiceStartedAt { get; set; } = DateTimeOffset.MinValue;
    public DateTimeOffset LastReloadAt { get; set; } = DateTimeOffset.MinValue;

    public string? CurrentFile { get; set; }
    public string? CurrentFileStatus { get; set; } // e.g. processing/done/error
    public long? CurrentFileOffset { get; set; }
    public long? CurrentRecordIndex { get; set; }
    public string? LastCheckpointResetReason { get; set; }

    public long RowsProcessed { get; set; }
    public long RowsInserted { get; set; }
    public long FilesProcessed { get; set; }
    public long FilesDiscovered { get; set; }

    public string? LastError { get; set; }

    public string? ConfigPath { get; set; }
    public int PollIntervalSeconds { get; set; }
}