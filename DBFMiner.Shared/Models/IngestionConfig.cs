using System.Text.Json.Serialization;

namespace DBFMiner.Shared.Models;

public sealed class IngestionConfig
{
    public string DbfRowsTable { get; set; } = "dbf_rows";
    public string IngestionFilesTable { get; set; } = "ingestion_files";
    public int BatchSize { get; set; } = 500;
}