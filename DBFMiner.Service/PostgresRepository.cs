using DBFMiner.Shared.Models;
using Npgsql;

namespace DBFMiner.Service;

public sealed class PostgresRepository
{
    private readonly ConfigProvider _configProvider;

    public PostgresRepository(ConfigProvider configProvider)
    {
        _configProvider = configProvider;
    }

    private NpgsqlConnection CreateConnection()
    {
        var cfg = _configProvider.Current;
        var pg = cfg.Postgres;
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = pg.Host,
            Port = pg.Port,
            Database = pg.Database,
            Username = pg.Username,
            Password = pg.Password,
            Pooling = true
        };
        return new NpgsqlConnection(csb.ConnectionString);
    }

    private string Schema => _configProvider.Current.Postgres.Schema;
    private string DbfRowsTable => string.IsNullOrWhiteSpace(_configProvider.Current.Ingestion.DbfRowsTable)
        ? "dbf_rows"
        : _configProvider.Current.Ingestion.DbfRowsTable;
    private string IngestionFilesTable => _configProvider.Current.Ingestion.IngestionFilesTable;

    public async Task EnsureTablesAsync(CancellationToken cancellationToken)
    {
        var schema = Schema;
        var dbfRows = DbfRowsTable;
        var ingestionFiles = IngestionFilesTable;

        var ddl = $@"
CREATE SCHEMA IF NOT EXISTS {QuoteIdent(schema)};

CREATE TABLE IF NOT EXISTS {QuoteIdent(schema)}.{QuoteIdent(dbfRows)} (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    meter_number INTEGER NOT NULL,
    source_year INTEGER NOT NULL,
    row_index BIGINT NOT NULL,
    file_offset BIGINT NOT NULL,
    row_hash TEXT NOT NULL,
    ""date"" DATE NOT NULL,
    ""time"" TIME NOT NULL,
    ""timestamp"" TIMESTAMP NOT NULL,
    ""count"" INTEGER NOT NULL,
    ""kod"" INTEGER NOT NULL,
    ""key"" INTEGER NOT NULL,
    ""avr"" BOOLEAN NOT NULL,
    ""dna"" BOOLEAN NOT NULL,
    ""pit"" BOOLEAN NOT NULL,
    ""ktime"" BOOLEAN NOT NULL,
    ""avrtime"" INTEGER NOT NULL,
    ""pittime"" INTEGER NOT NULL,
    ""fs"" INTEGER NOT NULL,
    ""shift"" INTEGER NOT NULL,
    ""shift_end"" BOOLEAN NOT NULL,
    ""sensor1_alarm"" BOOLEAN NOT NULL,
    ""sensor2_alarm"" BOOLEAN NOT NULL,
    ""time_sync"" BOOLEAN NOT NULL,
    ""read_attempt"" BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS {QuoteIdent($"{dbfRows}_source_file_row_index_uq")}
    ON {QuoteIdent(schema)}.{QuoteIdent(dbfRows)} (source_file, row_index);

ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(dbfRows)} ADD COLUMN IF NOT EXISTS meter_number INTEGER NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(dbfRows)} ADD COLUMN IF NOT EXISTS source_year INTEGER NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(dbfRows)} ADD COLUMN IF NOT EXISTS ""timestamp"" TIMESTAMP NOT NULL DEFAULT TIMESTAMP '2000-01-01 00:00:00';

CREATE TABLE IF NOT EXISTS {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} (
    file_path TEXT PRIMARY KEY,
    file_last_write_ticks BIGINT NOT NULL,
    file_size BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL,
    rows_total BIGINT NOT NULL DEFAULT 0,
    rows_processed BIGINT NOT NULL DEFAULT 0,
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    error_text TEXT NULL,
    header_length INTEGER NOT NULL DEFAULT 0,
    record_length INTEGER NOT NULL DEFAULT 0,
    next_byte_offset BIGINT NOT NULL DEFAULT 0,
    next_record_index BIGINT NOT NULL DEFAULT 0,
    last_seen_record_count BIGINT NOT NULL DEFAULT 0,
    header_hash TEXT NULL,
    reset_reason TEXT NULL,
    last_successful_read_at TIMESTAMPTZ NULL
);

ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS file_size BIGINT NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS header_length INTEGER NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS record_length INTEGER NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS next_byte_offset BIGINT NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS next_record_index BIGINT NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS last_seen_record_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS header_hash TEXT NULL;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS reset_reason TEXT NULL;
ALTER TABLE {QuoteIdent(schema)}.{QuoteIdent(ingestionFiles)} ADD COLUMN IF NOT EXISTS last_successful_read_at TIMESTAMPTZ NULL;
";

        await using var conn = CreateConnection();
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(ddl, conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<DbfFileCheckpoint?> GetFileCheckpointAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
SELECT file_path, file_last_write_ticks, file_size, status, header_length, record_length,
       next_byte_offset, next_record_index, last_seen_record_count, header_hash,
       rows_processed, rows_inserted, error_text, reset_reason
FROM {QuoteIdent(Schema)}.{QuoteIdent(IngestionFilesTable)}
WHERE file_path = @file_path;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("file_path", filePath);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return new DbfFileCheckpoint
        {
            FilePath = reader.GetString(0),
            FileLastWriteTicks = reader.GetInt64(1),
            FileSize = reader.GetInt64(2),
            Status = reader.GetString(3),
            HeaderLength = reader.GetInt32(4),
            RecordLength = reader.GetInt32(5),
            NextByteOffset = reader.GetInt64(6),
            NextRecordIndex = reader.GetInt64(7),
            LastSeenRecordCount = reader.GetInt64(8),
            HeaderHash = reader.IsDBNull(9) ? null : reader.GetString(9),
            RowsProcessed = reader.GetInt64(10),
            RowsInserted = reader.GetInt64(11),
            ErrorText = reader.IsDBNull(12) ? null : reader.GetString(12),
            ResetReason = reader.IsDBNull(13) ? null : reader.GetString(13)
        };
    }

    public async Task MarkFileProcessingAsync(
        string filePath,
        long fileLastWriteTicks,
        long fileSize,
        DbfHeaderInfo header,
        long nextByteOffset,
        long nextRecordIndex,
        long lastSeenRecordCount,
        long rowsProcessed,
        long rowsInserted,
        string? resetReason,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
INSERT INTO {QuoteIdent(Schema)}.{QuoteIdent(IngestionFilesTable)}
    (file_path, file_last_write_ticks, file_size, status, started_at, finished_at, rows_total,
     rows_processed, rows_inserted, error_text, header_length, record_length, next_byte_offset,
     next_record_index, last_seen_record_count, header_hash, reset_reason, last_successful_read_at)
VALUES
    (@file_path, @ticks, @file_size, 'processing', now(), NULL, @rows_total,
     @rows_processed, @rows_inserted, NULL, @header_length, @record_length, @next_byte_offset,
     @next_record_index, @last_seen_record_count, @header_hash, @reset_reason, NULL)
ON CONFLICT (file_path) DO UPDATE SET
    file_last_write_ticks = EXCLUDED.file_last_write_ticks,
    file_size = EXCLUDED.file_size,
    status = EXCLUDED.status,
    started_at = EXCLUDED.started_at,
    finished_at = NULL,
    rows_total = EXCLUDED.rows_total,
    rows_processed = EXCLUDED.rows_processed,
    rows_inserted = EXCLUDED.rows_inserted,
    error_text = NULL,
    header_length = EXCLUDED.header_length,
    record_length = EXCLUDED.record_length,
    next_byte_offset = EXCLUDED.next_byte_offset,
    next_record_index = EXCLUDED.next_record_index,
    last_seen_record_count = EXCLUDED.last_seen_record_count,
    header_hash = EXCLUDED.header_hash,
    reset_reason = EXCLUDED.reset_reason;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("file_path", filePath);
        cmd.Parameters.AddWithValue("ticks", fileLastWriteTicks);
        cmd.Parameters.AddWithValue("file_size", fileSize);
        cmd.Parameters.AddWithValue("rows_total", lastSeenRecordCount);
        cmd.Parameters.AddWithValue("rows_processed", rowsProcessed);
        cmd.Parameters.AddWithValue("rows_inserted", rowsInserted);
        cmd.Parameters.AddWithValue("header_length", header.HeaderLength);
        cmd.Parameters.AddWithValue("record_length", header.RecordLength);
        cmd.Parameters.AddWithValue("next_byte_offset", nextByteOffset);
        cmd.Parameters.AddWithValue("next_record_index", nextRecordIndex);
        cmd.Parameters.AddWithValue("last_seen_record_count", lastSeenRecordCount);
        cmd.Parameters.AddWithValue("header_hash", (object?)header.HeaderHash ?? DBNull.Value);
        cmd.Parameters.AddWithValue("reset_reason", (object?)resetReason ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> PersistBatchAsync(
        string filePath,
        long fileLastWriteTicks,
        long fileSize,
        DbfHeaderInfo header,
        IReadOnlyList<DbfRowWriteModel> rows,
        long nextByteOffset,
        long nextRecordIndex,
        long lastSeenRecordCount,
        long rowsProcessed,
        long rowsInserted,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
            return 0;

        await using var conn = CreateConnection();
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var insertedRows = 0;
        var insertSql = $@"
INSERT INTO {QuoteIdent(Schema)}.{QuoteIdent(DbfRowsTable)}
    (source_file, meter_number, source_year, row_index, file_offset, row_hash, ""date"", ""time"", ""timestamp"", ""count"", ""kod"", ""key"",
     ""avr"", ""dna"", ""pit"", ""ktime"", ""avrtime"", ""pittime"", ""fs"", ""shift"",
     ""shift_end"", ""sensor1_alarm"", ""sensor2_alarm"", ""time_sync"", ""read_attempt"")
VALUES
    (@source_file, @meter_number, @source_year, @row_index, @file_offset, @row_hash, @date, @time, @timestamp, @count, @kod, @key,
     @avr, @dna, @pit, @ktime, @avrtime, @pittime, @fs, @shift,
     @shift_end, @sensor1_alarm, @sensor2_alarm, @time_sync, @read_attempt)
ON CONFLICT (source_file, row_index) DO NOTHING;";

        foreach (var row in rows)
        {
            await using var cmd = new NpgsqlCommand(insertSql, conn, tx);
            cmd.Parameters.AddWithValue("source_file", filePath);
            cmd.Parameters.AddWithValue("meter_number", row.MeterNumber);
            cmd.Parameters.AddWithValue("source_year", row.SourceYear);
            cmd.Parameters.AddWithValue("row_index", row.RowIndex);
            cmd.Parameters.AddWithValue("file_offset", row.FileOffset);
            cmd.Parameters.AddWithValue("row_hash", row.RowHash);
            cmd.Parameters.AddWithValue("date", row.Row.Date);
            cmd.Parameters.AddWithValue("time", row.Row.Time);
            cmd.Parameters.AddWithValue("timestamp", row.Row.Timestamp);
            cmd.Parameters.AddWithValue("count", row.Row.Count);
            cmd.Parameters.AddWithValue("kod", row.Row.Kod);
            cmd.Parameters.AddWithValue("key", row.Row.Key);
            cmd.Parameters.AddWithValue("avr", row.Row.Avr);
            cmd.Parameters.AddWithValue("dna", row.Row.Dna);
            cmd.Parameters.AddWithValue("pit", row.Row.Pit);
            cmd.Parameters.AddWithValue("ktime", row.Row.Ktime);
            cmd.Parameters.AddWithValue("avrtime", row.Row.AvrTime);
            cmd.Parameters.AddWithValue("pittime", row.Row.PitTime);
            cmd.Parameters.AddWithValue("fs", row.Row.Fs);
            cmd.Parameters.AddWithValue("shift", row.Row.Shift);
            cmd.Parameters.AddWithValue("shift_end", row.Row.ShiftEnd);
            cmd.Parameters.AddWithValue("sensor1_alarm", row.Row.Sensor1Alarm);
            cmd.Parameters.AddWithValue("sensor2_alarm", row.Row.Sensor2Alarm);
            cmd.Parameters.AddWithValue("time_sync", row.Row.TimeSync);
            cmd.Parameters.AddWithValue("read_attempt", row.Row.ReadAttempt);
            insertedRows += await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var cumulativeRowsInserted = rowsInserted + insertedRows;

        var updateSql = $@"
UPDATE {QuoteIdent(Schema)}.{QuoteIdent(IngestionFilesTable)}
SET
    file_last_write_ticks = @ticks,
    file_size = @file_size,
    status = 'processing',
    rows_total = @rows_total,
    rows_processed = @rows_processed,
    rows_inserted = @rows_inserted,
    error_text = NULL,
    header_length = @header_length,
    record_length = @record_length,
    next_byte_offset = @next_byte_offset,
    next_record_index = @next_record_index,
    last_seen_record_count = @last_seen_record_count,
    header_hash = @header_hash
WHERE file_path = @file_path;";

        await using (var updateCmd = new NpgsqlCommand(updateSql, conn, tx))
        {
            updateCmd.Parameters.AddWithValue("file_path", filePath);
            updateCmd.Parameters.AddWithValue("ticks", fileLastWriteTicks);
            updateCmd.Parameters.AddWithValue("file_size", fileSize);
            updateCmd.Parameters.AddWithValue("rows_total", lastSeenRecordCount);
            updateCmd.Parameters.AddWithValue("rows_processed", rowsProcessed);
            updateCmd.Parameters.AddWithValue("rows_inserted", cumulativeRowsInserted);
            updateCmd.Parameters.AddWithValue("header_length", header.HeaderLength);
            updateCmd.Parameters.AddWithValue("record_length", header.RecordLength);
            updateCmd.Parameters.AddWithValue("next_byte_offset", nextByteOffset);
            updateCmd.Parameters.AddWithValue("next_record_index", nextRecordIndex);
            updateCmd.Parameters.AddWithValue("last_seen_record_count", lastSeenRecordCount);
            updateCmd.Parameters.AddWithValue("header_hash", (object?)header.HeaderHash ?? DBNull.Value);
            await updateCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return insertedRows;
    }

    public async Task MarkFileCompletedAsync(
        string filePath,
        long fileLastWriteTicks,
        long fileSize,
        DbfHeaderInfo header,
        long nextByteOffset,
        long nextRecordIndex,
        long lastSeenRecordCount,
        long rowsProcessed,
        long rowsInserted,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
UPDATE {QuoteIdent(Schema)}.{QuoteIdent(IngestionFilesTable)}
SET
    status = 'done',
    file_last_write_ticks = @ticks,
    file_size = @file_size,
    finished_at = now(),
    rows_total = @rows_total,
    rows_processed = @rows_processed,
    rows_inserted = @rows_inserted,
    error_text = NULL,
    header_length = @header_length,
    record_length = @record_length,
    next_byte_offset = @next_byte_offset,
    next_record_index = @next_record_index,
    last_seen_record_count = @last_seen_record_count,
    header_hash = @header_hash,
    last_successful_read_at = now()
WHERE file_path = @file_path;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("file_path", filePath);
        cmd.Parameters.AddWithValue("ticks", fileLastWriteTicks);
        cmd.Parameters.AddWithValue("file_size", fileSize);
        cmd.Parameters.AddWithValue("rows_total", lastSeenRecordCount);
        cmd.Parameters.AddWithValue("rows_processed", rowsProcessed);
        cmd.Parameters.AddWithValue("rows_inserted", rowsInserted);
        cmd.Parameters.AddWithValue("header_length", header.HeaderLength);
        cmd.Parameters.AddWithValue("record_length", header.RecordLength);
        cmd.Parameters.AddWithValue("next_byte_offset", nextByteOffset);
        cmd.Parameters.AddWithValue("next_record_index", nextRecordIndex);
        cmd.Parameters.AddWithValue("last_seen_record_count", lastSeenRecordCount);
        cmd.Parameters.AddWithValue("header_hash", (object?)header.HeaderHash ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task UpdateFileErrorAsync(
        string filePath,
        long fileLastWriteTicks,
        long fileSize,
        DbfHeaderInfo? header,
        string errorText,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
INSERT INTO {QuoteIdent(Schema)}.{QuoteIdent(IngestionFilesTable)}
    (file_path, file_last_write_ticks, file_size, status, started_at, finished_at, rows_total,
     rows_processed, rows_inserted, error_text, header_length, record_length, next_byte_offset,
     next_record_index, last_seen_record_count, header_hash, reset_reason, last_successful_read_at)
VALUES
    (@file_path, @ticks, @file_size, 'error', now(), now(), 0,
     0, 0, @error_text, @header_length, @record_length, @next_byte_offset,
     @next_record_index, 0, @header_hash, NULL, NULL)
ON CONFLICT (file_path) DO UPDATE SET
    file_last_write_ticks = EXCLUDED.file_last_write_ticks,
    file_size = EXCLUDED.file_size,
    status = EXCLUDED.status,
    finished_at = EXCLUDED.finished_at,
    error_text = EXCLUDED.error_text,
    header_length = EXCLUDED.header_length,
    record_length = EXCLUDED.record_length,
    header_hash = EXCLUDED.header_hash;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("file_path", filePath);
        cmd.Parameters.AddWithValue("ticks", fileLastWriteTicks);
        cmd.Parameters.AddWithValue("file_size", fileSize);
        cmd.Parameters.AddWithValue("error_text", errorText);
        cmd.Parameters.AddWithValue("header_length", header?.HeaderLength ?? 0);
        cmd.Parameters.AddWithValue("record_length", header?.RecordLength ?? 0);
        cmd.Parameters.AddWithValue("next_byte_offset", header?.HeaderLength ?? 0);
        cmd.Parameters.AddWithValue("next_record_index", 0L);
        cmd.Parameters.AddWithValue("header_hash", (object?)header?.HeaderHash ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string QuoteIdent(string ident)
    {
        return "\"" + ident.Replace("\"", "\"\"") + "\"";
    }
}
