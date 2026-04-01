using DBFMiner.Shared.Dbf;
using DBFMiner.Shared.Models;

namespace DBFMiner.Service;

public sealed class DbfCheckpointDecision
{
    public required long StartByteOffset { get; init; }
    public required long StartRecordIndex { get; init; }
    public required long FullRecordCount { get; init; }
    public string? ResetReason { get; init; }

    public bool HasNewRecords => FullRecordCount > StartRecordIndex;

    public static DbfCheckpointDecision Create(
        string filePath,
        DbfFileCheckpoint? checkpoint,
        DbfHeaderInfo header,
        long fileSize,
        long fileLastWriteTicks)
    {
        var fullRecordCount = DbfBinaryReader.GetFullRecordCount(fileSize, header);

        if (checkpoint is null)
        {
            return new DbfCheckpointDecision
            {
                StartByteOffset = header.HeaderLength,
                StartRecordIndex = 0,
                FullRecordCount = fullRecordCount,
                ResetReason = null
            };
        }

        var startByteOffset = checkpoint.NextByteOffset;
        if (startByteOffset == 0 && checkpoint.NextRecordIndex == 0)
            startByteOffset = header.HeaderLength;

        if (checkpoint.HeaderLength > 0 && checkpoint.HeaderLength != header.HeaderLength)
            throw CreateCorruption(filePath, "header length changed");

        if (checkpoint.RecordLength > 0 && checkpoint.RecordLength != header.RecordLength)
            throw CreateCorruption(filePath, "record length changed");

        if (!string.IsNullOrWhiteSpace(checkpoint.HeaderHash) &&
            !string.Equals(checkpoint.HeaderHash, header.HeaderHash, StringComparison.Ordinal))
            throw CreateCorruption(filePath, "header hash changed");

        if (checkpoint.FileSize > fileSize)
            throw CreateCorruption(filePath, "file size shrank");

        if (startByteOffset < header.HeaderLength)
            throw CreateCorruption(filePath, "checkpoint offset points inside header");

        if (startByteOffset > fileSize)
            throw CreateCorruption(filePath, "checkpoint offset is beyond file size");

        if ((startByteOffset - header.HeaderLength) % header.RecordLength != 0)
            throw CreateCorruption(filePath, "checkpoint offset is not aligned to record length");

        var inferredRecordIndex = (startByteOffset - header.HeaderLength) / header.RecordLength;
        var startRecordIndex = checkpoint.NextRecordIndex > 0 ? checkpoint.NextRecordIndex : inferredRecordIndex;

        if (startRecordIndex != inferredRecordIndex)
            throw CreateCorruption(filePath, "checkpoint record index does not match checkpoint offset");

        if (checkpoint.LastSeenRecordCount > 0 && fullRecordCount < checkpoint.LastSeenRecordCount)
            throw CreateCorruption(filePath, "record count shrank");

        if (fullRecordCount < startRecordIndex)
            throw CreateCorruption(filePath, "checkpoint record index is beyond the readable record count");

        return new DbfCheckpointDecision
        {
            StartByteOffset = startByteOffset,
            StartRecordIndex = startRecordIndex,
            FullRecordCount = fullRecordCount,
            ResetReason = null
        };
    }

    private static InvalidDataException CreateCorruption(string filePath, string reason)
    {
        return new InvalidDataException(
            $"DBF file '{filePath}' is no longer append-only or checkpoint metadata is invalid: {reason}.");
    }
}
