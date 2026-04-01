using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using DBFMiner.Shared.Models;

namespace DBFMiner.Shared.Dbf;

public static class DbfBinaryReader
{
    public static async Task<DbfHeaderInfo> ReadHeaderAsync(Stream stream, CancellationToken cancellationToken)
    {
        if (!stream.CanSeek)
            throw new InvalidOperationException("DBF stream must support seeking.");

        stream.Position = 0;

        var headerPrefix = new byte[32];
        await stream.ReadExactlyAsync(headerPrefix, cancellationToken).ConfigureAwait(false);

        var version = headerPrefix[0];
        var year = 1900 + headerPrefix[1];
        var month = headerPrefix[2];
        var day = headerPrefix[3];
        var recordCount = BinaryPrimitives.ReadUInt32LittleEndian(headerPrefix.AsSpan(4, 4));
        var headerLength = BinaryPrimitives.ReadUInt16LittleEndian(headerPrefix.AsSpan(8, 2));
        var recordLength = BinaryPrimitives.ReadUInt16LittleEndian(headerPrefix.AsSpan(10, 2));

        if (headerLength < 33)
            throw new InvalidDataException($"Invalid DBF header length: {headerLength}");

        if (recordLength <= 1)
            throw new InvalidDataException($"Invalid DBF record length: {recordLength}");

        var fields = new List<DbfFieldDescriptor>();
        var descriptorBuffer = new byte[32];

        while (stream.Position < headerLength)
        {
            var marker = stream.ReadByte();
            if (marker < 0)
                throw new EndOfStreamException("Unexpected end of DBF header.");

            if (marker == 0x0D)
                break;

            descriptorBuffer[0] = (byte)marker;
            await stream.ReadExactlyAsync(descriptorBuffer.AsMemory(1, 31), cancellationToken).ConfigureAwait(false);

            var name = Encoding.ASCII
                .GetString(descriptorBuffer, 0, 11)
                .TrimEnd('\0', ' ');

            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidDataException("DBF field name is empty.");

            fields.Add(new DbfFieldDescriptor
            {
                Name = name,
                Type = (char)descriptorBuffer[11],
                Length = descriptorBuffer[16],
                DecimalCount = descriptorBuffer[17]
            });
        }

        if (fields.Count == 0)
            throw new InvalidDataException("DBF header does not contain any fields.");

        stream.Position = headerLength;

        return new DbfHeaderInfo
        {
            Version = version,
            LastUpdatedOn = new DateOnly(year, month, day),
            RecordCount = recordCount,
            HeaderLength = headerLength,
            RecordLength = recordLength,
            HeaderHash = ComputeHeaderHash(headerLength, recordLength, fields),
            Fields = fields
        };
    }

    public static long GetFullRecordCount(long fileSize, DbfHeaderInfo header)
    {
        if (fileSize <= header.HeaderLength)
            return 0;

        var countBySize = (fileSize - header.HeaderLength) / header.RecordLength;
        return Math.Min(countBySize, (long)header.RecordCount);
    }

    private static string ComputeHeaderHash(
        int headerLength,
        int recordLength,
        IReadOnlyList<DbfFieldDescriptor> fields)
    {
        var builder = new StringBuilder();
        builder.Append(headerLength)
            .Append('|')
            .Append(recordLength);

        foreach (var field in fields)
        {
            builder.Append('|')
                .Append(field.Name)
                .Append(':')
                .Append(field.Type)
                .Append(':')
                .Append(field.Length)
                .Append(':')
                .Append(field.DecimalCount);
        }

        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(builder.ToString()));
        return Convert.ToHexString(hash);
    }
}
