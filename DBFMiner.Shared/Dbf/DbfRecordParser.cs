using System.Globalization;
using System.Text;
using DBFMiner.Shared.Models;

namespace DBFMiner.Shared.Dbf;

public sealed class DbfRecordParser
{
    private static readonly string[] RequiredFields =
    {
        "DATE", "TIME", "COUNT", "KOD", "KEY", "AVR", "DNA",
        "PIT", "KTIME", "AVRTIME", "PITTIME", "FS"
    };

    private readonly IReadOnlyDictionary<string, (int Offset, int Length)> _fieldOffsets;
    private readonly int _recordLength;

    public DbfRecordParser(DbfHeaderInfo header)
    {
        _recordLength = header.RecordLength;

        var offsets = new Dictionary<string, (int Offset, int Length)>(StringComparer.OrdinalIgnoreCase);
        var cursor = 1;
        foreach (var field in header.Fields)
        {
            offsets[field.Name] = (cursor, field.Length);
            cursor += field.Length;
        }

        if (cursor != header.RecordLength)
            throw new InvalidDataException(
                $"DBF schema length mismatch. Fields consume {cursor} bytes, record length is {header.RecordLength}.");

        foreach (var requiredField in RequiredFields)
        {
            if (!offsets.ContainsKey(requiredField))
                throw new InvalidDataException($"DBF field '{requiredField}' is missing.");
        }

        _fieldOffsets = offsets;
    }

    public ParsedDbfRow? Parse(ReadOnlySpan<byte> record)
    {
        if (record.Length != _recordLength)
            throw new InvalidDataException($"Expected record length {_recordLength}, got {record.Length}.");

        var deletionFlag = record[0];
        if (deletionFlag == (byte)'*')
            return null;

        if (deletionFlag != (byte)' ')
            throw new InvalidDataException($"Unexpected DBF deletion flag: 0x{deletionFlag:X2}");

        if (IsEmptyPayloadRecord(record))
            return null;

        var date = DateOnly.ParseExact(GetRequiredText(record, "DATE"), "yyyyMMdd", CultureInfo.InvariantCulture);
        var time = TimeOnly.ParseExact(GetRequiredText(record, "TIME"), "HH:mm", CultureInfo.InvariantCulture);
        var fs = ParseRequiredInt(record, "FS");

        return new ParsedDbfRow
        {
            Date = date,
            Time = time,
            Timestamp = date.ToDateTime(time),
            Count = ParseRequiredInt(record, "COUNT"),
            Kod = ParseRequiredInt(record, "KOD"),
            Key = ParseRequiredInt(record, "KEY"),
            Avr = ParseRequiredBool(record, "AVR"),
            Dna = ParseRequiredBool(record, "DNA"),
            Pit = ParseRequiredBool(record, "PIT"),
            Ktime = ParseRequiredBool(record, "KTIME"),
            AvrTime = ParseRequiredInt(record, "AVRTIME"),
            PitTime = ParseRequiredInt(record, "PITTIME"),
            Fs = fs,
            Shift = fs & 0b0000_0011,
            ShiftEnd = (fs & 0b0000_0100) != 0,
            Sensor1Alarm = (fs & 0b0001_0000) != 0,
            Sensor2Alarm = (fs & 0b0010_0000) != 0,
            TimeSync = (fs & 0b0100_0000) != 0,
            ReadAttempt = (fs & 0b1000_0000) != 0
        };
    }

    private string GetRequiredText(ReadOnlySpan<byte> record, string fieldName)
    {
        var text = GetText(record, fieldName);
        if (string.IsNullOrWhiteSpace(text))
            throw new InvalidDataException($"DBF field '{fieldName}' is empty.");

        return text;
    }

    private int ParseRequiredInt(ReadOnlySpan<byte> record, string fieldName)
    {
        var text = GetRequiredText(record, fieldName);
        if (!int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            throw new InvalidDataException($"DBF field '{fieldName}' is not a valid integer: '{text}'.");

        return value;
    }

    private bool ParseRequiredBool(ReadOnlySpan<byte> record, string fieldName)
    {
        return ParseRequiredInt(record, fieldName) switch
        {
            0 => false,
            1 => true,
            var value => throw new InvalidDataException(
                $"DBF field '{fieldName}' must contain 0 or 1, got '{value}'.")
        };
    }

    private string GetText(ReadOnlySpan<byte> record, string fieldName)
    {
        var field = _fieldOffsets[fieldName];
        return Encoding.ASCII.GetString(record.Slice(field.Offset, field.Length)).Trim();
    }

    private bool IsEmptyPayloadRecord(ReadOnlySpan<byte> record)
    {
        return IsBlank(record, "COUNT")
            && IsBlank(record, "KOD")
            && IsBlank(record, "KEY")
            && IsBlank(record, "AVR")
            && IsBlank(record, "DNA")
            && IsBlank(record, "PIT")
            && IsBlank(record, "KTIME")
            && IsBlank(record, "AVRTIME")
            && IsBlank(record, "PITTIME")
            && IsBlank(record, "FS");
    }

    private bool IsBlank(ReadOnlySpan<byte> record, string fieldName)
    {
        return string.IsNullOrWhiteSpace(GetText(record, fieldName));
    }
}
