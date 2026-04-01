namespace DBFMiner.Shared.Models;

public sealed class ParsedDbfRow
{
    public DateOnly Date { get; init; }
    public TimeOnly Time { get; init; }
    public DateTime Timestamp { get; init; }

    public int Count { get; init; }
    public int Kod { get; init; }
    public int Key { get; init; }

    public bool Avr { get; init; }
    public bool Dna { get; init; }
    public bool Pit { get; init; }
    public bool Ktime { get; init; }

    public int AvrTime { get; init; }
    public int PitTime { get; init; }
    public int Fs { get; init; }
    public int Shift { get; init; }

    public bool ShiftEnd { get; init; }
    public bool Sensor1Alarm { get; init; }
    public bool Sensor2Alarm { get; init; }
    public bool TimeSync { get; init; }
    public bool ReadAttempt { get; init; }
}
