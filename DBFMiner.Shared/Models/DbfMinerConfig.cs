namespace DBFMiner.Shared.Models;

public sealed class DbfMinerConfig
{
    public const string SelectionModeAll = "all";
    public const string SelectionModeManual = "manual";
    public const string SelectionModeYears = "years";
    public const string SelectionModeMinYear = "minYear";

    public string DbfFolder { get; set; } = "";
    public string DbfSearchPattern { get; set; } = "*.dbf";
    public string DbfSelectionMode { get; set; } = SelectionModeAll;
    public List<string> SelectedDbfFiles { get; set; } = [];
    public List<int> SelectedDbfYears { get; set; } = [];
    public int? MinimumDbfYear { get; set; }

    public int PollIntervalSeconds { get; set; } = 10;

    public ApiConfig Api { get; set; } = new();
    public PostgresConfig Postgres { get; set; } = new();
    public IngestionConfig Ingestion { get; set; } = new();

    public bool IsDbfFileSelected(string filePath)
    {
        var mode = ResolveDbfSelectionMode();

        if (mode == SelectionModeAll)
            return true;

        if (mode == SelectionModeManual)
        {
            if (SelectedDbfFiles.Count == 0)
                return false;

            return SelectedDbfFiles.Any(selected =>
                string.Equals(selected, filePath, StringComparison.OrdinalIgnoreCase));
        }

        if (!TryExtractYearFromDbfPath(filePath, out var fileYear))
            return false;

        if (mode == SelectionModeYears)
            return SelectedDbfYears.Contains(fileYear);

        if (mode == SelectionModeMinYear)
            return !MinimumDbfYear.HasValue || fileYear >= MinimumDbfYear.Value;

        return true;
    }

    public string ResolveDbfSelectionMode()
    {
        if (string.IsNullOrWhiteSpace(DbfSelectionMode))
        {
            if (MinimumDbfYear.HasValue)
                return SelectionModeMinYear;

            if (SelectedDbfYears.Count > 0)
                return SelectionModeYears;

            if (SelectedDbfFiles.Count > 0)
                return SelectionModeManual;

            return SelectionModeAll;
        }

        return DbfSelectionMode switch
        {
            SelectionModeAll => SelectionModeAll,
            SelectionModeManual => SelectionModeManual,
            SelectionModeYears => SelectionModeYears,
            SelectionModeMinYear => SelectionModeMinYear,
            _ => SelectionModeAll
        };
    }

    public static bool TryExtractYearFromDbfPath(string filePath, out int year)
    {
        year = 0;
        if (!TryParseDbfFilePath(filePath, out var info))
            return false;

        year = info.Year;
        return true;
    }

    public static bool TryExtractMeterNumberFromDbfPath(string filePath, out int meterNumber)
    {
        meterNumber = 0;
        if (!TryParseDbfFilePath(filePath, out var info))
            return false;

        meterNumber = info.MeterNumber;
        return true;
    }

    public static bool TryParseDbfFilePath(string filePath, out DbfFileNameInfo info)
    {
        info = null!;

        var fileName = Path.GetFileNameWithoutExtension(filePath);
        if (string.IsNullOrWhiteSpace(fileName) || fileName.Length != 8)
            return false;

        if (!fileName.All(char.IsAsciiDigit))
            return false;

        var meterPart = fileName[..4];
        var yearPart = fileName[4..8];

        if (!int.TryParse(meterPart, out var meterNumber))
            return false;

        if (!int.TryParse(yearPart, out var year) || year is < 1900 or > 2100)
            return false;

        info = new DbfFileNameInfo
        {
            FilePath = filePath,
            FileName = fileName,
            MeterNumber = meterNumber,
            Year = year
        };

        return true;
    }
}

