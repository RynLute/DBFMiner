using DBFMiner.Shared.Models;

namespace DBFMiner.Tests;

public sealed class DbfSelectionTests
{
    [Fact]
    public void IsDbfFileSelected_ReturnsTrue_ForManualModeWhenFileExplicitlySelected()
    {
        var cfg = new DbfMinerConfig
        {
            DbfSelectionMode = DbfMinerConfig.SelectionModeManual,
            SelectedDbfFiles = [@"C:\data\00012021.dbf"]
        };

        Assert.True(cfg.IsDbfFileSelected(@"C:\data\00012021.dbf"));
        Assert.False(cfg.IsDbfFileSelected(@"C:\data\00012022.dbf"));
    }

    [Fact]
    public void IsDbfFileSelected_ReturnsTrue_ForYearsModeWhenYearIsSelected()
    {
        var cfg = new DbfMinerConfig
        {
            DbfSelectionMode = DbfMinerConfig.SelectionModeYears,
            SelectedDbfYears = [2021, 2024]
        };

        Assert.True(cfg.IsDbfFileSelected(@"C:\data\00012021.dbf"));
        Assert.False(cfg.IsDbfFileSelected(@"C:\data\00012022.dbf"));
    }

    [Fact]
    public void IsDbfFileSelected_ReturnsTrue_ForMinYearModeWhenFileYearIsNewEnough()
    {
        var cfg = new DbfMinerConfig
        {
            DbfSelectionMode = DbfMinerConfig.SelectionModeMinYear,
            MinimumDbfYear = 2023
        };

        Assert.False(cfg.IsDbfFileSelected(@"C:\data\00012021.dbf"));
        Assert.True(cfg.IsDbfFileSelected(@"C:\data\00012023.dbf"));
        Assert.True(cfg.IsDbfFileSelected(@"C:\data\00012026.dbf"));
    }

    [Theory]
    [InlineData(@"C:\data\00012021.dbf", 2021)]
    [InlineData(@"C:\data\04562099.dbf", 2099)]
    public void TryExtractYearFromDbfPath_ParsesYearFromFileName(string path, int expectedYear)
    {
        Assert.True(DbfMinerConfig.TryExtractYearFromDbfPath(path, out var year));
        Assert.Equal(expectedYear, year);
    }

    [Theory]
    [InlineData(@"C:\data\00012021.dbf", 1, 2021)]
    [InlineData(@"C:\data\12342026.dbf", 1234, 2026)]
    public void TryParseDbfFilePath_ParsesMeterNumberAndYear(string path, int expectedMeterNumber, int expectedYear)
    {
        Assert.True(DbfMinerConfig.TryParseDbfFilePath(path, out var info));
        Assert.Equal(expectedMeterNumber, info.MeterNumber);
        Assert.Equal(expectedYear, info.Year);
    }

    [Theory]
    [InlineData(@"C:\data\archive_1999.dbf")]
    [InlineData(@"C:\data\meter_2100_backup.dbf")]
    [InlineData(@"C:\data\1232026.dbf")]
    [InlineData(@"C:\data\abcd2026.dbf")]
    public void TryParseDbfFilePath_ReturnsFalse_ForNamesOutsideStrictPattern(string path)
    {
        Assert.False(DbfMinerConfig.TryParseDbfFilePath(path, out _));
    }
}
