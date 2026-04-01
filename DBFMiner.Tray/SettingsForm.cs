using System.Text;
using System.Text.Json;
using DBFMiner.Shared.Dbf;
using DBFMiner.Shared.Dto;
using DBFMiner.Shared.Models;
using DBFMiner.Shared.Serialization;
using Npgsql;

namespace DBFMiner.Tray;

public sealed partial class SettingsForm : Form
{
    private readonly string _configPath;
    private readonly HttpClient _httpClient;

    private readonly TextBox _dbfFolderTextBox;
    private readonly TextBox _dbfPatternTextBox;
    private readonly ComboBox _selectionModeComboBox;
    private readonly CheckedListBox _dbfFilesCheckedListBox;
    private readonly CheckedListBox _dbfYearsCheckedListBox;
    private readonly NumericUpDown _minimumYearNumeric;
    private readonly Label _previewSummaryLabel;

    private readonly NumericUpDown _pollIntervalNumeric;
    private readonly NumericUpDown _batchSizeNumeric;
    private readonly TextBox _apiHostTextBox;
    private readonly NumericUpDown _apiPortNumeric;

    private readonly TextBox _pgHostTextBox;
    private readonly NumericUpDown _pgPortNumeric;
    private readonly TextBox _pgDatabaseTextBox;
    private readonly TextBox _pgUsernameTextBox;
    private readonly TextBox _pgPasswordTextBox;
    private readonly TextBox _pgSchemaTextBox;

    private readonly Button _browseFolderButton;
    private readonly Button _previewFilesButton;
    private readonly Button _selectAllFilesButton;
    private readonly Button _clearSelectedFilesButton;
    private readonly Button _selectAllYearsButton;
    private readonly Button _clearYearsButton;
    private readonly Button _checkPostgresButton;
    private readonly Button _checkPipelineButton;
    private readonly Button _applyButton;
    private readonly Button _closeButton;

    private IReadOnlyList<string> _previewFiles = Array.Empty<string>();
    private IReadOnlyList<int> _previewYears = Array.Empty<int>();
    private string? _previewFolder;
    private string? _previewPattern;
    private HashSet<string> _selectedPreviewFiles = new(StringComparer.OrdinalIgnoreCase);
    private HashSet<int> _selectedPreviewYears = new();

    public event EventHandler? ReloadRequested;

    public SettingsForm(string configPath, HttpClient httpClient)
    {
        _configPath = configPath;
        _httpClient = httpClient;

        Text = "DBF Miner Settings";
        StartPosition = FormStartPosition.CenterParent;
        MinimumSize = new Size(1040, 840);
        Width = 1120;
        Height = 880;

        var root = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 2,
            Padding = new Padding(12)
        };
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        var tabs = new TabControl
        {
            Dock = DockStyle.Fill
        };

        _dbfFolderTextBox = CreateTextBox();
        _dbfPatternTextBox = CreateTextBox("*.dbf");
        _selectionModeComboBox = new ComboBox
        {
            Dock = DockStyle.Left,
            Width = 260,
            DropDownStyle = ComboBoxStyle.DropDownList
        };
        _selectionModeComboBox.Items.AddRange(
        [
            "All matching files",
            "Only selected files",
            "Only selected years",
            "Only files from min year"
        ]);
        _selectionModeComboBox.SelectedIndex = 0;
        _selectionModeComboBox.SelectedIndexChanged += (_, _) => UpdateSelectionModeUi();

        _dbfFilesCheckedListBox = new CheckedListBox
        {
            Dock = DockStyle.Fill,
            Height = 220,
            CheckOnClick = true,
            HorizontalScrollbar = true
        };
        _dbfFilesCheckedListBox.ItemCheck += (_, _) =>
        {
            BeginInvoke(new Action(() =>
            {
                if (GetSelectionMode() == DbfMinerConfig.SelectionModeManual)
                    _selectedPreviewFiles = GetCheckedPreviewFiles();
            }));
        };
        _dbfYearsCheckedListBox = new CheckedListBox
        {
            Dock = DockStyle.Fill,
            Height = 120,
            CheckOnClick = true,
            HorizontalScrollbar = true
        };
        _dbfYearsCheckedListBox.ItemCheck += (_, _) =>
        {
            BeginInvoke(new Action(() =>
            {
                if (GetSelectionMode() == DbfMinerConfig.SelectionModeYears)
                    _selectedPreviewYears = GetCheckedPreviewYears();
            }));
        };
        _minimumYearNumeric = CreateNumeric(1900, 2100, DateTime.Now.Year);

        _previewSummaryLabel = new Label
        {
            AutoSize = true,
            Text = "Use Preview to see which DBF files match the selected folder and pattern."
        };

        _pollIntervalNumeric = CreateNumeric(1, 3600, 10);
        _batchSizeNumeric = CreateNumeric(1, 50000, 500);
        _apiHostTextBox = CreateTextBox("127.0.0.1");
        _apiPortNumeric = CreateNumeric(1, 65535, 5055);

        _pgHostTextBox = CreateTextBox("localhost");
        _pgPortNumeric = CreateNumeric(1, 65535, 5432);
        _pgDatabaseTextBox = CreateTextBox("postgres");
        _pgUsernameTextBox = CreateTextBox("postgres");
        _pgPasswordTextBox = CreateTextBox("postgres");
        _pgPasswordTextBox.UseSystemPasswordChar = true;
        _pgSchemaTextBox = CreateTextBox("public");

        _browseFolderButton = new Button { AutoSize = true, Text = "Browse..." };
        _browseFolderButton.Click += (_, _) => BrowseFolder();

        _previewFilesButton = new Button { AutoSize = true, Text = "Preview DBF Files" };
        _previewFilesButton.Click += (_, _) => RefreshDbfPreview();

        _selectAllFilesButton = new Button { AutoSize = true, Text = "Select All" };
        _selectAllFilesButton.Click += (_, _) => SetAllPreviewItemsChecked(true);

        _clearSelectedFilesButton = new Button { AutoSize = true, Text = "Clear" };
        _clearSelectedFilesButton.Click += (_, _) => SetAllPreviewItemsChecked(false);
        _selectAllYearsButton = new Button { AutoSize = true, Text = "Select All Years" };
        _selectAllYearsButton.Click += (_, _) => SetAllPreviewYearsChecked(true);
        _clearYearsButton = new Button { AutoSize = true, Text = "Clear Years" };
        _clearYearsButton.Click += (_, _) => SetAllPreviewYearsChecked(false);

        _checkPostgresButton = new Button { AutoSize = true, Text = "Check PostgreSQL" };
        _checkPostgresButton.Click += async (_, _) => await CheckPostgresAsync();

        _checkPipelineButton = new Button { AutoSize = true, Text = "Check Full Pipeline" };
        _checkPipelineButton.Click += async (_, _) => await CheckFullPipelineAsync();

        _applyButton = new Button { AutoSize = true, Text = "Apply" };
        _applyButton.Click += async (_, _) => await ApplyAsync();

        _closeButton = new Button { AutoSize = true, Text = "Close" };
        _closeButton.Click += (_, _) => Close();

        tabs.TabPages.Add(BuildSourceTab());
        tabs.TabPages.Add(BuildServiceTab());
        tabs.TabPages.Add(BuildPostgresTab());

        var buttonsPanel = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.RightToLeft,
            AutoSize = true,
            WrapContents = false
        };
        buttonsPanel.Controls.Add(_applyButton);
        buttonsPanel.Controls.Add(_closeButton);

        root.Controls.Add(tabs, 0, 0);
        root.Controls.Add(buttonsPanel, 0, 1);

        Controls.Add(root);

        Load += async (_, _) => await LoadConfigAsync();
    }

    private TabPage BuildSourceTab()
    {
        var tab = new TabPage("Source");
        var container = new Panel
        {
            Dock = DockStyle.Fill,
            AutoScroll = true,
            Padding = new Padding(12)
        };

        var group = CreateGroupBox("DBF Source");
        var layout = CreateGroupLayout(3);

        var folderActions = new FlowLayoutPanel { Dock = DockStyle.Fill, AutoSize = true, WrapContents = false };
        folderActions.Controls.Add(_browseFolderButton);
        folderActions.Controls.Add(_previewFilesButton);

        var selectionActions = new FlowLayoutPanel { Dock = DockStyle.Fill, AutoSize = true, WrapContents = false };
        selectionActions.Controls.Add(_selectAllFilesButton);
        selectionActions.Controls.Add(_clearSelectedFilesButton);
        selectionActions.Controls.Add(_selectAllYearsButton);
        selectionActions.Controls.Add(_clearYearsButton);

        layout.Controls.Add(CreateLabel("DBF folder"), 0, 0);
        layout.Controls.Add(_dbfFolderTextBox, 1, 0);
        layout.Controls.Add(folderActions, 2, 0);

        layout.Controls.Add(CreateLabel("Search pattern"), 0, 1);
        layout.Controls.Add(_dbfPatternTextBox, 1, 1);
        layout.Controls.Add(CreateHintLabel("Top directory only, same behavior as the service."), 2, 1);

        layout.Controls.Add(CreateLabel("Processing mode"), 0, 2);
        layout.Controls.Add(_selectionModeComboBox, 1, 2);
        layout.Controls.Add(selectionActions, 2, 2);

        layout.Controls.Add(CreateLabel("Selected years"), 0, 3);
        layout.Controls.Add(_dbfYearsCheckedListBox, 1, 3);
        layout.SetColumnSpan(_dbfYearsCheckedListBox, 2);

        layout.Controls.Add(CreateLabel("Min year"), 0, 4);
        layout.Controls.Add(_minimumYearNumeric, 1, 4);

        layout.Controls.Add(CreateLabel("DBF files"), 0, 5);
        layout.Controls.Add(_dbfFilesCheckedListBox, 1, 5);
        layout.SetColumnSpan(_dbfFilesCheckedListBox, 2);

        layout.Controls.Add(CreateLabel("Preview summary"), 0, 6);
        layout.Controls.Add(_previewSummaryLabel, 1, 6);
        layout.SetColumnSpan(_previewSummaryLabel, 2);

        group.Controls.Add(layout);
        container.Controls.Add(group);
        tab.Controls.Add(container);
        return tab;
    }

    private TabPage BuildServiceTab()
    {
        var tab = new TabPage("Service");
        var container = new Panel
        {
            Dock = DockStyle.Fill,
            AutoScroll = true,
            Padding = new Padding(12)
        };

        var group = CreateGroupBox("Service");
        var layout = CreateGroupLayout(2);

        layout.Controls.Add(CreateLabel("Poll interval (sec)"), 0, 0);
        layout.Controls.Add(_pollIntervalNumeric, 1, 0);

        layout.Controls.Add(CreateLabel("Batch size"), 0, 1);
        layout.Controls.Add(_batchSizeNumeric, 1, 1);

        layout.Controls.Add(CreateLabel("API host"), 0, 2);
        layout.Controls.Add(_apiHostTextBox, 1, 2);

        layout.Controls.Add(CreateLabel("API port"), 0, 3);
        layout.Controls.Add(_apiPortNumeric, 1, 3);

        group.Controls.Add(layout);
        container.Controls.Add(group);
        tab.Controls.Add(container);
        return tab;
    }

    private TabPage BuildPostgresTab()
    {
        var tab = new TabPage("PostgreSQL");
        var container = new Panel
        {
            Dock = DockStyle.Fill,
            AutoScroll = true,
            Padding = new Padding(12)
        };

        var group = CreateGroupBox("PostgreSQL");
        var layout = CreateGroupLayout(2);

        var probeActions = new FlowLayoutPanel { Dock = DockStyle.Fill, AutoSize = true, WrapContents = false };
        probeActions.Controls.Add(_checkPostgresButton);
        probeActions.Controls.Add(_checkPipelineButton);

        layout.Controls.Add(CreateLabel("Host"), 0, 0);
        layout.Controls.Add(_pgHostTextBox, 1, 0);

        layout.Controls.Add(CreateLabel("Port"), 0, 1);
        layout.Controls.Add(_pgPortNumeric, 1, 1);

        layout.Controls.Add(CreateLabel("Database"), 0, 2);
        layout.Controls.Add(_pgDatabaseTextBox, 1, 2);

        layout.Controls.Add(CreateLabel("Username"), 0, 3);
        layout.Controls.Add(_pgUsernameTextBox, 1, 3);

        layout.Controls.Add(CreateLabel("Password"), 0, 4);
        layout.Controls.Add(_pgPasswordTextBox, 1, 4);

        layout.Controls.Add(CreateLabel("Schema"), 0, 5);
        layout.Controls.Add(_pgSchemaTextBox, 1, 5);

        layout.Controls.Add(CreateLabel("Checks"), 0, 6);
        layout.Controls.Add(probeActions, 1, 6);

        group.Controls.Add(layout);
        container.Controls.Add(group);
        tab.Controls.Add(container);
        return tab;
    }

    private async Task LoadConfigAsync()
    {
        try
        {
            var cfg = await LoadOrCreateConfigAsync().ConfigureAwait(true);
            BindConfig(cfg);
            RefreshDbfPreview();
        }
        catch (Exception ex)
        {
            MessageBox.Show(ex.Message, "Settings", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private async Task<DbfMinerConfig> LoadOrCreateConfigAsync()
    {
        if (!File.Exists(_configPath))
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_configPath)!);
            var defaultConfig = CreateDefaultConfig();
            var defaultJson = JsonSerializer.Serialize(defaultConfig, SharedJson.Indented);
            await File.WriteAllTextAsync(_configPath, defaultJson).ConfigureAwait(true);
            return defaultConfig;
        }

        var json = await File.ReadAllTextAsync(_configPath).ConfigureAwait(true);
        var cfg = JsonSerializer.Deserialize<DbfMinerConfig>(json, SharedJson.Default);

        if (cfg is null)
            throw new InvalidOperationException("Failed to deserialize config.json");

        return cfg;
    }

    private void BindConfig(DbfMinerConfig cfg)
    {
        _dbfFolderTextBox.Text = cfg.DbfFolder;
        _dbfPatternTextBox.Text = string.IsNullOrWhiteSpace(cfg.DbfSearchPattern) ? "*.dbf" : cfg.DbfSearchPattern;
        _selectedPreviewFiles = new HashSet<string>(cfg.SelectedDbfFiles, StringComparer.OrdinalIgnoreCase);
        _selectedPreviewYears = new HashSet<int>(cfg.SelectedDbfYears);
        _minimumYearNumeric.Value = ClampNumeric(_minimumYearNumeric, cfg.MinimumDbfYear ?? DateTime.Now.Year);
        _selectionModeComboBox.SelectedIndex = cfg.ResolveDbfSelectionMode() switch
        {
            DbfMinerConfig.SelectionModeManual => 1,
            DbfMinerConfig.SelectionModeYears => 2,
            DbfMinerConfig.SelectionModeMinYear => 3,
            _ => 0
        };

        _pollIntervalNumeric.Value = ClampNumeric(_pollIntervalNumeric, cfg.PollIntervalSeconds);
        _batchSizeNumeric.Value = ClampNumeric(_batchSizeNumeric, cfg.Ingestion.BatchSize);
        _apiHostTextBox.Text = cfg.Api.Host;
        _apiPortNumeric.Value = ClampNumeric(_apiPortNumeric, cfg.Api.Port);

        _pgHostTextBox.Text = cfg.Postgres.Host;
        _pgPortNumeric.Value = ClampNumeric(_pgPortNumeric, cfg.Postgres.Port);
        _pgDatabaseTextBox.Text = cfg.Postgres.Database;
        _pgUsernameTextBox.Text = cfg.Postgres.Username;
        _pgPasswordTextBox.Text = cfg.Postgres.Password;
        _pgSchemaTextBox.Text = cfg.Postgres.Schema;

        UpdateSelectionModeUi();
    }

    private DbfMinerConfig CollectConfig()
    {
        var selectionMode = GetSelectionMode();
        var selectedFiles = selectionMode == DbfMinerConfig.SelectionModeManual
            ? GetCheckedPreviewFiles().OrderBy(path => path, StringComparer.OrdinalIgnoreCase).ToList()
            : new List<string>();
        var selectedYears = selectionMode == DbfMinerConfig.SelectionModeYears
            ? GetCheckedPreviewYears().OrderBy(year => year).ToList()
            : new List<int>();
        int? minimumYear = selectionMode == DbfMinerConfig.SelectionModeMinYear
            ? Decimal.ToInt32(_minimumYearNumeric.Value)
            : null;

        if (selectionMode != DbfMinerConfig.SelectionModeManual)
            selectedFiles.Clear();

        return new DbfMinerConfig
        {
            DbfFolder = _dbfFolderTextBox.Text.Trim(),
            DbfSearchPattern = string.IsNullOrWhiteSpace(_dbfPatternTextBox.Text) ? "*.dbf" : _dbfPatternTextBox.Text.Trim(),
            DbfSelectionMode = selectionMode,
            SelectedDbfFiles = selectedFiles,
            SelectedDbfYears = selectedYears,
            MinimumDbfYear = minimumYear,
            PollIntervalSeconds = Decimal.ToInt32(_pollIntervalNumeric.Value),
            Api = new ApiConfig
            {
                Host = _apiHostTextBox.Text.Trim(),
                Port = Decimal.ToInt32(_apiPortNumeric.Value)
            },
            Postgres = CollectPostgresConfig(),
            Ingestion = new IngestionConfig
            {
                BatchSize = Decimal.ToInt32(_batchSizeNumeric.Value)
            }
        };
    }

    private PostgresConfig CollectPostgresConfig()
    {
        return new PostgresConfig
        {
            Host = _pgHostTextBox.Text.Trim(),
            Port = Decimal.ToInt32(_pgPortNumeric.Value),
            Database = _pgDatabaseTextBox.Text.Trim(),
            Username = _pgUsernameTextBox.Text.Trim(),
            Password = _pgPasswordTextBox.Text,
            Schema = _pgSchemaTextBox.Text.Trim()
        };
    }

    private async Task ApplyAsync()
    {
        try
        {
            ValidateInputs();

            var cfg = CollectConfig();
            var preview = EnsurePreviewFor(cfg.DbfFolder, cfg.DbfSearchPattern);
            var effectiveFiles = ResolveFilesForProcessing(preview);
            var confirmText = BuildConfirmationText(cfg, preview, effectiveFiles);

            if (MessageBox.Show(confirmText, "Confirm Settings", MessageBoxButtons.YesNo, MessageBoxIcon.Question) != DialogResult.Yes)
                return;

            var text = JsonSerializer.Serialize(cfg, SharedJson.Indented);

            Directory.CreateDirectory(Path.GetDirectoryName(_configPath)!);
            await File.WriteAllTextAsync(_configPath, text).ConfigureAwait(true);

            var url = $"http://{cfg.Api.Host}:{cfg.Api.Port}/api/config/reload";
            using var resp = await _httpClient.PostAsync(url, content: null).ConfigureAwait(true);
            var body = await resp.Content.ReadAsStringAsync().ConfigureAwait(true);

            if (!resp.IsSuccessStatusCode)
                throw new InvalidOperationException($"Reload failed: {body}");

            var reloadResp = JsonSerializer.Deserialize<ReloadResponseDto>(body, SharedJson.Default);

            MessageBox.Show(
                reloadResp?.Message ?? "Configuration applied",
                "Settings",
                MessageBoxButtons.OK,
                reloadResp?.Ok == true ? MessageBoxIcon.Information : MessageBoxIcon.Warning);

            ReloadRequested?.Invoke(this, EventArgs.Empty);
        }
        catch (Exception ex)
        {
            MessageBox.Show(ex.Message, "Apply", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private async Task CheckPostgresAsync()
    {
        try
        {
            ValidatePostgresInputs();
            SetBusyState(true);

            var pg = CollectPostgresConfig();
            var serverInfo = await EnsureTargetDatabaseAsync(pg).ConfigureAwait(true);
            var report = await RunPostgresWriteReadProbeAsync(pg, serverInfo).ConfigureAwait(true);

            MessageBox.Show(report, "PostgreSQL Check", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show(ex.Message, "PostgreSQL Check", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        finally
        {
            SetBusyState(false);
        }
    }

    private async Task CheckFullPipelineAsync()
    {
        try
        {
            ValidateInputs();
            SetBusyState(true);

            var cfg = CollectConfig();
            var preview = EnsurePreviewFor(cfg.DbfFolder, cfg.DbfSearchPattern);
            var effectiveFiles = ResolveFilesForProcessing(preview);
            if (effectiveFiles.Count == 0)
                throw new InvalidOperationException("No DBF files are selected for processing.");

            var sample = await ReadDbfSampleAsync(effectiveFiles[0]).ConfigureAwait(true);
            var serverInfo = await EnsureTargetDatabaseAsync(cfg.Postgres).ConfigureAwait(true);
            var report = await RunFullPipelineProbeAsync(cfg.Postgres, serverInfo, sample).ConfigureAwait(true);

            MessageBox.Show(report, "Full Pipeline Check", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show(ex.Message, "Full Pipeline Check", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        finally
        {
            SetBusyState(false);
        }
    }

    private async Task<PostgresServerInfo> EnsureTargetDatabaseAsync(PostgresConfig pg)
    {
        var adminBuilder = CreateConnectionString(pg, databaseOverride: "postgres");
        await using var adminConn = new NpgsqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync().ConfigureAwait(true);

        string serverVersion;
        string currentUser;
        await using (var infoCmd = new NpgsqlCommand("select version(), current_user;", adminConn))
        await using (var reader = await infoCmd.ExecuteReaderAsync().ConfigureAwait(true))
        {
            if (!await reader.ReadAsync().ConfigureAwait(true))
                throw new InvalidOperationException("PostgreSQL server responded without version info.");

            serverVersion = reader.GetString(0);
            currentUser = reader.GetString(1);
        }

        bool databaseExists;
        await using (var existsCmd = new NpgsqlCommand("select exists(select 1 from pg_database where datname = @db);", adminConn))
        {
            existsCmd.Parameters.AddWithValue("db", pg.Database);
            databaseExists = (bool)(await existsCmd.ExecuteScalarAsync().ConfigureAwait(true)
                ?? throw new InvalidOperationException("Failed to check database existence."));
        }

        var databaseCreated = false;
        if (!databaseExists)
        {
            var confirm = MessageBox.Show(
                $"Database '{pg.Database}' does not exist on {pg.Host}:{pg.Port}.{Environment.NewLine}{Environment.NewLine}Create it now?",
                "Create Database",
                MessageBoxButtons.YesNo,
                MessageBoxIcon.Question);

            if (confirm != DialogResult.Yes)
                throw new InvalidOperationException("Database check cancelled because the target database does not exist.");

            await using var createDbCmd = new NpgsqlCommand(
                $"create database {QuoteIdentifier(pg.Database)};",
                adminConn);
            await createDbCmd.ExecuteNonQueryAsync().ConfigureAwait(true);
            databaseCreated = true;
        }

        return new PostgresServerInfo
        {
            ServerVersion = serverVersion,
            CurrentUser = currentUser,
            DatabaseCreated = databaseCreated
        };
    }

    private async Task<string> RunPostgresWriteReadProbeAsync(PostgresConfig pg, PostgresServerInfo serverInfo)
    {
        await using var conn = new NpgsqlConnection(CreateConnectionString(pg).ConnectionString);
        await conn.OpenAsync().ConfigureAwait(true);

        await EnsureSchemaExistsAsync(conn, pg.Schema).ConfigureAwait(true);

        var tableName = "__dbfmk_pg_probe_" + Guid.NewGuid().ToString("N");
        var qualifiedTable = $"{QuoteIdentifier(pg.Schema)}.{QuoteIdentifier(tableName)}";
        var payload = "postgres-probe-" + Guid.NewGuid().ToString("N");

        await using var tx = await conn.BeginTransactionAsync().ConfigureAwait(true);

        await using (var createCmd = new NpgsqlCommand(
                         $"create table {qualifiedTable} (id integer primary key, payload text not null);",
                         conn,
                         tx))
        {
            await createCmd.ExecuteNonQueryAsync().ConfigureAwait(true);
        }

        await using (var insertCmd = new NpgsqlCommand(
                         $"insert into {qualifiedTable} (id, payload) values (1, @payload);",
                         conn,
                         tx))
        {
            insertCmd.Parameters.AddWithValue("payload", payload);
            await insertCmd.ExecuteNonQueryAsync().ConfigureAwait(true);
        }

        string? readPayload;
        await using (var readCmd = new NpgsqlCommand(
                         $"select payload from {qualifiedTable} where id = 1;",
                         conn,
                         tx))
        {
            readPayload = (string?)await readCmd.ExecuteScalarAsync().ConfigureAwait(true);
        }

        if (!string.Equals(payload, readPayload, StringComparison.Ordinal))
            throw new InvalidOperationException("PostgreSQL probe returned unexpected data.");

        await tx.RollbackAsync().ConfigureAwait(true);

        var report = new StringBuilder();
        report.AppendLine("PostgreSQL check completed successfully.")
            .AppendLine()
            .AppendLine($"Server: {pg.Host}:{pg.Port}")
            .AppendLine($"Database: {pg.Database}")
            .AppendLine($"Schema: {pg.Schema}")
            .AppendLine($"User: {serverInfo.CurrentUser}")
            .AppendLine($"Database created now: {serverInfo.DatabaseCreated}")
            .AppendLine("Schema check: ok")
            .AppendLine("Write test: ok")
            .AppendLine("Read test: ok")
            .AppendLine("Cleanup: rollback ok")
            .AppendLine()
            .AppendLine($"Server version: {serverInfo.ServerVersion}");

        return report.ToString();
    }

    private async Task<string> RunFullPipelineProbeAsync(
        PostgresConfig pg,
        PostgresServerInfo serverInfo,
        DbfProbeSample sample)
    {
        await using var conn = new NpgsqlConnection(CreateConnectionString(pg).ConnectionString);
        await conn.OpenAsync().ConfigureAwait(true);

        await EnsureSchemaExistsAsync(conn, pg.Schema).ConfigureAwait(true);

        var rowsTableName = "__dbfmk_pipeline_rows_" + Guid.NewGuid().ToString("N");
        var checkpointsTableName = "__dbfmk_pipeline_checkpoints_" + Guid.NewGuid().ToString("N");
        var qualifiedRowsTable = $"{QuoteIdentifier(pg.Schema)}.{QuoteIdentifier(rowsTableName)}";
        var qualifiedCheckpointsTable = $"{QuoteIdentifier(pg.Schema)}.{QuoteIdentifier(checkpointsTableName)}";

        await using var tx = await conn.BeginTransactionAsync().ConfigureAwait(true);

        await using (var createRowsCmd = new NpgsqlCommand(
                         $@"
create table {qualifiedRowsTable} (
    source_file text not null,
    meter_number integer not null,
    source_year integer not null,
    row_index bigint not null,
    file_offset bigint not null,
    row_hash text not null,
    ""date"" date not null,
    ""time"" time not null,
    ""timestamp"" timestamp not null,
    ""count"" integer not null,
    ""kod"" integer not null,
    ""key"" integer not null,
    ""avr"" boolean not null,
    ""dna"" boolean not null,
    ""pit"" boolean not null,
    ""ktime"" boolean not null,
    ""avrtime"" integer not null,
    ""pittime"" integer not null,
    ""fs"" integer not null,
    ""shift"" integer not null,
    ""shift_end"" boolean not null,
    ""sensor1_alarm"" boolean not null,
    ""sensor2_alarm"" boolean not null,
    ""time_sync"" boolean not null,
    ""read_attempt"" boolean not null,
    primary key (source_file, row_index)
);",
                         conn,
                         tx))
        {
            await createRowsCmd.ExecuteNonQueryAsync().ConfigureAwait(true);
        }

        await using (var createCheckpointCmd = new NpgsqlCommand(
                         $@"
create table {qualifiedCheckpointsTable} (
    file_path text primary key,
    next_byte_offset bigint not null,
    next_record_index bigint not null,
    header_hash text not null
);",
                         conn,
                         tx))
        {
            await createCheckpointCmd.ExecuteNonQueryAsync().ConfigureAwait(true);
        }

        await using (var insertRowCmd = new NpgsqlCommand(
                         $@"
insert into {qualifiedRowsTable}
    (source_file, meter_number, source_year, row_index, file_offset, row_hash, ""date"", ""time"", ""timestamp"", ""count"", ""kod"", ""key"",
     ""avr"", ""dna"", ""pit"", ""ktime"", ""avrtime"", ""pittime"", ""fs"", ""shift"",
     ""shift_end"", ""sensor1_alarm"", ""sensor2_alarm"", ""time_sync"", ""read_attempt"")
values
    (@source_file, @meter_number, @source_year, @row_index, @file_offset, @row_hash, @date, @time, @timestamp, @count, @kod, @key,
     @avr, @dna, @pit, @ktime, @avrtime, @pittime, @fs, @shift,
     @shift_end, @sensor1_alarm, @sensor2_alarm, @time_sync, @read_attempt);",
                         conn,
                         tx))
        {
            FillProbeRowParameters(insertRowCmd, sample);
            await insertRowCmd.ExecuteNonQueryAsync().ConfigureAwait(true);
        }

        await using (var insertCheckpointCmd = new NpgsqlCommand(
                         $@"
insert into {qualifiedCheckpointsTable} (file_path, next_byte_offset, next_record_index, header_hash)
values (@file_path, @next_byte_offset, @next_record_index, @header_hash);",
                         conn,
                         tx))
        {
            insertCheckpointCmd.Parameters.AddWithValue("file_path", sample.FilePath);
            insertCheckpointCmd.Parameters.AddWithValue("next_byte_offset", sample.NextByteOffset);
            insertCheckpointCmd.Parameters.AddWithValue("next_record_index", sample.RowIndex);
            insertCheckpointCmd.Parameters.AddWithValue("header_hash", sample.Header.HeaderHash);
            await insertCheckpointCmd.ExecuteNonQueryAsync().ConfigureAwait(true);
        }

        int readCount;
        int readFs;
        int readMeterNumber;
        int readSourceYear;
        DateTime readTimestamp;
        await using (var readBackCmd = new NpgsqlCommand(
                         $@"select meter_number, source_year, ""timestamp"", ""count"", ""fs"" from {qualifiedRowsTable}
where source_file = @source_file and row_index = @row_index;",
                         conn,
                         tx))
        {
            readBackCmd.Parameters.AddWithValue("source_file", sample.FilePath);
            readBackCmd.Parameters.AddWithValue("row_index", sample.RowIndex);

            await using var reader = await readBackCmd.ExecuteReaderAsync().ConfigureAwait(true);
            if (!await reader.ReadAsync().ConfigureAwait(true))
                throw new InvalidOperationException("Full pipeline probe failed: row was not found after insert.");

            readMeterNumber = reader.GetInt32(0);
            readSourceYear = reader.GetInt32(1);
            readTimestamp = reader.GetDateTime(2);
            readCount = reader.GetInt32(3);
            readFs = reader.GetInt32(4);
        }

        if (readMeterNumber != sample.MeterNumber ||
            readSourceYear != sample.SourceYear ||
            readTimestamp != sample.Row.Timestamp ||
            readCount != sample.Row.Count ||
            readFs != sample.Row.Fs)
            throw new InvalidOperationException("Full pipeline probe failed: row data read from PostgreSQL does not match parsed DBF data.");

        long checkpointOffset;
        await using (var checkpointCmd = new NpgsqlCommand(
                         $@"select next_byte_offset from {qualifiedCheckpointsTable} where file_path = @file_path;",
                         conn,
                         tx))
        {
            checkpointCmd.Parameters.AddWithValue("file_path", sample.FilePath);
            checkpointOffset = (long)(await checkpointCmd.ExecuteScalarAsync().ConfigureAwait(true)
                ?? throw new InvalidOperationException("Full pipeline probe failed: checkpoint was not stored."));
        }

        if (checkpointOffset != sample.NextByteOffset)
            throw new InvalidOperationException("Full pipeline probe failed: checkpoint offset does not match expected value.");

        await tx.RollbackAsync().ConfigureAwait(true);

        var report = new StringBuilder();
        report.AppendLine("Full pipeline check completed successfully.")
            .AppendLine()
            .AppendLine($"DBF file: {sample.FilePath}")
            .AppendLine($"Meter number: {sample.MeterNumber}")
            .AppendLine($"Source year: {sample.SourceYear}")
            .AppendLine($"Parsed row index: {sample.RowIndex}")
            .AppendLine($"File offset: {sample.FileOffset}")
            .AppendLine($"Timestamp: {sample.Row.Timestamp:yyyy-MM-dd HH:mm:ss}")
            .AppendLine($"FS: {sample.Row.Fs} | Shift: {sample.Row.Shift} | ShiftEnd: {sample.Row.ShiftEnd}")
            .AppendLine($"Next offset: {sample.NextByteOffset}")
            .AppendLine()
            .AppendLine($"PostgreSQL: {pg.Host}:{pg.Port}/{pg.Database}")
            .AppendLine($"Schema: {pg.Schema}")
            .AppendLine($"User: {serverInfo.CurrentUser}")
            .AppendLine($"Database created now: {serverInfo.DatabaseCreated}")
            .AppendLine("DBF read: ok")
            .AppendLine("DBF parse: ok")
            .AppendLine("Checkpoint write: ok")
            .AppendLine("Row write/read: ok")
            .AppendLine("Cleanup: rollback ok");

        return report.ToString();
    }

    private async Task<DbfProbeSample> ReadDbfSampleAsync(string filePath)
    {
        if (!DbfMinerConfig.TryParseDbfFilePath(filePath, out var fileInfo))
            throw new InvalidOperationException(
                $"DBF file name '{Path.GetFileName(filePath)}' must match pattern nnnndddd.dbf.");

        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var header = await DbfBinaryReader.ReadHeaderAsync(stream, CancellationToken.None).ConfigureAwait(true);
        var parser = new DbfRecordParser(header);
        var fullRecordCount = DbfBinaryReader.GetFullRecordCount(stream.Length, header);
        var buffer = new byte[header.RecordLength];

        stream.Position = header.HeaderLength;

        for (long index = 0; index < fullRecordCount; index++)
        {
            var fileOffset = stream.Position;
            await stream.ReadExactlyAsync(buffer, CancellationToken.None).ConfigureAwait(true);

            var row = parser.Parse(buffer);
            if (row is null)
                continue;

            return new DbfProbeSample
            {
                FilePath = filePath,
                MeterNumber = fileInfo.MeterNumber,
                SourceYear = fileInfo.Year,
                Header = header,
                Row = row,
                RowIndex = index + 1,
                FileOffset = fileOffset,
                NextByteOffset = fileOffset + header.RecordLength,
                RowHash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(buffer))
            };
        }

        throw new InvalidOperationException($"No active DBF records were found in '{filePath}'.");
    }

    private static void FillProbeRowParameters(NpgsqlCommand command, DbfProbeSample sample)
    {
        command.Parameters.AddWithValue("source_file", sample.FilePath);
        command.Parameters.AddWithValue("meter_number", sample.MeterNumber);
        command.Parameters.AddWithValue("source_year", sample.SourceYear);
        command.Parameters.AddWithValue("row_index", sample.RowIndex);
        command.Parameters.AddWithValue("file_offset", sample.FileOffset);
        command.Parameters.AddWithValue("row_hash", sample.RowHash);
        command.Parameters.AddWithValue("date", sample.Row.Date);
        command.Parameters.AddWithValue("time", sample.Row.Time);
        command.Parameters.AddWithValue("timestamp", sample.Row.Timestamp);
        command.Parameters.AddWithValue("count", sample.Row.Count);
        command.Parameters.AddWithValue("kod", sample.Row.Kod);
        command.Parameters.AddWithValue("key", sample.Row.Key);
        command.Parameters.AddWithValue("avr", sample.Row.Avr);
        command.Parameters.AddWithValue("dna", sample.Row.Dna);
        command.Parameters.AddWithValue("pit", sample.Row.Pit);
        command.Parameters.AddWithValue("ktime", sample.Row.Ktime);
        command.Parameters.AddWithValue("avrtime", sample.Row.AvrTime);
        command.Parameters.AddWithValue("pittime", sample.Row.PitTime);
        command.Parameters.AddWithValue("fs", sample.Row.Fs);
        command.Parameters.AddWithValue("shift", sample.Row.Shift);
        command.Parameters.AddWithValue("shift_end", sample.Row.ShiftEnd);
        command.Parameters.AddWithValue("sensor1_alarm", sample.Row.Sensor1Alarm);
        command.Parameters.AddWithValue("sensor2_alarm", sample.Row.Sensor2Alarm);
        command.Parameters.AddWithValue("time_sync", sample.Row.TimeSync);
        command.Parameters.AddWithValue("read_attempt", sample.Row.ReadAttempt);
    }

    private static async Task EnsureSchemaExistsAsync(NpgsqlConnection conn, string schema)
    {
        await using var cmd = new NpgsqlCommand(
            $"create schema if not exists {QuoteIdentifier(schema)};",
            conn);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(true);
    }

    private static NpgsqlConnectionStringBuilder CreateConnectionString(PostgresConfig pg, string? databaseOverride = null)
    {
        return new NpgsqlConnectionStringBuilder
        {
            Host = pg.Host,
            Port = pg.Port,
            Database = string.IsNullOrWhiteSpace(databaseOverride) ? pg.Database : databaseOverride,
            Username = pg.Username,
            Password = pg.Password,
            Pooling = true,
            Timeout = 5,
            CommandTimeout = 10
        };
    }

    private static string QuoteIdentifier(string identifier)
    {
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }

    private void SetBusyState(bool isBusy)
    {
        _applyButton.Enabled = !isBusy;
        _closeButton.Enabled = !isBusy;
        _checkPostgresButton.Enabled = !isBusy;
        _checkPipelineButton.Enabled = !isBusy;
        _previewFilesButton.Enabled = !isBusy;
        _browseFolderButton.Enabled = !isBusy;
        var selectionMode = GetSelectionMode();
        _selectAllFilesButton.Enabled = !isBusy && selectionMode == DbfMinerConfig.SelectionModeManual;
        _clearSelectedFilesButton.Enabled = !isBusy && selectionMode == DbfMinerConfig.SelectionModeManual;
        _selectAllYearsButton.Enabled = !isBusy && selectionMode == DbfMinerConfig.SelectionModeYears;
        _clearYearsButton.Enabled = !isBusy && selectionMode == DbfMinerConfig.SelectionModeYears;
        _selectionModeComboBox.Enabled = !isBusy;
        _dbfFilesCheckedListBox.Enabled = !isBusy && selectionMode == DbfMinerConfig.SelectionModeManual;
        _dbfYearsCheckedListBox.Enabled = !isBusy && selectionMode == DbfMinerConfig.SelectionModeYears;
        _minimumYearNumeric.Enabled = !isBusy && selectionMode == DbfMinerConfig.SelectionModeMinYear;
        UseWaitCursor = isBusy;
    }

    private void ValidateInputs()
    {
        if (string.IsNullOrWhiteSpace(_dbfFolderTextBox.Text))
            throw new InvalidOperationException("Select a DBF folder.");

        if (!Directory.Exists(_dbfFolderTextBox.Text.Trim()))
            throw new InvalidOperationException("The selected DBF folder does not exist.");

        if (string.IsNullOrWhiteSpace(_dbfPatternTextBox.Text))
            throw new InvalidOperationException("Specify a DBF search pattern.");

        if (string.IsNullOrWhiteSpace(_apiHostTextBox.Text))
            throw new InvalidOperationException("Specify the API host.");

        ValidatePostgresInputs();

        var preview = EnsurePreviewFor(_dbfFolderTextBox.Text.Trim(), _dbfPatternTextBox.Text.Trim());
        if (GetSelectionMode() != DbfMinerConfig.SelectionModeAll && ResolveFilesForProcessing(preview).Count == 0)
            throw new InvalidOperationException("Select at least one DBF file for processing.");
    }

    private void ValidatePostgresInputs()
    {
        if (string.IsNullOrWhiteSpace(_pgHostTextBox.Text))
            throw new InvalidOperationException("Specify the PostgreSQL host.");

        if (string.IsNullOrWhiteSpace(_pgDatabaseTextBox.Text))
            throw new InvalidOperationException("Specify the PostgreSQL database.");

        if (string.IsNullOrWhiteSpace(_pgUsernameTextBox.Text))
            throw new InvalidOperationException("Specify the PostgreSQL username.");

        if (string.IsNullOrWhiteSpace(_pgSchemaTextBox.Text))
            throw new InvalidOperationException("Specify the PostgreSQL schema.");
    }

    private void BrowseFolder()
    {
        using var dialog = new FolderBrowserDialog
        {
            ShowNewFolderButton = false,
            Description = "Select the folder that contains DBF files",
            SelectedPath = Directory.Exists(_dbfFolderTextBox.Text) ? _dbfFolderTextBox.Text : ""
        };

        if (dialog.ShowDialog(this) != DialogResult.OK)
            return;

        _dbfFolderTextBox.Text = dialog.SelectedPath;
        RefreshDbfPreview();
    }

    private void RefreshDbfPreview()
    {
        try
        {
            var preservedSelection = GetCheckedPreviewFiles();
            if (preservedSelection.Count > 0)
                _selectedPreviewFiles = preservedSelection;

            var preview = EnsurePreviewFor(_dbfFolderTextBox.Text.Trim(), _dbfPatternTextBox.Text.Trim());
            UpdatePreviewUi(preview, _dbfFolderTextBox.Text.Trim(), _dbfPatternTextBox.Text.Trim());
        }
        catch (Exception ex)
        {
            _previewFiles = Array.Empty<string>();
            _previewFolder = null;
            _previewPattern = null;
            _dbfFilesCheckedListBox.Items.Clear();
            _previewSummaryLabel.Text = $"Preview failed: {ex.Message}";
        }
    }

    private IReadOnlyList<string> EnsurePreviewFor(string folder, string pattern)
    {
        if (string.IsNullOrWhiteSpace(folder))
        {
            _previewFiles = Array.Empty<string>();
            _previewFolder = folder;
            _previewPattern = pattern;
            return _previewFiles;
        }

        var effectivePattern = string.IsNullOrWhiteSpace(pattern) ? "*.dbf" : pattern;
        if (!string.Equals(_previewFolder, folder, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(_previewPattern, effectivePattern, StringComparison.OrdinalIgnoreCase))
        {
            _previewFiles = Directory
                .EnumerateFiles(folder, effectivePattern, SearchOption.TopDirectoryOnly)
                .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            _previewFolder = folder;
            _previewPattern = effectivePattern;
        }

        return _previewFiles;
    }

    private void UpdatePreviewUi(IReadOnlyList<string> files, string folder, string pattern)
    {
        _dbfFilesCheckedListBox.Items.Clear();

        _previewYears = files
            .Select(path => DbfMinerConfig.TryExtractYearFromDbfPath(path, out var year) ? year : (int?)null)
            .Where(year => year.HasValue)
            .Select(year => year!.Value)
            .Distinct()
            .OrderBy(year => year)
            .ToArray();

        _dbfYearsCheckedListBox.Items.Clear();
        foreach (var year in _previewYears)
        {
            var isChecked = GetSelectionMode() == DbfMinerConfig.SelectionModeAll ||
                            GetSelectionMode() == DbfMinerConfig.SelectionModeYears && _selectedPreviewYears.Contains(year);
            _dbfYearsCheckedListBox.Items.Add(year, isChecked);
        }

        foreach (var file in files)
        {
            var isChecked = GetSelectionMode() == DbfMinerConfig.SelectionModeAll ||
                            GetSelectionMode() == DbfMinerConfig.SelectionModeManual && _selectedPreviewFiles.Contains(file);
            _dbfFilesCheckedListBox.Items.Add(file, isChecked);
        }

        if (GetSelectionMode() != DbfMinerConfig.SelectionModeAll)
        {
            _selectedPreviewFiles = GetCheckedPreviewFiles();
            _selectedPreviewYears = GetCheckedPreviewYears();
        }

        var effectivePattern = string.IsNullOrWhiteSpace(pattern) ? "*.dbf" : pattern;
        var selectedCount = ResolveFilesForProcessing(files).Count;

        _previewSummaryLabel.Text = files.Count switch
        {
            0 => $"No DBF files found in '{folder}' with pattern '{effectivePattern}'.",
            _ when GetSelectionMode() == DbfMinerConfig.SelectionModeAll =>
                $"{files.Count} DBF files found. All matching files will be processed automatically.",
            _ when GetSelectionMode() == DbfMinerConfig.SelectionModeYears =>
                $"{files.Count} DBF files found. Years selected: {_selectedPreviewYears.Count}. Files to process: {selectedCount}.",
            _ when GetSelectionMode() == DbfMinerConfig.SelectionModeMinYear =>
                $"{files.Count} DBF files found. Minimum year: {Decimal.ToInt32(_minimumYearNumeric.Value)}. Files to process: {selectedCount}.",
            _ => $"{files.Count} DBF files found. Selected for processing: {selectedCount}."
        };

        UpdateSelectionModeUi();
    }

    private void UpdateSelectionModeUi()
    {
        var selectionMode = GetSelectionMode();
        var manualMode = selectionMode == DbfMinerConfig.SelectionModeManual;
        var yearsMode = selectionMode == DbfMinerConfig.SelectionModeYears;
        var minYearMode = selectionMode == DbfMinerConfig.SelectionModeMinYear;

        _dbfFilesCheckedListBox.Enabled = manualMode;
        _selectAllFilesButton.Enabled = manualMode;
        _clearSelectedFilesButton.Enabled = manualMode;
        _dbfYearsCheckedListBox.Enabled = yearsMode;
        _selectAllYearsButton.Enabled = yearsMode;
        _clearYearsButton.Enabled = yearsMode;
        _minimumYearNumeric.Enabled = minYearMode;

        if (selectionMode == DbfMinerConfig.SelectionModeAll)
        {
            for (var i = 0; i < _dbfFilesCheckedListBox.Items.Count; i++)
                _dbfFilesCheckedListBox.SetItemChecked(i, true);
            for (var i = 0; i < _dbfYearsCheckedListBox.Items.Count; i++)
                _dbfYearsCheckedListBox.SetItemChecked(i, true);
        }
    }

    private void SetAllPreviewItemsChecked(bool isChecked)
    {
        for (var i = 0; i < _dbfFilesCheckedListBox.Items.Count; i++)
            _dbfFilesCheckedListBox.SetItemChecked(i, isChecked);

        _selectedPreviewFiles = GetCheckedPreviewFiles();
        UpdatePreviewUi(_previewFiles, _dbfFolderTextBox.Text.Trim(), _dbfPatternTextBox.Text.Trim());
    }

    private HashSet<string> GetCheckedPreviewFiles()
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in _dbfFilesCheckedListBox.CheckedItems)
        {
            if (item is string path)
                result.Add(path);
        }
        return result;
    }

    private HashSet<int> GetCheckedPreviewYears()
    {
        var result = new HashSet<int>();
        foreach (var item in _dbfYearsCheckedListBox.CheckedItems)
        {
            if (item is int year)
                result.Add(year);
        }
        return result;
    }

    private void SetAllPreviewYearsChecked(bool isChecked)
    {
        for (var i = 0; i < _dbfYearsCheckedListBox.Items.Count; i++)
            _dbfYearsCheckedListBox.SetItemChecked(i, isChecked);

        _selectedPreviewYears = GetCheckedPreviewYears();
        UpdatePreviewUi(_previewFiles, _dbfFolderTextBox.Text.Trim(), _dbfPatternTextBox.Text.Trim());
    }

    private List<string> ResolveFilesForProcessing(IReadOnlyList<string> previewFiles)
    {
        var selectionMode = GetSelectionMode();
        if (selectionMode == DbfMinerConfig.SelectionModeAll)
            return previewFiles.ToList();

        if (selectionMode == DbfMinerConfig.SelectionModeManual)
        {
            var selected = GetCheckedPreviewFiles();
            return previewFiles
                .Where(selected.Contains)
                .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        if (selectionMode == DbfMinerConfig.SelectionModeYears)
        {
            var selectedYears = GetCheckedPreviewYears();
            return previewFiles
                .Where(path => DbfMinerConfig.TryExtractYearFromDbfPath(path, out var year) && selectedYears.Contains(year))
                .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        var minimumYear = Decimal.ToInt32(_minimumYearNumeric.Value);
        return previewFiles
            .Where(path => DbfMinerConfig.TryExtractYearFromDbfPath(path, out var year) && year >= minimumYear)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string BuildConfirmationText(
        DbfMinerConfig cfg,
        IReadOnlyList<string> preview,
        IReadOnlyList<string> effectiveFiles)
    {
        var summaryLines = new List<string>
        {
            "Apply these settings?",
            "",
            $"DBF folder: {cfg.DbfFolder}",
            $"Pattern: {cfg.DbfSearchPattern}",
            $"Preview matches: {preview.Count}",
            $"Processing mode: {DescribeSelectionMode(cfg.DbfSelectionMode)}",
            $"Files to process: {effectiveFiles.Count}",
            $"Poll interval: {cfg.PollIntervalSeconds} sec",
            $"Batch size: {cfg.Ingestion.BatchSize}",
            $"PostgreSQL: {cfg.Postgres.Host}:{cfg.Postgres.Port}/{cfg.Postgres.Database}",
            $"Schema: {cfg.Postgres.Schema}"
        };

        if (effectiveFiles.Count > 0)
        {
            summaryLines.Add("");
            summaryLines.Add("Files to process:");
            foreach (var file in effectiveFiles.Take(8))
                summaryLines.Add(file);

            if (effectiveFiles.Count > 8)
                summaryLines.Add($"... and {effectiveFiles.Count - 8} more");
        }
        else
        {
            summaryLines.Add("");
            summaryLines.Add("No DBF files are currently selected for processing.");
        }

        return string.Join(Environment.NewLine, summaryLines);
    }

    private static GroupBox CreateGroupBox(string text)
    {
        return new GroupBox
        {
            Dock = DockStyle.Top,
            AutoSize = true,
            Padding = new Padding(12),
            Text = text
        };
    }

    private static TableLayoutPanel CreateGroupLayout(int columns)
    {
        var layout = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            AutoSize = true,
            ColumnCount = columns
        };

        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 180));
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        if (columns == 3)
            layout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

        return layout;
    }

    private static Label CreateLabel(string text)
    {
        return new Label
        {
            AutoSize = true,
            Text = text,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(0, 8, 8, 8)
        };
    }

    private static Label CreateHintLabel(string text)
    {
        return new Label
        {
            AutoSize = true,
            ForeColor = SystemColors.GrayText,
            Text = text,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(8)
        };
    }

    private static TextBox CreateTextBox(string text = "")
    {
        return new TextBox
        {
            Dock = DockStyle.Fill,
            Margin = new Padding(0, 4, 0, 4),
            Text = text
        };
    }

    private static NumericUpDown CreateNumeric(decimal minimum, decimal maximum, decimal value)
    {
        return new NumericUpDown
        {
            Dock = DockStyle.Left,
            Minimum = minimum,
            Maximum = maximum,
            Value = value,
            Width = 140,
            Margin = new Padding(0, 4, 0, 4)
        };
    }

    private static decimal ClampNumeric(NumericUpDown numeric, int value)
    {
        var decimalValue = Convert.ToDecimal(value);
        if (decimalValue < numeric.Minimum)
            return numeric.Minimum;
        if (decimalValue > numeric.Maximum)
            return numeric.Maximum;
        return decimalValue;
    }

    private string GetSelectionMode()
    {
        return _selectionModeComboBox.SelectedIndex switch
        {
            1 => DbfMinerConfig.SelectionModeManual,
            2 => DbfMinerConfig.SelectionModeYears,
            3 => DbfMinerConfig.SelectionModeMinYear,
            _ => DbfMinerConfig.SelectionModeAll
        };
    }

    private static DbfMinerConfig CreateDefaultConfig()
    {
        return new DbfMinerConfig
        {
            DbfFolder = "",
            DbfSearchPattern = "*.dbf",
            DbfSelectionMode = DbfMinerConfig.SelectionModeAll,
            PollIntervalSeconds = 10,
            Api = new ApiConfig
            {
                Host = "127.0.0.1",
                Port = 5055
            },
            Postgres = new PostgresConfig(),
            Ingestion = new IngestionConfig()
        };
    }

    private static string DescribeSelectionMode(string selectionMode)
    {
        return selectionMode switch
        {
            DbfMinerConfig.SelectionModeManual => "Only selected files",
            DbfMinerConfig.SelectionModeYears => "Only selected years",
            DbfMinerConfig.SelectionModeMinYear => "Only files from min year",
            _ => "All matching files"
        };
    }

    private sealed class PostgresServerInfo
    {
        public required string ServerVersion { get; init; }
        public required string CurrentUser { get; init; }
        public required bool DatabaseCreated { get; init; }
    }

    private sealed class DbfProbeSample
    {
        public required string FilePath { get; init; }
        public required int MeterNumber { get; init; }
        public required int SourceYear { get; init; }
        public required DbfHeaderInfo Header { get; init; }
        public required ParsedDbfRow Row { get; init; }
        public required long RowIndex { get; init; }
        public required long FileOffset { get; init; }
        public required long NextByteOffset { get; init; }
        public required string RowHash { get; init; }
    }
}
