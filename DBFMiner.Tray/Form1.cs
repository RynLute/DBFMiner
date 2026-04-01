using System.Text.Json;
using DBFMiner.Shared;
using DBFMiner.Shared.Dto;
using DBFMiner.Shared.Models;
using DBFMiner.Shared.Serialization;
using Microsoft.Win32;

namespace DBFMiner.Tray;

public partial class Form1 : Form
{
    private const string RunKeyPath = @"Software\Microsoft\Windows\CurrentVersion\Run";
    private const string RunValueName = "DbfMiner.Tray";

    private readonly HttpClient _httpClient = new();
    private NotifyIcon? _notifyIcon;
    private ContextMenuStrip? _menu;
    private ToolStripMenuItem? _statusMenuItem;
    private ToolStripMenuItem? _autostartMenuItem;
    private System.Windows.Forms.Timer? _pollTimer;

    public Form1()
    {
        InitializeComponent();

        WindowState = FormWindowState.Minimized;
        ShowInTaskbar = false;
        FormBorderStyle = FormBorderStyle.FixedToolWindow;
        Visible = false;

        BuildTray();
        UpdateAutostartUi();

        _pollTimer = new System.Windows.Forms.Timer
        {
            Interval = 2000
        };
        _pollTimer.Tick += async (_, _) => await PollServiceStatusAsync();
        _pollTimer.Start();
    }

    private void BuildTray()
    {
        _menu = new ContextMenuStrip();

        var settingsItem = new ToolStripMenuItem("Settings");
        settingsItem.Click += (_, _) => ShowSettingsDialog();
        _menu.Items.Add(settingsItem);

        _statusMenuItem = new ToolStripMenuItem("Status: polling");
        _statusMenuItem.Enabled = false;
        _menu.Items.Add(_statusMenuItem);

        _menu.Items.Add(new ToolStripSeparator());

        _autostartMenuItem = new ToolStripMenuItem("Start with Windows")
        {
            CheckOnClick = true
        };
        _autostartMenuItem.CheckedChanged += (_, _) =>
        {
            SetAutostartEnabled(_autostartMenuItem.Checked);
        };
        _menu.Items.Add(_autostartMenuItem);

        _menu.Items.Add(new ToolStripSeparator());

        var exitItem = new ToolStripMenuItem("Exit");
        exitItem.Click += (_, _) =>
        {
            _pollTimer?.Stop();
            _notifyIcon?.Dispose();
            Close();
        };
        _menu.Items.Add(exitItem);

        _notifyIcon = new NotifyIcon
        {
            Text = "DBFMiner",
            Icon = SystemIcons.Application,
            Visible = true,
            ContextMenuStrip = _menu
        };
    }

    private void UpdateAutostartUi()
    {
        if (_autostartMenuItem is null)
            return;

        _autostartMenuItem.Checked = IsAutostartEnabled();
    }

    private static bool IsAutostartEnabled()
    {
        try
        {
            using var key = Registry.CurrentUser.OpenSubKey(RunKeyPath, writable: false);
            if (key is null)
                return false;

            var value = key.GetValue(RunValueName) as string;
            return !string.IsNullOrWhiteSpace(value);
        }
        catch
        {
            return false;
        }
    }

    private void SetAutostartEnabled(bool enabled)
    {
        try
        {
            if (enabled)
            {
                using var key = Registry.CurrentUser.CreateSubKey(RunKeyPath);
                if (key is null)
                    return;

                key.SetValue(RunValueName, $"\"{Application.ExecutablePath}\"", RegistryValueKind.String);
            }
            else
            {
                using var key = Registry.CurrentUser.OpenSubKey(RunKeyPath, writable: true);
                key?.DeleteValue(RunValueName, throwOnMissingValue: false);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show(ex.Message, "Autostart", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private static string GetConfigPath() => ConfigPaths.DefaultConfigPath;

    private async Task PollServiceStatusAsync()
    {
        try
        {
            var status = await GetServiceStatusAsync();
            if (_statusMenuItem is null)
                return;

            var offset = status.CurrentFileOffset.HasValue
                ? $" | Offset: {status.CurrentFileOffset.Value}"
                : "";
            var extra = status.LastError is null ? "" : $" | Error: {status.LastError}";
            var files = status.FilesDiscovered > 0
                ? $"{status.FilesProcessed}/{status.FilesDiscovered}"
                : status.FilesProcessed.ToString();
            _statusMenuItem.Text =
                $"Status: {status.ServiceState} | Files: {files} | Rows: {status.RowsProcessed}/{status.RowsInserted}{offset}{extra}";
        }
        catch
        {
            if (_statusMenuItem is null)
                return;

            _statusMenuItem.Text = "Status: service unavailable";
        }
    }

    private async Task<ServiceStatusDto> GetServiceStatusAsync()
    {
        var cfg = await LoadConfigAsync().ConfigureAwait(false);
        var url = $"http://{cfg.Api.Host}:{cfg.Api.Port}/api/status";

        using var resp = await _httpClient.GetAsync(url).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();

        var json = await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
        var dto = JsonSerializer.Deserialize(json, SharedJson.DefaultContext.ServiceStatusDto);

        if (dto is null)
            throw new InvalidOperationException("Empty /api/status response");

        return dto;
    }

    private void ShowSettingsDialog()
    {
        using var form = new SettingsForm(GetConfigPath(), _httpClient);
        form.ReloadRequested += async (_, _) =>
        {
            await PollServiceStatusAsync();
        };
        form.ShowDialog(this);
    }

    protected override void OnFormClosed(FormClosedEventArgs e)
    {
        _pollTimer?.Stop();
        _notifyIcon?.Dispose();
        _httpClient.Dispose();
        base.OnFormClosed(e);
    }

    private static async Task<DbfMinerConfig> LoadConfigAsync()
    {
        var path = GetConfigPath();
        if (!File.Exists(path))
        {
            var defaultConfig = new DbfMinerConfig
            {
                DbfFolder = "",
                DbfSearchPattern = "*.dbf",
                PollIntervalSeconds = 10,
                Api = new()
                {
                    Host = "127.0.0.1",
                    Port = 5055
                },
                Postgres = new(),
                Ingestion = new()
            };
            var json = JsonSerializer.Serialize(defaultConfig, SharedJson.IndentedContext.DbfMinerConfig);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await File.WriteAllTextAsync(path, json).ConfigureAwait(false);
            return defaultConfig;
        }

        var text = await File.ReadAllTextAsync(path).ConfigureAwait(false);
        var cfg = JsonSerializer.Deserialize(text, SharedJson.DefaultContext.DbfMinerConfig);

        if (cfg is null)
            throw new InvalidOperationException("Failed to read config.json");

        return cfg;
    }
}
