namespace DBFMiner.Shared.Models;

public sealed class PostgresConfig
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5432;
    public string Database { get; set; } = "postgres";
    public string Username { get; set; } = "postgres";
    public string Password { get; set; } = "postgres";
    public string Schema { get; set; } = "public";
}