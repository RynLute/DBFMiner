using System.Text.Json.Serialization;
using DBFMiner.Shared.Dto;
using DBFMiner.Shared.Models;

namespace DBFMiner.Shared.Serialization;

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(DbfMinerConfig))]
[JsonSerializable(typeof(ApiConfig))]
[JsonSerializable(typeof(PostgresConfig))]
[JsonSerializable(typeof(IngestionConfig))]
[JsonSerializable(typeof(ServiceStatusDto))]
[JsonSerializable(typeof(ReloadResponseDto))]
public partial class AppJsonSerializerContext : JsonSerializerContext
{
}