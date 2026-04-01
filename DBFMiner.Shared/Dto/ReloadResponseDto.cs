namespace DBFMiner.Shared.Dto;

public sealed class ReloadResponseDto
{
    public bool Ok { get; set; }
    public string? Message { get; set; }

    public ServiceStatusDto? Status { get; set; }
}