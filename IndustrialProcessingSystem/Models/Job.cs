using System;
using IndustrialProcessingSystem.Enums;

namespace IndustrialProcessingSystem.Models;

public class Job
{
    public Guid Id { get; set; }
    public JobType Type { get; set; }
    public string Payload { get; set; } = string.Empty;
    public int Priority { get; set; }
}
