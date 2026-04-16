using IndustrialProcessingSystem.Models;

namespace IndustrialProcessingSystem.Configuration;

public class SystemConfig
{
    public int WorkerCount { get; set; }
    public int MaxQueueSize { get; set; }
    public List<Job> InitialJobs { get; set; } = [];
}
