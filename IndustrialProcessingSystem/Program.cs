using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Services;

namespace IndustrialProcessingSystem;

class Program
{
    static void Main(string[] args)
    {
        try
        {
            var loader = new SystemConfigLoader();
            var config = loader.Load(Path.Combine("Configuration", "SystemConfig.xml"));
            var processingSystem = new ProcessingSystem(config);

            Console.WriteLine($"WorkerCount: {config.WorkerCount}");
            Console.WriteLine($"MaxQueueSize: {config.MaxQueueSize}");
            Console.WriteLine($"InitialJobs: {config.InitialJobs.Count}");
            Console.WriteLine($"ProcessingSystem queued jobs: {processingSystem.QueuedCount}");

            foreach (var job in config.InitialJobs)
            {
                Console.WriteLine($"- Job {job.Id}: Type={job.Type}, Priority={job.Priority}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load configuration: {ex.Message}");
        }
    }
}
