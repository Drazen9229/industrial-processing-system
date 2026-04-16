using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Models;
using IndustrialProcessingSystem.Services;

namespace IndustrialProcessingSystem;

class Program
{
    static async Task Main(string[] args)
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

            var demoJob = new Job
            {
                Id = Guid.NewGuid(),
                Type = JobType.IO,
                Payload = "delay:500",
                Priority = 1
            };

            try
            {
                var jobHandle = processingSystem.Submit(demoJob);
                var result = await jobHandle.Result;
                Console.WriteLine($"Demo IO job result: {result}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Demo IO job failed: {ex.Message}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load configuration: {ex.Message}");
        }
    }
}
