using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Infrastructure;
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
            var jobEventFileLogger = new JobEventFileLogger(Path.Combine("logs", "job-events.log"));
            jobEventFileLogger.Attach(processingSystem);
            var reportsDirectory = Path.Combine("reports");

            processingSystem.JobCompleted += (job, result) =>
                Console.WriteLine($"[EVENT] JobCompleted: Id={job.Id}, Type={job.Type}, Result={result}");
            processingSystem.JobFailed += (job, ex) =>
                Console.WriteLine($"[EVENT] JobFailed: Id={job.Id}, Type={job.Type}, Error={ex.Message}");

            var reportScheduler = new ProcessingReportScheduler(
                processingSystem,
                reportsDirectory,
                TimeSpan.FromSeconds(1));
            reportScheduler.Start();
            Console.WriteLine($"Periodic XML reports directory: {Path.GetFullPath(reportsDirectory)}");

            processingSystem.Start();

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

            var failingJob = new Job
            {
                Id = Guid.NewGuid(),
                Type = JobType.IO,
                Payload = "delay:abc",
                Priority = 1
            };

            try
            {
                var failingHandle = processingSystem.Submit(failingJob);
                var failingResult = await failingHandle.Result;
                Console.WriteLine($"Failing demo job result (unexpected): {failingResult}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failing demo IO job threw while awaiting result: {ex.Message}");
            }
            
            await Task.Delay(8000);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load configuration: {ex.Message}");
        }
    }
}
