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
            processingSystem.JobAborted += (job, ex) =>
                Console.WriteLine($"[EVENT] JobAborted: Id={job.Id}, Type={job.Type}, Error={ex.Message}");

            var reportScheduler = new ProcessingReportScheduler(
                processingSystem,
                reportsDirectory,
                TimeSpan.FromMinutes(1));
            reportScheduler.Start();
            Console.WriteLine($"Periodic XML reports directory: {Path.GetFullPath(reportsDirectory)}");

            processingSystem.Start();

            Console.WriteLine($"WorkerCount: {config.WorkerCount}");
            Console.WriteLine($"MaxQueueSize: {config.MaxQueueSize}");
            Console.WriteLine($"InitialJobs: {config.InitialJobs.Count}");
            Console.WriteLine($"ProcessingSystem queued jobs: {processingSystem.QueuedCount}");

            using var producerCts = new CancellationTokenSource();
            var stopSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ConsoleCancelEventHandler onCancelKeyPress = (_, eventArgs) =>
            {
                eventArgs.Cancel = true;
                producerCts.Cancel();
                stopSignal.TrySetResult();
            };

            Console.CancelKeyPress += onCancelKeyPress;

            var producerTasks = Enumerable
                .Range(1, config.WorkerCount)
                .Select(producerId => Task.Run(() => ProducerLoopAsync(producerId, processingSystem, producerCts.Token)))
                .ToArray();

            try
            {
                Console.WriteLine($"Started {producerTasks.Length} producer task(s).");
                Console.WriteLine("Press Enter or Ctrl+C to stop...");

                var enterTask = Task.Run(() => Console.ReadLine());
                await Task.WhenAny(enterTask, stopSignal.Task);
            }
            finally
            {
                Console.CancelKeyPress -= onCancelKeyPress;
                producerCts.Cancel();
                await Task.WhenAll(producerTasks);
                await reportScheduler.StopAsync();
                await processingSystem.StopAsync();
            }

            Console.WriteLine("Producers stopped. Exiting.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed: {ex.Message}");
        }
    }

    private static async Task ProducerLoopAsync(int producerId, ProcessingSystem processingSystem, CancellationToken cancellationToken)
    {
        var random = new Random(unchecked(Environment.TickCount + (producerId * 397)));

        while (!cancellationToken.IsCancellationRequested)
        {
            var job = CreateRandomJob(random);

            try
            {
                _ = processingSystem.Submit(job);
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"[Producer {producerId}] Submit rejected for job {job.Id} (Type={job.Type}, Priority={job.Priority}): {ex.Message}");
            }

            try
            {
                await Task.Delay(random.Next(50, 201), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private static Job CreateRandomJob(Random random)
    {
        var type = random.Next(0, 2) == 0 ? JobType.Prime : JobType.IO;
        var payload = type == JobType.Prime
            ? BuildPrimePayload(random)
            : BuildIoPayload(random);

        return new Job
        {
            Id = Guid.NewGuid(),
            Type = type,
            Payload = payload,
            Priority = random.Next(1, 11)
        };
    }

    private static string BuildPrimePayload(Random random)
    {
        var numbersUpperBound = random.Next(2_000, 100_001);
        var threadCount = random.Next(1, 9);
        return $"numbers:{numbersUpperBound},threads:{threadCount}";
    }

    private static string BuildIoPayload(Random random)
    {
        var delayMs = random.Next(50, 2_501);
        return $"delay:{delayMs}";
    }
}
