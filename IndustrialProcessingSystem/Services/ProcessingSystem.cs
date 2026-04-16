using System.Globalization;
using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Models;

namespace IndustrialProcessingSystem.Services;

public class ProcessingSystem
{
    private readonly int _workerCount;
    private readonly int _maxQueueSize;
    private readonly List<QueuedJob> _queuedJobs = [];
    private readonly List<Task> _workers = [];
    private readonly object _queueLock = new();
    private readonly SemaphoreSlim _queueSignal;

    public event Action<Job, int>? JobCompleted;
    public event Action<Job, Exception>? JobFailed;

    public ProcessingSystem(SystemConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (config.WorkerCount <= 0)
        {
            throw new InvalidOperationException("WorkerCount must be greater than 0.");
        }

        if (config.MaxQueueSize <= 0)
        {
            throw new InvalidOperationException("MaxQueueSize must be greater than 0.");
        }

        if (config.InitialJobs is null)
        {
            throw new InvalidOperationException("InitialJobs cannot be null.");
        }

        _workerCount = config.WorkerCount;
        _maxQueueSize = config.MaxQueueSize;

        if (config.InitialJobs.Count > _maxQueueSize)
        {
            throw new InvalidOperationException(
                $"Initial jobs count ({config.InitialJobs.Count}) exceeds MaxQueueSize ({_maxQueueSize}).");
        }

        lock (_queueLock)
        {
            foreach (var job in config.InitialJobs)
            {
                ValidateJob(job);
                var queuedJob = new QueuedJob(
                    job,
                    new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously));
                EnqueueByPriority(queuedJob);
            }
        }

        _queueSignal = new SemaphoreSlim(_queuedJobs.Count, int.MaxValue);
        for (var i = 0; i < _workerCount; i++)
        {
            _workers.Add(Task.Run(WorkerLoopAsync));
        }
    }

    public int WorkerCount => _workerCount;
    public int MaxQueueSize => _maxQueueSize;

    public int QueuedCount
    {
        get
        {
            lock (_queueLock)
            {
                return _queuedJobs.Count;
            }
        }
    }

    public JobHandle Submit(Job job)
    {
        ValidateJob(job);

        var queuedJob = new QueuedJob(
            job,
            new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously));
        lock (_queueLock)
        {
            if (_queuedJobs.Count >= _maxQueueSize)
            {
                throw new InvalidOperationException("Queue is full. Cannot accept new jobs.");
            }

            EnqueueByPriority(queuedJob);
            _queueSignal.Release();
        }

        return new JobHandle
        {
            Id = job.Id,
            Result = queuedJob.CompletionSource.Task
        };
    }

    private static void ValidateJob(Job job)
    {
        ArgumentNullException.ThrowIfNull(job);

        if (job.Id == Guid.Empty)
        {
            throw new ArgumentException("Job Id must not be empty.", nameof(job));
        }

        if (!Enum.IsDefined(typeof(JobType), job.Type))
        {
            throw new ArgumentException("Job Type is invalid.", nameof(job));
        }

        if (string.IsNullOrWhiteSpace(job.Payload))
        {
            throw new ArgumentException("Job Payload is required.", nameof(job));
        }

        if (job.Priority <= 0)
        {
            throw new ArgumentException("Job Priority must be greater than 0.", nameof(job));
        }
    }

    private void EnqueueByPriority(QueuedJob queuedJob)
    {
        var insertIndex = _queuedJobs.FindIndex(existing => existing.Job.Priority > queuedJob.Job.Priority);
        if (insertIndex < 0)
        {
            _queuedJobs.Add(queuedJob);
        }
        else
        {
            _queuedJobs.Insert(insertIndex, queuedJob);
        }
    }

    private bool TryDequeueNext(out QueuedJob? queuedJob)
    {
        lock (_queueLock)
        {
            if (_queuedJobs.Count == 0)
            {
                queuedJob = null;
                return false;
            }

            queuedJob = _queuedJobs[0];
            _queuedJobs.RemoveAt(0);
            return true;
        }
    }

    private async Task<int> ExecuteJobAsync(Job job)
    {
        ArgumentNullException.ThrowIfNull(job);

        return job.Type switch
        {
            JobType.IO => await ExecuteIoJobAsync(job.Payload),
            JobType.Prime => ExecutePrimeJob(job.Payload),
            _ => throw new InvalidOperationException($"Unsupported job type: {job.Type}.")
        };
    }

    private static int ExecutePrimeJob(string payload)
    {
        var (numbers, threads) = ParsePrimePayload(payload);
        _ = threads; // TODO: Use threads to parallelize prime execution in a future step.

        var primeCount = 0;
        for (var value = 2; value <= numbers; value++)
        {
            if (IsPrime(value))
            {
                primeCount++;
            }
        }

        return primeCount;
    }

    private static (int Numbers, int Threads) ParsePrimePayload(string payload)
    {
        const string invalidPayloadMessage = "Prime payload is invalid. Expected format: numbers:<value>,threads:<value> with numbers > 1 and threads > 0.";

        if (string.IsNullOrWhiteSpace(payload))
        {
            throw new InvalidOperationException(invalidPayloadMessage);
        }

        var segments = payload.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length != 2)
        {
            throw new InvalidOperationException(invalidPayloadMessage);
        }

        int? numbers = null;
        int? threads = null;

        foreach (var segment in segments)
        {
            var keyValue = segment.Split(':', 2, StringSplitOptions.TrimEntries);
            if (keyValue.Length != 2 || keyValue[1].Length == 0)
            {
                throw new InvalidOperationException(invalidPayloadMessage);
            }

            var valueText = keyValue[1].Replace("_", string.Empty, StringComparison.Ordinal);
            if (!int.TryParse(valueText, NumberStyles.None, CultureInfo.InvariantCulture, out var value))
            {
                throw new InvalidOperationException(invalidPayloadMessage);
            }

            if (keyValue[0].Equals("numbers", StringComparison.OrdinalIgnoreCase))
            {
                if (numbers.HasValue)
                {
                    throw new InvalidOperationException(invalidPayloadMessage);
                }

                numbers = value;
                continue;
            }

            if (keyValue[0].Equals("threads", StringComparison.OrdinalIgnoreCase))
            {
                if (threads.HasValue)
                {
                    throw new InvalidOperationException(invalidPayloadMessage);
                }

                threads = value;
                continue;
            }

            throw new InvalidOperationException(invalidPayloadMessage);
        }

        if (!numbers.HasValue || !threads.HasValue || numbers.Value <= 1 || threads.Value <= 0)
        {
            throw new InvalidOperationException(invalidPayloadMessage);
        }

        return (numbers.Value, threads.Value);
    }

    private static bool IsPrime(int value)
    {
        if (value < 2)
        {
            return false;
        }

        if (value == 2)
        {
            return true;
        }

        if (value % 2 == 0)
        {
            return false;
        }

        var limit = (int)Math.Sqrt(value);
        for (var divisor = 3; divisor <= limit; divisor += 2)
        {
            if (value % divisor == 0)
            {
                return false;
            }
        }

        return true;
    }

    private static async Task<int> ExecuteIoJobAsync(string payload)
    {
        var delayMilliseconds = ParseDelayMilliseconds(payload);
        await Task.Delay(delayMilliseconds);
        return delayMilliseconds;
    }

    private static int ParseDelayMilliseconds(string payload)
    {
        const string invalidPayloadMessage = "IO payload is invalid. Expected format: delay:<milliseconds>.";
        const string payloadPrefix = "delay:";

        if (string.IsNullOrWhiteSpace(payload) ||
            !payload.StartsWith(payloadPrefix, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(invalidPayloadMessage);
        }

        var delayText = payload[payloadPrefix.Length..].Trim();
        if (delayText.Length == 0)
        {
            throw new InvalidOperationException(invalidPayloadMessage);
        }

        var normalizedDelayText = delayText.Replace("_", string.Empty, StringComparison.Ordinal);
        if (!int.TryParse(
                normalizedDelayText,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var delayMilliseconds) ||
            delayMilliseconds < 0)
        {
            throw new InvalidOperationException(invalidPayloadMessage);
        }

        return delayMilliseconds;
    }

    private async Task WorkerLoopAsync()
    {
        while (true)
        {
            await _queueSignal.WaitAsync();

            if (!TryDequeueNext(out var queuedJob))
            {
                continue;
            }

            try
            {
                var result = await ExecuteJobAsync(queuedJob.Job);
                queuedJob.CompletionSource.TrySetResult(result);
                OnJobCompleted(queuedJob.Job, result);
            }
            catch (Exception ex)
            {
                queuedJob.CompletionSource.TrySetException(ex);
                OnJobFailed(queuedJob.Job, ex);
            }
        }
    }

    private void OnJobCompleted(Job job, int result)
    {
        try
        {
            JobCompleted?.Invoke(job, result);
        }
        catch
        {
            // Intentionally swallow subscriber exceptions to keep workers running.
        }
    }

    private void OnJobFailed(Job job, Exception ex)
    {
        try
        {
            JobFailed?.Invoke(job, ex);
        }
        catch
        {
            // Intentionally swallow subscriber exceptions to keep workers running.
        }
    }

    private sealed class QueuedJob
    {
        public QueuedJob(Job job, TaskCompletionSource<int> completionSource)
        {
            Job = job;
            CompletionSource = completionSource;
        }

        public Job Job { get; }
        public TaskCompletionSource<int> CompletionSource { get; }
    }
}
