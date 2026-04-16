using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Models;

namespace IndustrialProcessingSystem.Services;

public class ProcessingSystem
{
    private readonly int _workerCount;
    private readonly int _maxQueueSize;
    private readonly List<Job> _queuedJobs = [];
    private readonly object _queueLock = new();

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
                EnqueueByPriority(job);
            }
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

        TaskCompletionSource<int> resultSource;
        lock (_queueLock)
        {
            if (_queuedJobs.Count >= _maxQueueSize)
            {
                throw new InvalidOperationException("Queue is full. Cannot accept new jobs.");
            }

            EnqueueByPriority(job);
            resultSource = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        // TODO: Complete this task when worker execution is implemented.
        return new JobHandle
        {
            Id = job.Id,
            Result = resultSource.Task
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

    private void EnqueueByPriority(Job job)
    {
        var insertIndex = _queuedJobs.FindIndex(existing => existing.Priority > job.Priority);
        if (insertIndex < 0)
        {
            _queuedJobs.Add(job);
        }
        else
        {
            _queuedJobs.Insert(insertIndex, job);
        }
    }
}
