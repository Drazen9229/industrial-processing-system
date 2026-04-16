using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Models;

namespace IndustrialProcessingSystem.Services;

public class ProcessingSystem
{
    private readonly int _workerCount;
    private readonly int _maxQueueSize;
    private readonly List<QueuedJob> _queuedJobs = [];
    private readonly object _queueLock = new();
    private readonly SemaphoreSlim _queueSignal;

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

        // TODO: Complete this task when worker execution is implemented.
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

    // Will be used by worker loops once execution is implemented.
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
