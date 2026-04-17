using System.Diagnostics;
using System.Globalization;
using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Models;
using System.Xml.Linq;

namespace IndustrialProcessingSystem.Services;

public class ProcessingSystem : IAsyncDisposable, IDisposable
{
    private const int MaxAttempts = 3;
    private static readonly TimeSpan JobTimeout = TimeSpan.FromSeconds(2);
    private readonly int _workerCount;
    private readonly int _maxQueueSize;
    private readonly List<QueuedJob> _queuedJobs = [];
    private readonly HashSet<Guid> _acceptedJobIds = [];
    private readonly HashSet<Guid> _jobsInSystem = [];
    private readonly Dictionary<Guid, Job> _jobRegistry = [];
    private readonly List<Task> _workers = [];
    private readonly object _queueLock = new();
    private readonly object _startLock = new();
    private readonly object _statsLock = new();
    private readonly List<CompletedJobStat> _completedJobs = [];
    private readonly List<FailedJobStat> _failedJobs = [];
    private readonly SemaphoreSlim _queueSignal;
    private readonly CancellationTokenSource _shutdownCts = new();
    private int _stopRequested;
    private int _disposed;

    public event Action<Job, int>? JobCompleted;
    public event Action<Job, Exception>? JobFailed;
    public event Action<Job, Exception>? JobAborted;

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

                if (!_acceptedJobIds.Add(job.Id))
                {
                    throw new InvalidOperationException($"Duplicate job Id '{job.Id}' detected in InitialJobs.");
                }

                _jobsInSystem.Add(job.Id);
                _jobRegistry[job.Id] = job;
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

    public int InSystemCount
    {
        get
        {
            lock (_queueLock)
            {
                return _jobsInSystem.Count;
            }
        }
    }

    public void Start()
    {
        lock (_startLock)
        {
            ThrowIfDisposed();

            if (_workers.Count > 0)
            {
                throw new InvalidOperationException("ProcessingSystem has already been started.");
            }

            if (_shutdownCts.IsCancellationRequested)
            {
                throw new InvalidOperationException("ProcessingSystem is stopping or already stopped.");
            }

            for (var i = 0; i < _workerCount; i++)
            {
                _workers.Add(Task.Run(() => WorkerLoopAsync(_shutdownCts.Token)));
            }
        }
    }

    public JobHandle Submit(Job job)
    {
        ThrowIfDisposed();
        ValidateJob(job);

        QueuedJob queuedJob;
        lock (_queueLock)
        {
            if (_shutdownCts.IsCancellationRequested)
            {
                throw new InvalidOperationException("ProcessingSystem is stopping. Cannot accept new jobs.");
            }

            if (_acceptedJobIds.Contains(job.Id))
            {
                throw new InvalidOperationException($"Job with Id '{job.Id}' has already been accepted.");
            }

            if (_jobsInSystem.Count >= _maxQueueSize)
            {
                throw new InvalidOperationException("System capacity reached. Cannot accept new jobs.");
            }

            _acceptedJobIds.Add(job.Id);
            _jobsInSystem.Add(job.Id);
            _jobRegistry[job.Id] = job;
            queuedJob = new QueuedJob(
                job,
                new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously));
            EnqueueByPriority(queuedJob);
            _queueSignal.Release();
        }

        return new JobHandle
        {
            Id = job.Id,
            Result = queuedJob.CompletionSource.Task
        };
    }

    public IEnumerable<Job> GetTopJobs(int n)
    {
        if (n <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(n), "n must be greater than 0.");
        }

        lock (_queueLock)
        {
            return _queuedJobs
                .Take(n)
                .Select(queuedJob => queuedJob.Job)
                .ToArray();
        }
    }

    public Job GetJob(Guid id)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("Job Id must not be empty.", nameof(id));
        }

        lock (_queueLock)
        {
            if (_jobRegistry.TryGetValue(id, out var job))
            {
                return job;
            }
        }

        throw new InvalidOperationException($"Job with Id '{id}' was not found.");
    }

    public void GenerateXmlReport(string filePath)
    {
        ThrowIfDisposed();

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("Report file path is required.", nameof(filePath));
        }

        var fullPath = Path.GetFullPath(filePath);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        List<CompletedJobStat> completedSnapshot;
        List<FailedJobStat> failedSnapshot;
        lock (_statsLock)
        {
            completedSnapshot = [.. _completedJobs];
            failedSnapshot = [.. _failedJobs];
        }

        var completedByType = completedSnapshot
            .GroupBy(stat => stat.JobType)
            .OrderBy(group => group.Key.ToString(), StringComparer.Ordinal)
            .Select(group => new XElement(
                "JobType",
                new XAttribute("name", group.Key),
                new XAttribute("count", group.Count()),
                new XAttribute("averageDurationMs", Math.Round(group.Average(stat => stat.DurationMs)))));

        var failedByType = failedSnapshot
            .GroupBy(stat => stat.JobType)
            .OrderBy(group => group.Key.ToString(), StringComparer.Ordinal)
            .Select(group => new XElement(
                "JobType",
                new XAttribute("name", group.Key),
                new XAttribute("count", group.Count()),
                new XAttribute("averageDurationMs", Math.Round(group.Average(stat => stat.DurationMs)))));

        var report = new XDocument(
            new XElement(
                "ProcessingReport",
                new XAttribute("generatedAt", DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)),
                new XElement(
                    "Summary",
                    new XAttribute("totalCompleted", completedSnapshot.Count),
                    new XAttribute("totalFailed", failedSnapshot.Count),
                    new XAttribute("totalProcessed", completedSnapshot.Count + failedSnapshot.Count)),
                new XElement("CompletedByType", completedByType),
                new XElement("FailedByType", failedByType)));

        report.Save(fullPath);
    }

    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _stopRequested, 1) == 0)
        {
            _shutdownCts.Cancel();
            CancelQueuedJobs();
        }

        Task[] workersSnapshot;
        lock (_startLock)
        {
            workersSnapshot = _workers.ToArray();
        }

        if (workersSnapshot.Length == 0)
        {
            return;
        }

        try
        {
            await Task.WhenAll(workersSnapshot);
        }
        catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
        {
        }
    }

    public void Stop()
    {
        StopAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        try
        {
            Stop();
        }
        finally
        {
            _shutdownCts.Dispose();
            _queueSignal.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        try
        {
            await StopAsync();
        }
        finally
        {
            _shutdownCts.Dispose();
            _queueSignal.Dispose();
        }
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

    private bool TryDequeueNext(out QueuedJob queuedJob)
    {
        lock (_queueLock)
        {
            if (_queuedJobs.Count == 0)
            {
                queuedJob = null!;
                return false;
            }

            queuedJob = _queuedJobs[0];
            _queuedJobs.RemoveAt(0);
            return true;
        }
    }

    private async Task<int> ExecuteJobAsync(Job job, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);

        return job.Type switch
        {
            JobType.IO => await ExecuteIoJobAsync(job.Payload, cancellationToken),
            JobType.Prime => ExecutePrimeJob(job.Payload, cancellationToken),
            _ => throw new InvalidOperationException($"Unsupported job type: {job.Type}.")
        };
    }

    private async Task<int> ExecuteJobWithTimeoutAsync(Job job, CancellationToken cancellationToken)
    {
        using var timeoutCts = new CancellationTokenSource(JobTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);
        try
        {
            return await ExecuteJobAsync(job, linkedCts.Token);
        }
        catch (OperationCanceledException ex) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Job '{job.Id}' exceeded timeout of {JobTimeout.TotalSeconds} seconds.",
                ex);
        }
    }

    private async Task<int> ExecuteWithRetryAsync(
        Job job,
        Action<int, Exception, bool> onAttemptFailed,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(onAttemptFailed);
        Exception? lastException = null;

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await ExecuteJobWithTimeoutAsync(job, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastException = ex;
                var isFinalAttempt = attempt == MaxAttempts;
                onAttemptFailed(attempt, ex, isFinalAttempt);

                if (isFinalAttempt)
                {
                    throw;
                }
            }
        }

        throw lastException ?? new InvalidOperationException("Job execution failed.");
    }

    private static int ExecutePrimeJob(string payload, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (numbers, threads) = ParsePrimePayload(payload);
        var ranges = BuildPrimeRanges(numbers, threads);
        var primeCount = 0;
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = threads,
            CancellationToken = cancellationToken
        };

        Parallel.ForEach(
            ranges,
            parallelOptions,
            () => 0,
            (range, _, localPrimeCount) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (var value = range.Start; value <= range.End; value++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (IsPrime(value))
                    {
                        localPrimeCount++;
                    }
                }

                return localPrimeCount;
            },
            localPrimeCount => Interlocked.Add(ref primeCount, localPrimeCount));

        return primeCount;
    }

    private static (int Numbers, int Threads) ParsePrimePayload(string payload)
    {
        const string invalidPayloadMessage = "Prime payload is invalid. Expected format: numbers:<value>,threads:<value> with numbers > 1.";

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
            if (!int.TryParse(valueText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
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

        if (!numbers.HasValue || !threads.HasValue || numbers.Value <= 1)
        {
            throw new InvalidOperationException(invalidPayloadMessage);
        }

        return (numbers.Value, Math.Clamp(threads.Value, 1, 8));
    }

    private static IReadOnlyList<(int Start, int End)> BuildPrimeRanges(int numbers, int threads)
    {
        var totalValues = numbers - 1;
        var partitionCount = Math.Min(threads, totalValues);
        var basePartitionSize = totalValues / partitionCount;
        var remainder = totalValues % partitionCount;
        var ranges = new List<(int Start, int End)>(partitionCount);

        var start = 2;
        for (var index = 0; index < partitionCount; index++)
        {
            var currentPartitionSize = basePartitionSize + (index < remainder ? 1 : 0);
            var end = start + currentPartitionSize - 1;
            ranges.Add((start, end));
            start = end + 1;
        }

        return ranges;
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

    private static Task<int> ExecuteIoJobAsync(string payload, CancellationToken cancellationToken)
    {
        const int sleepChunkMilliseconds = 50;

        var delayMilliseconds = ParseDelayMilliseconds(payload);
        var remainingDelayMilliseconds = delayMilliseconds;

        cancellationToken.ThrowIfCancellationRequested();

        while (remainingDelayMilliseconds > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var chunk = Math.Min(sleepChunkMilliseconds, remainingDelayMilliseconds);
            Thread.Sleep(chunk);
            remainingDelayMilliseconds -= chunk;
        }

        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Random.Shared.Next(0, 101));
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

    private void CancelQueuedJobs()
    {
        List<QueuedJob> queuedJobs;
        lock (_queueLock)
        {
            queuedJobs = [.. _queuedJobs];
            _queuedJobs.Clear();

            foreach (var queuedJob in queuedJobs)
            {
                _jobsInSystem.Remove(queuedJob.Job.Id);
            }
        }

        foreach (var queuedJob in queuedJobs)
        {
            queuedJob.CompletionSource.TrySetCanceled(_shutdownCts.Token);
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                await _queueSignal.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            if (!TryDequeueNext(out var queuedJob))
            {
                continue;
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var result = await ExecuteWithRetryAsync(
                    queuedJob.Job,
                    (attempt, ex, isFinalAttempt) => OnJobAttemptFailed(queuedJob.Job, attempt, ex, isFinalAttempt),
                    cancellationToken);
                RecordCompletedJob(queuedJob.Job, result, stopwatch.ElapsedMilliseconds);
                queuedJob.CompletionSource.TrySetResult(result);
                OnJobCompleted(queuedJob.Job, result);
            }
            catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
            {
                queuedJob.CompletionSource.TrySetCanceled(cancellationToken);
                OnJobAborted(queuedJob.Job, ex);
            }
            catch (Exception ex)
            {
                RecordFailedJob(queuedJob.Job, ex.Message, stopwatch.ElapsedMilliseconds);
                queuedJob.CompletionSource.TrySetException(ex);
            }
            finally
            {
                RemoveFromSystem(queuedJob.Job.Id);
            }
        }
    }

    private void RemoveFromSystem(Guid jobId)
    {
        lock (_queueLock)
        {
            _jobsInSystem.Remove(jobId);
        }
    }

    private void RecordCompletedJob(Job job, int result, long durationMs)
    {
        lock (_statsLock)
        {
            _completedJobs.Add(new CompletedJobStat(job.Id, job.Type, result, durationMs));
        }
    }

    private void RecordFailedJob(Job job, string errorMessage, long durationMs)
    {
        lock (_statsLock)
        {
            _failedJobs.Add(new FailedJobStat(job.Id, job.Type, errorMessage, durationMs));
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
            //intentionally swallow exceptions to keep workers running
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
            //intentionally swallow exceptions to keep workers running
        }
    }

    private void OnJobAttemptFailed(Job job, int attempt, Exception ex, bool isFinalAttempt)
    {
        _ = attempt;
        if (isFinalAttempt)
        {
            OnJobAborted(job, ex);
            return;
        }

        OnJobFailed(job, ex);
    }

    private void OnJobAborted(Job job, Exception ex)
    {
        try
        {
            JobAborted?.Invoke(job, ex);
        }
        catch
        {
            //intentionally swallow exceptions to keep workers running
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

    private sealed record CompletedJobStat(Guid JobId, JobType JobType, int Result, long DurationMs);
    private sealed record FailedJobStat(Guid JobId, JobType JobType, string ErrorMessage, long DurationMs);
}
