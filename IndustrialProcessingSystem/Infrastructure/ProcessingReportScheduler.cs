using IndustrialProcessingSystem.Services;

namespace IndustrialProcessingSystem.Infrastructure;

public class ProcessingReportScheduler : IAsyncDisposable, IDisposable
{
    private const int MaxReportFilesToKeep = 10;
    private readonly ProcessingSystem _processingSystem;
    private readonly string _reportsDirectory;
    private readonly TimeSpan _interval;
    private long _nextReportSlot;
    private bool _started;
    private CancellationTokenSource? _schedulerCts;
    private Task? _runTask;
    private readonly object _lifecycleLock = new();

    public ProcessingReportScheduler(ProcessingSystem processingSystem, string reportsDirectory, TimeSpan interval)
    {
        ArgumentNullException.ThrowIfNull(processingSystem);

        if (string.IsNullOrWhiteSpace(reportsDirectory))
        {
            throw new ArgumentException("Reports directory is required.", nameof(reportsDirectory));
        }

        if (interval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(interval), "Interval must be greater than zero.");
        }

        _processingSystem = processingSystem;
        _reportsDirectory = Path.GetFullPath(reportsDirectory);
        _interval = interval;

        Directory.CreateDirectory(_reportsDirectory);
        _nextReportSlot = ResolveInitialReportSlot();
    }

    public void Start()
    {
        lock (_lifecycleLock)
        {
            if (_started)
            {
                return;
            }

            _started = true;
            _schedulerCts = new CancellationTokenSource();
            _runTask = Task.Run(() => RunLoopSafeAsync(_schedulerCts.Token));
        }
    }

    public async Task StopAsync()
    {
        Task? runTask;
        CancellationTokenSource? schedulerCts;

        lock (_lifecycleLock)
        {
            if (!_started)
            {
                return;
            }

            _started = false;
            schedulerCts = _schedulerCts;
            runTask = _runTask;
            _schedulerCts = null;
            _runTask = null;
        }

        schedulerCts?.Cancel();

        if (runTask is not null)
        {
            try
            {
                await runTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        schedulerCts?.Dispose();
    }

    public void Dispose()
    {
        StopAsync().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
    }

    private async Task RunLoopSafeAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var timer = new PeriodicTimer(_interval);
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    GenerateReportAndApplyRetention();
                }
                catch
                {
                    //intentionally swallow report failures
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch
        {
            //intentionally swallow scheduler loop failures
        }
    }

    private void GenerateReportAndApplyRetention()
    {
        Directory.CreateDirectory(_reportsDirectory);

        var slot = (int)((Interlocked.Increment(ref _nextReportSlot) - 1) % MaxReportFilesToKeep);
        if (slot < 0)
        {
            slot += MaxReportFilesToKeep;
        }

        var reportFileName = $"report-{slot}.xml";
        var reportFilePath = Path.Combine(_reportsDirectory, reportFileName);
        _processingSystem.GenerateXmlReport(reportFilePath);
    }

    private long ResolveInitialReportSlot()
    {
        var knownSlots = new Dictionary<int, DateTime>();

        foreach (var path in Directory.GetFiles(_reportsDirectory, "report-*.xml"))
        {
            var fileName = Path.GetFileNameWithoutExtension(path);
            if (!fileName.StartsWith("report-", StringComparison.Ordinal))
            {
                continue;
            }

            var slotText = fileName["report-".Length..];
            if (!int.TryParse(slotText, out var slot))
            {
                continue;
            }

            if (slot < 0 || slot >= MaxReportFilesToKeep)
            {
                continue;
            }

            knownSlots[slot] = File.GetLastWriteTimeUtc(path);
        }

        if (knownSlots.Count < MaxReportFilesToKeep)
        {
            for (var slot = 0; slot < MaxReportFilesToKeep; slot++)
            {
                if (!knownSlots.ContainsKey(slot))
                {
                    return slot;
                }
            }
        }

        return knownSlots
            .OrderBy(entry => entry.Value)
            .ThenBy(entry => entry.Key)
            .First()
            .Key;
    }
}
