using IndustrialProcessingSystem.Services;

namespace IndustrialProcessingSystem.Infrastructure;

public class ProcessingReportScheduler
{
    private const int MaxReportFilesToKeep = 10;
    private readonly ProcessingSystem _processingSystem;
    private readonly string _reportsDirectory;
    private readonly TimeSpan _interval;
    private long _nextReportSlot;
    private int _started;

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
        if (Interlocked.Exchange(ref _started, 1) == 1)
        {
            return;
        }

        _ = Task.Run(RunLoopSafeAsync);
    }

    private async Task RunLoopSafeAsync()
    {
        try
        {
            using var timer = new PeriodicTimer(_interval);
            while (await timer.WaitForNextTickAsync())
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
