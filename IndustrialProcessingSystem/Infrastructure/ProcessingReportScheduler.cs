using IndustrialProcessingSystem.Services;

namespace IndustrialProcessingSystem.Infrastructure;

public class ProcessingReportScheduler
{
    private const int MaxReportFilesToKeep = 10;
    private readonly ProcessingSystem _processingSystem;
    private readonly string _reportsDirectory;
    private readonly TimeSpan _interval;
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
                    // Intentionally swallow scheduler/report failures for now.
                }
            }
        }
        catch
        {
            // Intentionally swallow scheduler loop failures for now.
        }
    }

    private void GenerateReportAndApplyRetention()
    {
        Directory.CreateDirectory(_reportsDirectory);

        var reportFileName = $"report-{DateTime.UtcNow:yyyyMMdd-HHmmss-fff}.xml";
        var reportFilePath = Path.Combine(_reportsDirectory, reportFileName);
        _processingSystem.GenerateXmlReport(reportFilePath);

        var reportFiles = Directory
            .GetFiles(_reportsDirectory, "report-20*.xml")
            .OrderByDescending(path => Path.GetFileName(path), StringComparer.Ordinal)
            .ToList();

        foreach (var oldFile in reportFiles.Skip(MaxReportFilesToKeep))
        {
            try
            {
                File.Delete(oldFile);
            }
            catch
            {
                // Intentionally swallow cleanup failures for now.
            }
        }
    }
}
