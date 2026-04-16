using IndustrialProcessingSystem.Models;
using IndustrialProcessingSystem.Services;

namespace IndustrialProcessingSystem.Infrastructure;

public class JobEventFileLogger
{
    private readonly string _logFilePath;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public JobEventFileLogger(string logFilePath)
    {
        if (string.IsNullOrWhiteSpace(logFilePath))
        {
            throw new ArgumentException("Log file path is required.", nameof(logFilePath));
        }

        _logFilePath = Path.GetFullPath(logFilePath);
        var directoryPath = Path.GetDirectoryName(_logFilePath);
        if (!string.IsNullOrWhiteSpace(directoryPath))
        {
            Directory.CreateDirectory(directoryPath);
        }
    }

    public void Attach(ProcessingSystem processingSystem)
    {
        ArgumentNullException.ThrowIfNull(processingSystem);

        processingSystem.JobCompleted += OnJobCompleted;
        processingSystem.JobFailed += OnJobFailed;
        processingSystem.JobAborted += OnJobAborted;
    }

    private void OnJobCompleted(Job job, int result)
    {
        var line = FormatLogLine("COMPLETED", job.Id, result.ToString());
        _ = AppendLineSafeAsync(line);
    }

    private void OnJobFailed(Job job, Exception ex)
    {
        var line = FormatLogLine("FAILED", job.Id, ex.Message);
        _ = AppendLineSafeAsync(line);
    }

    private void OnJobAborted(Job job, Exception ex)
    {
        var line = FormatLogLine("ABORT", job.Id, ex.Message);
        _ = AppendLineSafeAsync(line);
    }

    private static string FormatLogLine(string status, Guid jobId, string value)
    {
        var normalizedValue = string.IsNullOrWhiteSpace(value)
            ? "N/A"
            : value.Replace("\r", " ").Replace("\n", " ");
        return $"[{DateTimeOffset.UtcNow:O}] [{status}] {jobId}, {normalizedValue}";
    }

    private async Task AppendLineSafeAsync(string line)
    {
        try
        {
            await AppendLineSerializedAsync(line);
        }
        catch
        {
            // Intentionally swallow logging failures for now.
        }
    }

    private async Task AppendLineSerializedAsync(string line)
    {
        await _writeLock.WaitAsync();
        try
        {
            await File.AppendAllTextAsync(_logFilePath, line + Environment.NewLine);
        }
        finally
        {
            _writeLock.Release();
        }
    }
}
