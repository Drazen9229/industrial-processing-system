using System.Reflection;
using IndustrialProcessingSystem.Configuration;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Models;
using IndustrialProcessingSystem.Services;
using Xunit;
using Xunit.Sdk;

namespace IndustrialProcessingSystem.Tests;

public class ProcessingSystemTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public void Submit_ReturnsHandleWithSameJobId()
    {
        var system = CreateSystem();
        var job = CreatePrimeJob(Guid.NewGuid(), 3);

        var handle = system.Submit(job);

        Assert.Equal(job.Id, handle.Id);
    }

    [Fact]
    public void Submit_RejectsDuplicateJobId()
    {
        var system = CreateSystem();
        var jobId = Guid.NewGuid();

        system.Submit(CreatePrimeJob(jobId, 2));

        Assert.Throws<InvalidOperationException>(() => system.Submit(CreateIoJob(jobId, 1)));
    }

    [Fact]
    public void GetTopJobs_ReturnsPriorityOrder_AndDoesNotMutateQueue()
    {
        var first = CreatePrimeJob(Guid.NewGuid(), 5);
        var second = CreatePrimeJob(Guid.NewGuid(), 1);
        var third = CreateIoJob(Guid.NewGuid(), 3);
        var system = CreateSystem(initialJobs: [first, second, third]);

        var top = system.GetTopJobs(3).Select(job => job.Id).ToArray();
        var topAgain = system.GetTopJobs(3).Select(job => job.Id).ToArray();

        Assert.Equal([second.Id, third.Id, first.Id], top);
        Assert.Equal(top, topAgain);
        Assert.Equal(3, system.QueuedCount);
    }

    [Fact]
    public void GetJob_ReturnsExpectedJob()
    {
        var job = CreatePrimeJob(Guid.NewGuid(), 4);
        var system = CreateSystem(initialJobs: [job]);

        var result = system.GetJob(job.Id);

        Assert.Equal(job.Id, result.Id);
        Assert.Equal(job.Type, result.Type);
        Assert.Equal(job.Payload, result.Payload);
        Assert.Equal(job.Priority, result.Priority);
    }

    [Fact]
    public void PrimePayloadThreadCount_IsConstrainedToOneAndEight()
    {
        var method = typeof(ProcessingSystem).GetMethod("ParsePrimePayload", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var lowerResult = (ValueTuple<int, int>)method!.Invoke(null, new object?[] { "numbers:100,threads:0" })!;
        var upperResult = (ValueTuple<int, int>)method.Invoke(null, new object?[] { "numbers:100,threads:99" })!;

        Assert.Equal(1, lowerResult.Item2);
        Assert.Equal(8, upperResult.Item2);
    }

    [Fact]
    public async Task PrimeJob_ProcessesSuccessfully()
    {
        var system = CreateSystem(workerCount: 1);
        var job = CreatePrimeJob(Guid.NewGuid(), 1, "numbers:100,threads:4");

        system.Start();
        var handle = system.Submit(job);
        var result = await WaitWithTimeoutAsync(handle.Result);

        Assert.Equal(25, result);
    }

    [Fact]
    public async Task IoJob_ProcessesSuccessfully()
    {
        var system = CreateSystem(workerCount: 1);
        var job = CreateIoJob(Guid.NewGuid(), 1, "delay:50");

        system.Start();
        var handle = system.Submit(job);
        var result = await WaitWithTimeoutAsync(handle.Result);

        Assert.InRange(result, 0, 100);
    }

    [Fact]
    public async Task InvalidJob_RetriesAndRaisesAbortEvent()
    {
        var system = CreateSystem(workerCount: 1);
        var job = CreatePrimeJob(Guid.NewGuid(), 1, "numbers:1,threads:2");
        var abortSignal = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failedAttempts = 0;

        system.JobFailed += (_, _) => Interlocked.Increment(ref failedAttempts);
        system.JobAborted += (abortedJob, _) => abortSignal.TrySetResult(abortedJob.Id);

        system.Start();
        var handle = system.Submit(job);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await WaitWithTimeoutAsync(handle.Result));
        var abortedJobId = await WaitWithTimeoutAsync(abortSignal.Task);

        Assert.Equal(2, Volatile.Read(ref failedAttempts));
        Assert.Equal(job.Id, abortedJobId);
    }

    [Fact]
    public void Submit_RejectsWhenInSystemCapacityIsReached()
    {
        var system = CreateSystem(maxQueueSize: 2, initialJobs: [CreatePrimeJob(Guid.NewGuid(), 1)]);

        system.Submit(CreateIoJob(Guid.NewGuid(), 2));

        Assert.Throws<InvalidOperationException>(() => system.Submit(CreatePrimeJob(Guid.NewGuid(), 3)));
    }

    private static ProcessingSystem CreateSystem(int workerCount = 1, int maxQueueSize = 16, IEnumerable<Job>? initialJobs = null)
    {
        return new ProcessingSystem(new SystemConfig
        {
            WorkerCount = workerCount,
            MaxQueueSize = maxQueueSize,
            InitialJobs = initialJobs?.ToList() ?? []
        });
    }

    private static Job CreatePrimeJob(Guid id, int priority, string payload = "numbers:30,threads:2")
    {
        return new Job
        {
            Id = id,
            Type = JobType.Prime,
            Payload = payload,
            Priority = priority
        };
    }

    private static Job CreateIoJob(Guid id, int priority, string payload = "delay:0")
    {
        return new Job
        {
            Id = id,
            Type = JobType.IO,
            Payload = payload,
            Priority = priority
        };
    }

    private static async Task<T> WaitWithTimeoutAsync<T>(Task<T> task)
    {
        var timeoutTask = Task.Delay(TestTimeout);
        var completedTask = await Task.WhenAny(task, timeoutTask);
        if (completedTask != task)
        {
            throw new XunitException($"Task did not complete within {TestTimeout.TotalSeconds:0} seconds.");
        }

        return await task;
    }
}
