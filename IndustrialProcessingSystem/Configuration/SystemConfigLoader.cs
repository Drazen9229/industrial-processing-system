using System.Xml.Linq;
using IndustrialProcessingSystem.Enums;
using IndustrialProcessingSystem.Models;

namespace IndustrialProcessingSystem.Configuration;

public class SystemConfigLoader
{
    public SystemConfig Load(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("Configuration file path is required.", nameof(filePath));
        }

        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"Configuration file was not found: {filePath}", filePath);
        }

        XDocument document;
        try
        {
            document = XDocument.Load(filePath);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to read XML configuration from '{filePath}'.", ex);
        }

        var root = document.Element("SystemConfig")
                   ?? throw new InvalidOperationException("Missing root element 'SystemConfig'.");

        var workerCount = ParsePositiveInt(root.Element("WorkerCount"), "WorkerCount");
        var maxQueueSize = ParsePositiveInt(root.Element("MaxQueueSize"), "MaxQueueSize");

        var jobsElement = root.Element("Jobs")
                         ?? throw new InvalidOperationException("Missing required element 'Jobs'.");

        var jobs = new List<Job>();
        var jobElements = jobsElement.Elements("Job").ToList();

        for (var i = 0; i < jobElements.Count; i++)
        {
            var jobElement = jobElements[i];
            jobs.Add(ParseJob(jobElement, i));
        }

        return new SystemConfig
        {
            WorkerCount = workerCount,
            MaxQueueSize = maxQueueSize,
            InitialJobs = jobs
        };
    }

    private static int ParsePositiveInt(XElement? element, string elementName)
    {
        if (element is null)
        {
            throw new InvalidOperationException($"Missing required element '{elementName}'.");
        }

        if (!int.TryParse(element.Value, out var value))
        {
            throw new InvalidOperationException($"Element '{elementName}' must be a valid integer.");
        }

        if (value <= 0)
        {
            throw new InvalidOperationException($"Element '{elementName}' must be greater than 0.");
        }

        return value;
    }

    private static Job ParseJob(XElement jobElement, int index)
    {
        var typeText = jobElement.Attribute("Type")?.Value;
        if (string.IsNullOrWhiteSpace(typeText))
        {
            throw new InvalidOperationException($"Job at index {index} is missing required attribute 'Type'.");
        }

        if (!Enum.TryParse<JobType>(typeText, ignoreCase: true, out var jobType))
        {
            throw new InvalidOperationException($"Job at index {index} has invalid Type '{typeText}'.");
        }

        var payload = jobElement.Attribute("Payload")?.Value;
        if (string.IsNullOrWhiteSpace(payload))
        {
            throw new InvalidOperationException($"Job at index {index} is missing required attribute 'Payload'.");
        }

        var priorityText = jobElement.Attribute("Priority")?.Value;
        if (string.IsNullOrWhiteSpace(priorityText))
        {
            throw new InvalidOperationException($"Job at index {index} is missing required attribute 'Priority'.");
        }

        if (!int.TryParse(priorityText, out var priority) || priority <= 0)
        {
            throw new InvalidOperationException($"Job at index {index} has invalid Priority '{priorityText}'. Priority must be greater than 0.");
        }

        var idText = jobElement.Attribute("Id")?.Value;
        Guid id;
        if (string.IsNullOrWhiteSpace(idText))
        {
            id = Guid.NewGuid();
        }
        else if (!Guid.TryParse(idText, out id))
        {
            throw new InvalidOperationException($"Job at index {index} has invalid Id '{idText}'.");
        }

        if (id == Guid.Empty)
        {
            throw new InvalidOperationException($"Job at index {index} has an empty Id.");
        }

        return new Job
        {
            Id = id,
            Type = jobType,
            Payload = payload,
            Priority = priority
        };
    }
}
