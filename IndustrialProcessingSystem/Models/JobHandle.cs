using System;
using System.Threading.Tasks;

namespace IndustrialProcessingSystem.Models;

public class JobHandle
{
    public Guid Id { get; set; }
    public Task<int> Result { get; set; } = null!;
}
