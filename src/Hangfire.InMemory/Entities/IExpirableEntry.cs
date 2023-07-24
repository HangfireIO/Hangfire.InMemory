using System;

namespace Hangfire.InMemory.Entities
{
    internal interface IExpirableEntry
    {
        string Key { get; }
        DateTime? ExpireAt { get; set; }
    }
}