using System;

namespace Hangfire.InMemory.Entities
{
    internal sealed class CounterEntry : IExpirableEntry
    {
        public CounterEntry(string id)
        {
            Key = id;
        }

        public string Key { get; }
        public long Value { get; set; }
        public DateTime? ExpireAt { get; set; }
    }
}