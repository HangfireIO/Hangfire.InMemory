using System;
using System.Collections.Generic;

namespace Hangfire.InMemory.Entities
{
    internal sealed class HashEntry : IExpirableEntry
    {
        public HashEntry(string id, StringComparer comparer)
        {
            Key = id;
            Value = new Dictionary<string, string>(comparer);
        }

        public string Key { get; }
        public IDictionary<string, string> Value { get; }
        public DateTime? ExpireAt { get; set; }
    }
}