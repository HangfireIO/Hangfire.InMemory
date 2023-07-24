using System;
using System.Collections.Generic;

namespace Hangfire.InMemory.Entities
{
    internal sealed class StateEntry
    {
        private readonly Dictionary<string, string> _data;

        public StateEntry(string name, string reason, IDictionary<string, string> data, DateTime createdAt, StringComparer comparer)
        {
            _data = data != null ? new Dictionary<string, string>(data, comparer) : null;
            Name = name;
            Reason = reason;
            CreatedAt = createdAt;
        }

        public string Name { get; }
        public string Reason { get; }
        public DateTime CreatedAt { get; }

        public IDictionary<string, string> GetData()
        {
            return _data != null 
                ? new Dictionary<string, string>(_data, _data.Comparer)
                : null;
        }
    }
}