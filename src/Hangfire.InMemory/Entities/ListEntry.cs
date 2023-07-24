using System;
using System.Collections.Generic;

namespace Hangfire.InMemory.Entities
{
    internal sealed class ListEntry : IExpirableEntry
    {
        private readonly StringComparer _stringComparer;
        private List<string> _value = new List<string>();

        public ListEntry(string id, StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
            Key = id;
        }

        public string Key { get; }
        public DateTime? ExpireAt { get; set; }

        public int Count => _value.Count;

        public string this[int index] => _value[Count - index - 1];

        public void Add(string value)
        {
            _value.Add(value);
        }

        public void RemoveAll(string value)
        {
            _value.RemoveAll(other => _stringComparer.Equals(value, other));
        }

        public int Trim(int keepStartingFrom, int keepEndingAt)
        {
            var from = Math.Max(0, keepStartingFrom);
            var to = Math.Min(_value.Count - 1, keepEndingAt);

            var result = new List<string>();

            for (var index = to; index >= from; index--)
            {
                result.Add(this[index]);
            }

            _value = result;
            return _value.Count;
        }
    }
}