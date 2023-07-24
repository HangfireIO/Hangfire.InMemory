using System;
using System.Collections;
using System.Collections.Generic;

namespace Hangfire.InMemory.Entities
{
    internal sealed class SetEntry : IExpirableEntry, IEnumerable<SortedSetItem>
    {
        private readonly IDictionary<string, SortedSetItem> _hash;
        private readonly SortedSet<SortedSetItem> _value;

        public SetEntry(string id, StringComparer stringComparer)
        {
            _hash = new Dictionary<string, SortedSetItem>(stringComparer);
            _value = new SortedSet<SortedSetItem>(new SortedSetItemComparer(stringComparer));
            Key = id;
        }

        public string Key { get; }
        public DateTime? ExpireAt { get; set; }

        public int Count => _value.Count;

        public void Add(string value, double score)
        {
            if (!_hash.TryGetValue(value, out var entry))
            {
                entry = new SortedSetItem(value, score);
                _value.Add(entry);
                _hash.Add(value, entry);
            }
            else
            {
                // Element already exists, just need to add a score value – re-create it.
                _value.Remove(entry);

                entry = new SortedSetItem(value, score);
                _value.Add(entry);
                _hash[value] = entry;
            }
        }

        public List<string> GetViewBetween(double from, double to, int count)
        {
            var view = _value.GetViewBetween(
                new SortedSetItem(null, from),
                new SortedSetItem(null, to));

            var result = new List<string>(view.Count);
            foreach (var entry in view)
            {
                if (count-- == 0) break;
                result.Add(entry.Value);
            }

            return result;
        }
        
        public string GetFirstBetween(double from, double to)
        {
            var view = _value.GetViewBetween(
                new SortedSetItem(null, from),
                new SortedSetItem(null, to));

            return view.Count > 0 ? view.Min.Value : null;
        }

        public void Remove(string value)
        {
            if (_hash.TryGetValue(value, out var entry))
            {
                _value.Remove(entry);
                _hash.Remove(value);
            }
        }

        public bool Contains(string value)
        {
            return _hash.ContainsKey(value);
        }

        public IEnumerator<SortedSetItem> GetEnumerator()
        {
            return _value.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}