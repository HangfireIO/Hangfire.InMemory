using System;
using System.Collections;
using System.Collections.Generic;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Memory
{
    internal interface IExpirableEntry
    {
        string Key { get; }
        DateTime? ExpireAt { get; set; }
    }

    internal sealed class ListEntry : IExpirableEntry
    {
        private List<string> _value = new List<string>();

        public ListEntry(string id)
        {
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

        public void Remove(string value)
        {
            _value.Remove(value);
        }

        internal void Update(List<string> value)
        {
            _value = value;
        }
    }

    internal sealed class HashEntry : IExpirableEntry
    {
        public HashEntry(string id)
        {
            Key = id;
        }

        public string Key { get; }
        // TODO: What about case sensitivity here?
        public IDictionary<string, string> Value { get; } = new Dictionary<string, string>();
        public DateTime? ExpireAt { get; set; }
    }

    internal sealed class SetEntry : IExpirableEntry, IEnumerable<SortedSetEntry<string>>
    {
        // TODO: What about case sensitivity here?
        private readonly IDictionary<string, SortedSetEntry<string>> _hash = new Dictionary<string, SortedSetEntry<string>>();
        private readonly SortedSet<SortedSetEntry<string>> _value = new SortedSet<SortedSetEntry<string>>(new SortedSetEntryComparer<string>());

        public SetEntry(string id)
        {
            Key = id;
        }

        public string Key { get; }
        public DateTime? ExpireAt { get; set; }

        public int Count => _value.Count;

        public void Add(string value, double score)
        {
            if (!_hash.TryGetValue(value, out var entry))
            {
                entry = new SortedSetEntry<string>(value) { Score = score };
                _value.Add(entry);
                _hash.Add(value, entry);
            }
            else
            {
                // Element already exists, just need to add a score value – re-create it.
                _value.Remove(entry);
                entry.Score = score;
                _value.Add(entry);
            }
        }

        public void Remove(string value)
        {
            if (_hash.TryGetValue(value, out var entry))
            {
                _value.Remove(entry);
                _hash.Remove(value);
            }
        }

        public IEnumerator<SortedSetEntry<string>> GetEnumerator()
        {
            return _value.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

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

    internal sealed class BackgroundJobEntry : IExpirableEntry
    {
        private const int StateCountForRegularJob = 4; // (Scheduled) -> Enqueued -> Processing -> Succeeded

        public string Key { get; set; }
        public InvocationData InvocationData { get; set; }

        // TODO What case sensitivity to use here?
        public IDictionary<string, string> Parameters { get; set; }

        public StateEntry State { get; set; }
        public ICollection<StateEntry> History { get; set; } = new List<StateEntry>(StateCountForRegularJob);
        public DateTime CreatedAt { get; set; }
        public DateTime? ExpireAt { get; set; }
    }

    internal sealed class ServerEntry
    {
        public ServerContext Context { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime HeartbeatAt { get; set; }
    }

    internal sealed class LockEntry
    {
        public MemoryConnection Owner { get; set; }
        public int ReferenceCount { get; set; }
        public int Level { get; set; }
    }

    internal sealed class SortedSetEntry<T>
    {
        public SortedSetEntry(T value)
        {
            Value = value;
        }

        public T Value { get; }
        public double Score { get; set; }
    }

    internal sealed class StateEntry
    {
        public string Name { get; set; }
        public string Reason { get; set; }
        public IDictionary<string, string> Data { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    internal sealed class SortedSetEntryComparer<T> : IComparer<SortedSetEntry<T>>
        where T : IComparable<T>
    {
        public int Compare(SortedSetEntry<T> x, SortedSetEntry<T> y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (ReferenceEquals(null, y)) return 1;
            if (ReferenceEquals(null, x)) return -1;

            var scoreComparison = x.Score.CompareTo(y.Score);
            if (scoreComparison != 0) return scoreComparison;

            return x.Value.CompareTo(y.Value);
        }
    }

    internal sealed class BackgroundJobStateCreatedAtComparer : IComparer<BackgroundJobEntry>
    {
        public int Compare(BackgroundJobEntry x, BackgroundJobEntry y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (y?.State == null) return 1;
            if (x?.State == null) return -1;

            var createdAtComparison = x.State.CreatedAt.CompareTo(y.State.CreatedAt);
            if (createdAtComparison != 0) return createdAtComparison;

            return String.Compare(x.Key, y.Key, StringComparison.Ordinal);
        }
    }

    internal sealed class ExpirableEntryComparer : IComparer<IExpirableEntry>
    {
        public int Compare(IExpirableEntry x, IExpirableEntry y)
        {
            if (ReferenceEquals(x, y)) return 0;

            // TODO: Nulls last, our indexes shouldn't contain nulls anyway
            // TODO: Check this
            if (x == null) return -1;
            if (y == null) return +1;

            if (!x.ExpireAt.HasValue)
            {
                throw new InvalidOperationException("Left side does not contain ExpireAt value");
            }

            if (!y.ExpireAt.HasValue)
            {
                throw new InvalidOperationException("Right side does not contain ExpireAt value");
            }

            var expirationCompare = x.ExpireAt.Value.CompareTo(y.ExpireAt.Value);
            if (expirationCompare != 0) return expirationCompare;

            return String.Compare(x.Key, y.Key, StringComparison.Ordinal);
        }
    }
}
