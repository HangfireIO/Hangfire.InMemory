using System;
using System.Collections.Concurrent;
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
        public ListEntry(string id)
        {
            Key = id;
        }

        public string Key { get; }
        public List<string> Value { get; set; } = new List<string>();
        public DateTime? ExpireAt { get; set; }
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

    internal sealed class SetEntry : IExpirableEntry
    {
        public SetEntry(string id)
        {
            Key = id;
        }

        public string Key { get; }
        public SortedSet<SortedSetEntry<string>> Value { get; } = new SortedSet<SortedSetEntry<string>>(new SortedSetEntryComparer<string>());

        // TODO: What about case sensitivity here?
        public IDictionary<string, SortedSetEntry<string>> Hash { get; } = new Dictionary<string, SortedSetEntry<string>>();
        public DateTime? ExpireAt { get; set; }
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
        public string Key { get; set; }
        public InvocationData InvocationData { get; set; }

        // TODO What case sensitivity to use here?
        public IDictionary<string, string> Parameters { get; set; }

        public StateEntry State { get; set; }
        public ICollection<StateEntry> History { get; set; } = new List<StateEntry>();
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

    internal sealed class MemoryState
    {
        // TODO: We can remove indexes when empty and re-create them when required to always have minimum size 
        internal readonly SortedSet<BackgroundJobEntry> _jobIndex = new SortedSet<BackgroundJobEntry>(new ExpirableEntryComparer());
        internal readonly SortedSet<CounterEntry> _counterIndex = new SortedSet<CounterEntry>(new ExpirableEntryComparer());
        internal readonly SortedSet<HashEntry> _hashIndex = new SortedSet<HashEntry>(new ExpirableEntryComparer());
        internal readonly SortedSet<ListEntry> _listIndex = new SortedSet<ListEntry>(new ExpirableEntryComparer());
        internal readonly SortedSet<SetEntry> _setIndex = new SortedSet<SetEntry>(new ExpirableEntryComparer());

        internal readonly IDictionary<string, SortedSet<BackgroundJobEntry>> _jobStateIndex = new Dictionary<string, SortedSet<BackgroundJobEntry>>(StringComparer.OrdinalIgnoreCase);

        // TODO: We can remove dictionaries when empty and re-create them when required to always have minimum size
        internal readonly IDictionary<string, LockEntry> _locks = CreateDictionary<LockEntry>();
        internal readonly IDictionary<string, BackgroundJobEntry> _jobs = CreateDictionary<BackgroundJobEntry>();
        internal readonly IDictionary<string, HashEntry> _hashes = CreateDictionary<HashEntry>();
        internal readonly IDictionary<string, ListEntry> _lists = CreateDictionary<ListEntry>();
        internal readonly IDictionary<string, SetEntry> _sets = CreateDictionary<SetEntry>();
        internal readonly IDictionary<string, BlockingCollection<string>> _queues = CreateDictionary<BlockingCollection<string>>();
        internal readonly IDictionary<string, CounterEntry> _counters = CreateDictionary<CounterEntry>();
        internal readonly IDictionary<string, ServerEntry> _servers = CreateDictionary<ServerEntry>();

        private static IDictionary<string, T> CreateDictionary<T>()
        {
            return new Dictionary<string, T>(StringComparer.Ordinal);
        }
    }
}