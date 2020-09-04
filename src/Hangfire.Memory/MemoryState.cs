using System;
using System.Collections;
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
        private readonly Dictionary<string, BackgroundJobEntry> _jobs = CreateDictionary<BackgroundJobEntry>();
        private readonly Dictionary<string, HashEntry> _hashes = CreateDictionary<HashEntry>();
        private readonly Dictionary<string, ListEntry> _lists = CreateDictionary<ListEntry>();
        private readonly Dictionary<string, SetEntry> _sets = CreateDictionary<SetEntry>();
        private readonly Dictionary<string, CounterEntry> _counters = CreateDictionary<CounterEntry>();
        private readonly Dictionary<string, BlockingCollection<string>> _queues = CreateDictionary<BlockingCollection<string>>();
        internal readonly IDictionary<string, ServerEntry> _servers = CreateDictionary<ServerEntry>();

        public IReadOnlyDictionary<string, BackgroundJobEntry> Jobs => _jobs;
        public IReadOnlyDictionary<string, HashEntry> Hashes => _hashes;
        public IReadOnlyDictionary<string, ListEntry> Lists => _lists;
        public IReadOnlyDictionary<string, SetEntry> Sets => _sets;
        public IReadOnlyDictionary<string, CounterEntry> Counters => _counters;
        public IReadOnlyDictionary<string, BlockingCollection<string>> Queues => _queues;

        public BlockingCollection<string> QueueGetOrCreate(string name)
        {
            if (!_queues.TryGetValue(name, out var queue))
            {
                // TODO: Refactor this to unify creation of a queue
                _queues.Add(name, queue = new BlockingCollection<string>());
            }

            return queue;
        }

        public BackgroundJobEntry JobGetOrThrow(string jobId)
        {
            if (!_jobs.TryGetValue(jobId, out var backgroundJob))
            {
                throw new InvalidOperationException($"Background job with '{jobId}' identifier does not exist.");
            }

            return backgroundJob;
        }

        public void JobCreate(BackgroundJobEntry job)
        {
            _jobs.Add(job.Key, job);
            _jobIndex.Add(job);
        }

        public void JobSetState(BackgroundJobEntry job, StateEntry state)
        {
            if (job.State != null && _jobStateIndex.TryGetValue(job.State.Name, out var indexEntry))
            {
                indexEntry.Remove(job);
                if (indexEntry.Count == 0) _jobStateIndex.Remove(job.State.Name);
            }

            job.State = state;
            job.History.Add(state);

            if (!_jobStateIndex.TryGetValue(state.Name, out indexEntry))
            {
                _jobStateIndex.Add(state.Name, indexEntry = new SortedSet<BackgroundJobEntry>(new BackgroundJobStateCreatedAtComparer()));
            }

            indexEntry.Add(job);
        }

        public void JobExpire(BackgroundJobEntry job, TimeSpan? expireIn)
        {
            EntryExpire(job, _jobIndex, expireIn);
        }

        public void JobDelete(BackgroundJobEntry entry)
        {
            if (entry.ExpireAt.HasValue)
            {
                _jobIndex.Remove(entry);
            }

            _jobs.Remove(entry.Key);

            if (entry.State?.Name != null && _jobStateIndex.TryGetValue(entry.State.Name, out var stateIndex))
            {
                stateIndex.Remove(entry);
                if (stateIndex.Count == 0) _jobStateIndex.Remove(entry.State.Name);
            }
        }

        public HashEntry HashGetOrAdd(string key)
        {
            if (!_hashes.TryGetValue(key, out var hash))
            {
                _hashes.Add(key, hash = new HashEntry(key));
            }

            return hash;
        }

        public void HashExpire(HashEntry hash, TimeSpan? expireIn)
        {
            EntryExpire(hash, _hashIndex, expireIn);
        }

        public void HashDelete(HashEntry hash)
        {
            _hashes.Remove(hash.Key);
            if (hash.ExpireAt.HasValue)
            {
                _hashIndex.Remove(hash);
            }
        }

        public SetEntry SetGetOrAdd(string key)
        {
            if (!_sets.TryGetValue(key, out var set))
            {
                _sets.Add(key, set = new SetEntry(key));
            }

            return set;
        }

        public void SetExpire(SetEntry set, TimeSpan? expireIn)
        {
            EntryExpire(set, _setIndex, expireIn);
        }

        public void SetDelete(SetEntry set)
        {
            _sets.Remove(set.Key);

            if (set.ExpireAt.HasValue)
            {
                _setIndex.Remove(set);
            }
        }

        public ListEntry ListGetOrAdd(string key)
        {
            if (!_lists.TryGetValue(key, out var list))
            {
                _lists.Add(key, list = new ListEntry(key));
            }

            return list;
        }

        public void ListExpire(ListEntry entry, TimeSpan? expireIn)
        {
            EntryExpire(entry, _listIndex, expireIn);
        }

        public void ListDelete(ListEntry list)
        {
            _lists.Remove(list.Key);

            if (list.ExpireAt.HasValue)
            {
                _listIndex.Remove(list);
            }
        }

        public CounterEntry CounterGetOrAdd(string key)
        {
            if (!_counters.TryGetValue(key, out var counter))
            {
                _counters.Add(key, counter = new CounterEntry(key));
            }

            return counter;
        }

        public bool CounterTryGet(string key, out CounterEntry entry)
        {
            return _counters.TryGetValue(key, out entry);
        }

        public void CounterExpire(CounterEntry counter, TimeSpan? expireIn)
        {
            EntryExpire(counter, _counterIndex, expireIn);
        }

        public void CounterDelete(CounterEntry entry)
        {
            _counters.Remove(entry.Key);

            if (entry.ExpireAt.HasValue)
            {
                _counterIndex.Remove(entry);
            }
        }

        private static void EntryExpire<T>(T entity, ISet<T> index, TimeSpan? expireIn)
            where T : IExpirableEntry
        {
            if (entity.ExpireAt.HasValue)
            {
                index.Remove(entity);
            }

            if (expireIn.HasValue)
            {
                // TODO: Replace DateTime.UtcNow everywhere with some time factory
                entity.ExpireAt = DateTime.UtcNow.Add(expireIn.Value);
                index.Add(entity);
            }
            else
            {
                entity.ExpireAt = null;
            }
        }

        private static Dictionary<string, T> CreateDictionary<T>()
        {
            return new Dictionary<string, T>(StringComparer.Ordinal);
        }
    }
}