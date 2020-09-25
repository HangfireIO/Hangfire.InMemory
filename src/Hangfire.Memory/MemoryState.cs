using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Hangfire.Memory
{
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
        private readonly ConcurrentDictionary<string, BackgroundJobEntry> _jobs = CreateConcurrentDictionary<BackgroundJobEntry>();
        private readonly Dictionary<string, HashEntry> _hashes = CreateDictionary<HashEntry>();
        private readonly Dictionary<string, ListEntry> _lists = CreateDictionary<ListEntry>();
        private readonly Dictionary<string, SetEntry> _sets = CreateDictionary<SetEntry>();
        private readonly Dictionary<string, CounterEntry> _counters = CreateDictionary<CounterEntry>();
        private readonly ConcurrentDictionary<string, BlockingCollection<string>> _queues = CreateConcurrentDictionary<BlockingCollection<string>>();
        private readonly Dictionary<string, ServerEntry> _servers = CreateDictionary<ServerEntry>();

        public ConcurrentDictionary<string, BackgroundJobEntry> Jobs => _jobs; // TODO Implement workaround for net45 to return IReadOnlyDictionary (and the same for _queues)
        public IReadOnlyDictionary<string, HashEntry> Hashes => _hashes;
        public IReadOnlyDictionary<string, ListEntry> Lists => _lists;
        public IReadOnlyDictionary<string, SetEntry> Sets => _sets;
        public IReadOnlyDictionary<string, CounterEntry> Counters => _counters;
        public ConcurrentDictionary<string, BlockingCollection<string>> Queues => _queues; // net45 target does not have ConcurrentDictionary that implements IReadOnlyDictionary
        public IReadOnlyDictionary<string, ServerEntry> Servers => _servers;

        public BlockingCollection<string> QueueGetOrCreate(string name)
        {
            if (!_queues.TryGetValue(name, out var queue))
            {
                // TODO: Refactor this to unify creation of a queue
                _queues.GetOrAdd(name, _ => queue = new BlockingCollection<string>());
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
            if (!_jobs.TryAdd(job.Key, job))
            {
                // TODO: Panic
            }

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

            _jobs.TryRemove(entry.Key, out _);

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

        public void ServerAdd(string serverId, ServerEntry entry)
        {
            _servers.Add(serverId, entry);
        }

        public void ServerRemove(string serverId)
        {
            _servers.Remove(serverId);
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

        private static ConcurrentDictionary<string, T> CreateConcurrentDictionary<T>()
        {
            return new ConcurrentDictionary<string, T>(StringComparer.Ordinal);
        }
    }
}