// This file is part of Hangfire.InMemory. Copyright © 2020 Hangfire OÜ.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Hangfire.InMemory.Entities;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryState
    {
        private readonly JobStateCreatedAtComparer _jobEntryComparer;

        // State index uses case-insensitive comparisons, despite of the current settings. SQL Server
        // uses case-insensitive by default, and Redis doesn't use state index that's based on user values.
        private readonly Dictionary<string, SortedSet<JobEntry>> _jobStateIndex = new Dictionary<string, SortedSet<JobEntry>>(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, LockEntry<JobStorageConnection>> _locks;
        private readonly ConcurrentDictionary<string, JobEntry> _jobs;
        private readonly Dictionary<string, HashEntry> _hashes;
        private readonly Dictionary<string, ListEntry> _lists;
        private readonly Dictionary<string, SetEntry> _sets;
        private readonly Dictionary<string, CounterEntry> _counters;
        private readonly ConcurrentDictionary<string, QueueEntry> _queues;
        private readonly Dictionary<string, ServerEntry> _servers;

        public InMemoryState(InMemoryStorageOptions options)
        {
            Options = options;

            _jobEntryComparer = new JobStateCreatedAtComparer(options.StringComparer);

            _locks = CreateDictionary<LockEntry<JobStorageConnection>>(options.StringComparer);
            _jobs = CreateConcurrentDictionary<JobEntry>(options.StringComparer);
            _hashes = CreateDictionary<HashEntry>(options.StringComparer);
            _lists = CreateDictionary<ListEntry>(options.StringComparer);
            _sets = CreateDictionary<SetEntry>(options.StringComparer);
            _counters = CreateDictionary<CounterEntry>(options.StringComparer);
            _queues = CreateConcurrentDictionary<QueueEntry>(options.StringComparer);
            _servers = CreateDictionary<ServerEntry>(options.StringComparer);

            var expirableEntryComparer = new ExpirableEntryComparer(options.StringComparer);
            ExpiringJobsIndex = new SortedSet<JobEntry>(expirableEntryComparer);
            ExpiringCountersIndex = new SortedSet<CounterEntry>(expirableEntryComparer);
            ExpiringHashesIndex = new SortedSet<HashEntry>(expirableEntryComparer);
            ExpiringListsIndex = new SortedSet<ListEntry>(expirableEntryComparer);
            ExpiringSetsIndex = new SortedSet<SetEntry>(expirableEntryComparer);
        }

        public InMemoryStorageOptions Options { get; }

#if NET451
        public ConcurrentDictionary<string, JobEntry> Jobs => _jobs;
#else
        public IReadOnlyDictionary<string, JobEntry> Jobs => _jobs;
#endif
        public IDictionary<string, LockEntry<JobStorageConnection>> Locks => _locks;
        public IReadOnlyDictionary<string, HashEntry> Hashes => _hashes;
        public IReadOnlyDictionary<string, ListEntry> Lists => _lists;
        public IReadOnlyDictionary<string, SetEntry> Sets => _sets;
        public IReadOnlyDictionary<string, CounterEntry> Counters => _counters;
#if NET451
        public ConcurrentDictionary<string, QueueEntry> Queues => _queues;
#else
        public IReadOnlyDictionary<string, QueueEntry> Queues => _queues;
#endif
        public IReadOnlyDictionary<string, ServerEntry> Servers => _servers;

        public IReadOnlyDictionary<string, SortedSet<JobEntry>> JobStateIndex => _jobStateIndex;

        public SortedSet<JobEntry> ExpiringJobsIndex { get; }
        public SortedSet<CounterEntry> ExpiringCountersIndex { get; }
        public SortedSet<HashEntry> ExpiringHashesIndex { get; }
        public SortedSet<ListEntry> ExpiringListsIndex { get; }
        public SortedSet<SetEntry> ExpiringSetsIndex { get; }

        public QueueEntry QueueGetOrCreate(string name)
        {
            if (!_queues.TryGetValue(name, out var entry))
            {
                entry = _queues.GetOrAdd(name, _ => new QueueEntry());
            }

            return entry;
        }

        public void JobCreate(JobEntry entry, MonotonicTime now, TimeSpan? expireIn, bool ignoreMaxExpirationTime = false)
        {
            if (!_jobs.TryAdd(entry.Key, entry))
            {
                throw new InvalidOperationException($"Background job with key '{entry.Key}' already exists.");
            }

            EntryExpire(entry, ExpiringJobsIndex, now, expireIn, ignoreMaxExpirationTime);
        }

        public void JobSetState(JobEntry entry, StateEntry state)
        {
            if (entry.State != null && _jobStateIndex.TryGetValue(entry.State.Name, out var indexEntry))
            {
                indexEntry.Remove(entry);
                if (indexEntry.Count == 0) _jobStateIndex.Remove(entry.State.Name);
            }

            entry.State = state;

            if (!_jobStateIndex.TryGetValue(state.Name, out indexEntry))
            {
                _jobStateIndex.Add(state.Name, indexEntry = new SortedSet<JobEntry>(_jobEntryComparer));
            }

            indexEntry.Add(entry);
        }

        public void JobExpire(JobEntry entry, MonotonicTime now, TimeSpan? expireIn)
        {
            EntryExpire(entry, ExpiringJobsIndex, now, expireIn);
        }

        public void JobDelete(JobEntry entry)
        {
            EntryRemove(entry, _jobs, ExpiringJobsIndex);

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
                _hashes.Add(key, hash = new HashEntry(key, Options.StringComparer));
            }

            return hash;
        }

        public void HashExpire(HashEntry hash, MonotonicTime now, TimeSpan? expireIn)
        {
            EntryExpire(hash, ExpiringHashesIndex, now, expireIn);
        }

        public void HashDelete(HashEntry hash)
        {
            EntryRemove(hash, _hashes, ExpiringHashesIndex);
        }

        public SetEntry SetGetOrAdd(string key)
        {
            if (!_sets.TryGetValue(key, out var set))
            {
                _sets.Add(key, set = new SetEntry(key, Options.StringComparer));
            }

            return set;
        }

        public void SetExpire(SetEntry set, MonotonicTime now, TimeSpan? expireIn)
        {
            EntryExpire(set, ExpiringSetsIndex, now, expireIn);
        }

        public void SetDelete(SetEntry set)
        {
            EntryRemove(set, _sets, ExpiringSetsIndex);
        }

        public ListEntry ListGetOrAdd(string key)
        {
            if (!_lists.TryGetValue(key, out var list))
            {
                _lists.Add(key, list = new ListEntry(key, Options.StringComparer));
            }

            return list;
        }

        public void ListExpire(ListEntry entry, MonotonicTime now, TimeSpan? expireIn)
        {
            EntryExpire(entry, ExpiringListsIndex, now, expireIn);
        }

        public void ListDelete(ListEntry list)
        {
            EntryRemove(list, _lists, ExpiringListsIndex);
        }

        public CounterEntry CounterGetOrAdd(string key)
        {
            if (!_counters.TryGetValue(key, out var counter))
            {
                _counters.Add(key, counter = new CounterEntry(key));
            }

            return counter;
        }

        public void CounterExpire(CounterEntry counter, MonotonicTime now, TimeSpan? expireIn)
        {
            // We don't apply MaxExpirationTime rules for counters, because they
            // usually have fixed size, and because statistics should be kept for
            // days.
            EntryExpire(counter, ExpiringCountersIndex, now, expireIn, ignoreMaxExpirationTime: true);
        }

        public void CounterDelete(CounterEntry entry)
        {
            EntryRemove(entry, _counters, ExpiringCountersIndex);
        }

        public void ServerAdd(string serverId, ServerEntry entry)
        {
            _servers.Add(serverId, entry);
        }

        public void ServerRemove(string serverId)
        {
            _servers.Remove(serverId);
        }

        private static void EntryRemove<T>(T entry, IDictionary<string, T> index, ISet<T> expirationIndex)
            where T : IExpirableEntry
        {
            index.Remove(entry.Key);

            if (entry.ExpireAt.HasValue)
            {
                expirationIndex.Remove(entry);
            }
        }

        private void EntryExpire<T>(T entry, ISet<T> index, MonotonicTime now, TimeSpan? expireIn, bool ignoreMaxExpirationTime = false)
            where T : IExpirableEntry
        {
            if (entry.ExpireAt.HasValue)
            {
                index.Remove(entry);
            }

            if (expireIn.HasValue)
            {
                if (!ignoreMaxExpirationTime && Options.MaxExpirationTime.HasValue && expireIn > Options.MaxExpirationTime)
                {
                    expireIn = Options.MaxExpirationTime;
                }

                entry.ExpireAt = now.Add(expireIn.Value);
                index.Add(entry);
            }
            else
            {
                entry.ExpireAt = null;
            }
        }

        private static Dictionary<string, T> CreateDictionary<T>(StringComparer comparer)
        {
            return new Dictionary<string, T>(comparer);
        }

        private static ConcurrentDictionary<string, T> CreateConcurrentDictionary<T>(StringComparer comparer)
        {
            return new ConcurrentDictionary<string, T>(comparer);
        }
    }
}