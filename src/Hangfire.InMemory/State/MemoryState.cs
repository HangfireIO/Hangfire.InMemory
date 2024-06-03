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

namespace Hangfire.InMemory.State
{
    internal sealed class MemoryState<TKey>
        where TKey : IComparable<TKey>
    {
        private readonly JobStateCreatedAtComparer<TKey> _jobEntryComparer;

        public MemoryState(StringComparer stringComparer, IComparer<TKey>? keyComparer)
        {
            _jobEntryComparer = new JobStateCreatedAtComparer<TKey>(keyComparer);

            Locks = new ConcurrentDictionary<string, LockEntry<JobStorageConnection>>(stringComparer);
            Queues = new ConcurrentDictionary<string, QueueEntry<TKey>>(stringComparer);

            Jobs = new SortedDictionary<TKey, JobEntry<TKey>>(keyComparer);

            Hashes = new SortedDictionary<string, HashEntry>(stringComparer);
            Lists = new SortedDictionary<string, ListEntry>(stringComparer);
            Sets = new SortedDictionary<string, SetEntry>(stringComparer);
            Counters = new SortedDictionary<string, CounterEntry>(stringComparer);
            Servers = new SortedDictionary<string, ServerEntry>(stringComparer);

            ExpiringJobsIndex = new SortedSet<JobEntry<TKey>>(new ExpirableEntryComparer<TKey>(keyComparer));

            var expirableEntryComparer = new ExpirableEntryComparer<string>(stringComparer);
            ExpiringCountersIndex = new SortedSet<CounterEntry>(expirableEntryComparer);
            ExpiringHashesIndex = new SortedSet<HashEntry>(expirableEntryComparer);
            ExpiringListsIndex = new SortedSet<ListEntry>(expirableEntryComparer);
            ExpiringSetsIndex = new SortedSet<SetEntry>(expirableEntryComparer);

            StringComparer = stringComparer;
        }

        public StringComparer StringComparer { get; }

        public ConcurrentDictionary<string, LockEntry<JobStorageConnection>> Locks { get; }
        public ConcurrentDictionary<string, QueueEntry<TKey>> Queues { get; }

        public SortedDictionary<TKey, JobEntry<TKey>> Jobs { get; }
        public SortedDictionary<string, HashEntry> Hashes { get; }
        public SortedDictionary<string, ListEntry> Lists { get; }
        public SortedDictionary<string, SetEntry> Sets { get; }
        public SortedDictionary<string, CounterEntry> Counters { get; }
        public SortedDictionary<string, ServerEntry> Servers { get; }

        // State index uses case-insensitive comparisons, despite the current settings. SQL Server
        // uses case-insensitive by default, and Redis doesn't use state index that's based on user values.
        public Dictionary<string, SortedSet<JobEntry<TKey>>> JobStateIndex { get; } = new(StringComparer.OrdinalIgnoreCase);

        public SortedSet<JobEntry<TKey>> ExpiringJobsIndex { get; }
        public SortedSet<CounterEntry> ExpiringCountersIndex { get; }
        public SortedSet<HashEntry> ExpiringHashesIndex { get; }
        public SortedSet<ListEntry> ExpiringListsIndex { get; }
        public SortedSet<SetEntry> ExpiringSetsIndex { get; }

        public QueueEntry<TKey> QueueGetOrCreate(string name)
        {
            if (!Queues.TryGetValue(name, out var entry))
            {
                entry = Queues.GetOrAdd(name, _ => new QueueEntry<TKey>());
            }

            return entry;
        }

        public void JobCreate(JobEntry<TKey> entry, TimeSpan? expireIn)
        {
            Jobs[entry.Key] = entry;

            // Background job is not yet initialized after calling this method, and
            // transaction is expected a few moments later that will initialize this
            // job. To prevent early, non-expected eviction when max expiration time
            // limit is low or close to zero, that can lead to exceptions, we're just
            // ignoring this limit in very rare occasions when background job is not
            // initialized for reasons I can't even realize with an in-memory storage.
            if (EntryExpire<TKey, JobEntry<TKey>>(entry, ExpiringJobsIndex, entry.CreatedAt, expireIn, maxExpiration: null))
            {
                JobDelete(entry);
            }
        }

        public void JobSetState(JobEntry<TKey> entry, StateEntry state)
        {
            if (entry.State != null && JobStateIndex.TryGetValue(entry.State.Name, out var indexEntry))
            {
                indexEntry.Remove(entry);
                if (indexEntry.Count == 0) JobStateIndex.Remove(entry.State.Name);
            }

            entry.State = state;

            if (!JobStateIndex.TryGetValue(state.Name, out indexEntry))
            {
                JobStateIndex.Add(state.Name, indexEntry = new SortedSet<JobEntry<TKey>>(_jobEntryComparer));
            }

            indexEntry.Add(entry);
        }

        public void JobExpire(JobEntry<TKey> entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire<TKey, JobEntry<TKey>>(entry, ExpiringJobsIndex, now, expireIn, maxExpiration))
            {
                JobDelete(entry);
            }
        }

        public void JobDelete(JobEntry<TKey> entry)
        {
            EntryRemove(entry, Jobs, ExpiringJobsIndex);

            if (entry.State?.Name != null && JobStateIndex.TryGetValue(entry.State.Name, out var stateIndex))
            {
                stateIndex.Remove(entry);
                if (stateIndex.Count == 0) JobStateIndex.Remove(entry.State.Name);
            }
        }

        public long GetCountByStateName(string stateName)
        {
            if (JobStateIndex.TryGetValue(stateName, out var index))
            {
                return index.Count;
            }

            return 0;
        }

        public HashEntry HashGetOrAdd(string key)
        {
            if (!Hashes.TryGetValue(key, out var hash))
            {
                Hashes.Add(key, hash = new HashEntry(key, StringComparer));
            }

            return hash;
        }

        public void HashExpire(HashEntry hash, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire<string, HashEntry>(hash, ExpiringHashesIndex, now, expireIn, maxExpiration))
            {
                HashDelete(hash);
            }
        }

        public void HashDelete(HashEntry hash)
        {
            EntryRemove(hash, Hashes, ExpiringHashesIndex);
        }

        public SetEntry SetGetOrAdd(string key)
        {
            if (!Sets.TryGetValue(key, out var set))
            {
                Sets.Add(key, set = new SetEntry(key, StringComparer));
            }

            return set;
        }

        public void SetExpire(SetEntry set, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire<string, SetEntry>(set, ExpiringSetsIndex, now, expireIn, maxExpiration))
            {
                SetDelete(set);
            }
        }

        public void SetDelete(SetEntry set)
        {
            EntryRemove(set, Sets, ExpiringSetsIndex);
        }

        public ListEntry ListGetOrAdd(string key)
        {
            if (!Lists.TryGetValue(key, out var list))
            {
                Lists.Add(key, list = new ListEntry(key));
            }

            return list;
        }

        public void ListExpire(ListEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire<string, ListEntry>(entry, ExpiringListsIndex, now, expireIn, maxExpiration))
            {
                ListDelete(entry);
            }
        }

        public void ListDelete(ListEntry list)
        {
            EntryRemove(list, Lists, ExpiringListsIndex);
        }

        public CounterEntry CounterGetOrAdd(string key)
        {
            if (!Counters.TryGetValue(key, out var counter))
            {
                Counters.Add(key, counter = new CounterEntry(key));
            }

            return counter;
        }

        public void CounterExpire(CounterEntry counter, MonotonicTime? now, TimeSpan? expireIn)
        {
            // We don't apply MaxExpirationTime rules for counters, because they
            // usually have fixed size, and because statistics should be kept for
            // days.
            if (EntryExpire<string, CounterEntry>(counter, ExpiringCountersIndex, now, expireIn, maxExpiration: null))
            {
                CounterDelete(counter);
            }
        }

        public void CounterDelete(CounterEntry entry)
        {
            EntryRemove(entry, Counters, ExpiringCountersIndex);
        }

        public void ServerAdd(string serverId, ServerEntry entry)
        {
            Servers.Add(serverId, entry);
        }

        public void ServerRemove(string serverId)
        {
            Servers.Remove(serverId);
        }

        public void EvictExpiredEntries(MonotonicTime now)
        {
            EvictFromIndex<TKey, JobEntry<TKey>>(now, ExpiringJobsIndex, JobDelete);
            EvictFromIndex<string, HashEntry>(now, ExpiringHashesIndex, HashDelete);
            EvictFromIndex<string, ListEntry>(now, ExpiringListsIndex, ListDelete);
            EvictFromIndex<string, SetEntry>(now, ExpiringSetsIndex, SetDelete);
            EvictFromIndex<string, CounterEntry>(now, ExpiringCountersIndex, CounterDelete);
        }

        private static void EvictFromIndex<TEntryKey, TEntry>(MonotonicTime now, SortedSet<TEntry> index, Action<TEntry> action)
            where TEntry : IExpirableEntry<TEntryKey>
        {
            TEntry entry;

            while (index.Count > 0 && (entry = index.Min).ExpireAt.HasValue && now >= entry.ExpireAt)
            {
                action(entry);
            }
        }

        private static void EntryRemove<TEntryKey, TEntry>(TEntry entry, IDictionary<TEntryKey, TEntry> index, ISet<TEntry> expirationIndex)
            where TEntry : IExpirableEntry<TEntryKey>
        {
            index.Remove(entry.Key);

            if (entry.ExpireAt.HasValue)
            {
                expirationIndex.Remove(entry);
            }
        }

        private static bool EntryExpire<TEntryKey, TEntry>(TEntry entry, SortedSet<TEntry> index, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
            where TEntry : IExpirableEntry<TEntryKey>
        {
            if (entry.ExpireAt.HasValue)
            {
                index.Remove(entry);
            }

            if (now.HasValue && expireIn.HasValue)
            {
                if (maxExpiration.HasValue && expireIn > maxExpiration.Value)
                {
                    expireIn = maxExpiration.Value;
                }

                if (expireIn <= TimeSpan.Zero)
                {
                    entry.ExpireAt = null; // Expiration Index doesn't contain this entry
                    return true;
                }

                entry.ExpireAt = now.Value.Add(expireIn.Value);
                index.Add(entry);
                return false;
            }

            entry.ExpireAt = null;
            return false;
        }
    }
}