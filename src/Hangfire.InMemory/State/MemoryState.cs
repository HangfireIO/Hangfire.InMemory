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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Hangfire.InMemory.Entities;

namespace Hangfire.InMemory.State
{
    internal sealed class MemoryState<TKey> : IMemoryState<TKey>
        where TKey : IComparable<TKey>
    {
        public MemoryState(StringComparer stringComparer, IComparer<TKey>? keyComparer)
        {
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

        public ConcurrentDictionary<string, QueueEntry<TKey>> Queues { get; }

        public SortedDictionary<TKey, JobEntry<TKey>> Jobs { get; }
        public SortedDictionary<string, HashEntry> Hashes { get; }
        public SortedDictionary<string, ListEntry> Lists { get; }
        public SortedDictionary<string, SetEntry> Sets { get; }
        public SortedDictionary<string, CounterEntry> Counters { get; }
        public SortedDictionary<string, ServerEntry> Servers { get; }

        // State index uses case-insensitive comparisons, despite the current settings. SQL Server
        // uses case-insensitive by default, and Redis doesn't use state index that's based on user values.
        public Dictionary<string, SortedSet<TKey>> JobStateIndex { get; } = new(StringComparer.OrdinalIgnoreCase);

        public SortedSet<JobEntry<TKey>> ExpiringJobsIndex { get; }
        public SortedSet<CounterEntry> ExpiringCountersIndex { get; }
        public SortedSet<HashEntry> ExpiringHashesIndex { get; }
        public SortedSet<ListEntry> ExpiringListsIndex { get; }
        public SortedSet<SetEntry> ExpiringSetsIndex { get; }

        public IReadOnlyCollection<string> QueueGetIndex()
        {
            return new CollectionReadOnlyCollectionAdapter<string>(Queues.Keys);
        }

        public bool QueueTryGet(string name, [MaybeNullWhen(false)] out QueueEntry<TKey> entry)
        {
            return Queues.TryGetValue(name, out entry);
        }

        public QueueEntry<TKey> QueueGetOrAdd(string name)
        {
            return Queues.GetOrAdd(name, static _ => new QueueEntry<TKey>());
        }

        public bool JobTryGet(TKey key, [MaybeNullWhen(false)] out JobEntry<TKey> entry)
        {
            return Jobs.TryGetValue(key, out entry);
        }

        public bool JobTryGetStateIndex(string name, out IReadOnlyCollection<TKey> indexEntry)
        {
            if (JobStateIndex.TryGetValue(name, out var entry))
            {
#if NET451
                indexEntry = new SortedSetReadOnlyCollectionAdapter<TKey>(entry);
#else
                indexEntry = entry;
#endif
                return true;
            }

            indexEntry = null!;
            return false;
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

        public void JobSetState(JobEntry<TKey> entry, StateRecord state)
        {
            if (entry.State != null && JobStateIndex.TryGetValue(entry.State.Name, out var indexEntry))
            {
                indexEntry.Remove(entry.Key);
                if (indexEntry.Count == 0) JobStateIndex.Remove(entry.State.Name);
            }

            entry.State = state;

            if (!JobStateIndex.TryGetValue(state.Name, out indexEntry))
            {
                JobStateIndex.Add(state.Name, indexEntry = new SortedSet<TKey>());
            }

            indexEntry.Add(entry.Key);
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
                stateIndex.Remove(entry.Key);
                if (stateIndex.Count == 0) JobStateIndex.Remove(entry.State.Name);
            }
        }

        public bool HashTryGet(string key, [MaybeNullWhen(false)] out HashEntry entry)
        {
            return Hashes.TryGetValue(key, out entry);
        }

        public HashEntry HashGetOrAdd(string key)
        {
            if (!Hashes.TryGetValue(key, out var entry))
            {
                Hashes.Add(key, entry = new HashEntry(key, StringComparer));
            }

            return entry;
        }

        public void HashExpire(HashEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire<string, HashEntry>(entry, ExpiringHashesIndex, now, expireIn, maxExpiration))
            {
                HashDelete(entry);
            }
        }

        public void HashDelete(HashEntry entry)
        {
            EntryRemove(entry, Hashes, ExpiringHashesIndex);
        }

        public bool SetTryGet(string key, [MaybeNullWhen(false)] out SetEntry entry)
        {
            return Sets.TryGetValue(key, out entry);
        }

        public SetEntry SetGetOrAdd(string key)
        {
            if (!Sets.TryGetValue(key, out var entry))
            {
                Sets.Add(key, entry = new SetEntry(key, StringComparer));
            }

            return entry;
        }

        public void SetExpire(SetEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire<string, SetEntry>(entry, ExpiringSetsIndex, now, expireIn, maxExpiration))
            {
                SetDelete(entry);
            }
        }

        public void SetDelete(SetEntry entry)
        {
            EntryRemove(entry, Sets, ExpiringSetsIndex);
        }

        public bool ListTryGet(string key, [MaybeNullWhen(false)] out ListEntry entry)
        {
            return Lists.TryGetValue(key, out entry);
        }

        public ListEntry ListGetOrAdd(string key)
        {
            if (!Lists.TryGetValue(key, out var entry))
            {
                Lists.Add(key, entry = new ListEntry(key));
            }

            return entry;
        }

        public void ListExpire(ListEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire<string, ListEntry>(entry, ExpiringListsIndex, now, expireIn, maxExpiration))
            {
                ListDelete(entry);
            }
        }

        public void ListDelete(ListEntry entry)
        {
            EntryRemove(entry, Lists, ExpiringListsIndex);
        }

        public bool CounterTryGet(string key, [MaybeNullWhen(false)] out CounterEntry entry)
        {
            return Counters.TryGetValue(key, out entry);
        }

        public CounterEntry CounterGetOrAdd(string key)
        {
            if (!Counters.TryGetValue(key, out var entry))
            {
                Counters.Add(key, entry = new CounterEntry(key));
            }

            return entry;
        }

        public void CounterExpire(CounterEntry entry, MonotonicTime? now, TimeSpan? expireIn)
        {
            // We don't apply MaxExpirationTime rules for counters, because they
            // usually have fixed size, and because statistics should be kept for
            // days.
            if (EntryExpire<string, CounterEntry>(entry, ExpiringCountersIndex, now, expireIn, maxExpiration: null))
            {
                CounterDelete(entry);
            }
        }

        public void CounterDelete(CounterEntry entry)
        {
            EntryRemove(entry, Counters, ExpiringCountersIndex);
        }

        public IReadOnlyCollection<string> ServerGetIndex()
        {
#if NET451
            return new SortedDictionaryReadOnlyCollectionAdapter<string, ServerEntry>(Servers.Keys);
#else
            return Servers.Keys;
#endif
        }

        public bool ServerTryGet(string serverId, [MaybeNullWhen(false)] out ServerEntry entry)
        {
            return Servers.TryGetValue(serverId, out entry);
        }

        public bool ServerTryAdd(string serverId, ServerEntry entry)
        {
            if (Servers.ContainsKey(serverId)) return false;

            Servers.Add(serverId, entry);
            return true;
        }

        public bool ServerRemove(string serverId)
        {
            return Servers.Remove(serverId);
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
            TEntry? entry;

            while (index.Count > 0 && (entry = index.Min)?.ExpireAt != null && now >= entry.ExpireAt)
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

        private sealed class CollectionReadOnlyCollectionAdapter<T>(ICollection<T> collection) : IReadOnlyCollection<T>
        {
            public IEnumerator<T> GetEnumerator()
            {
                return collection.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public int Count => collection.Count;
        }

#if NET451
        private sealed class SortedSetReadOnlyCollectionAdapter<T>(SortedSet<T> set) : IReadOnlyCollection<T>
        {
            public SortedSet<T>.Enumerator GetEnumerator()
            {
                return set.GetEnumerator();
            }

            IEnumerator<T> IEnumerable<T>.GetEnumerator()
            {
                return GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public int Count => set.Count;
        }

        private sealed class SortedDictionaryReadOnlyCollectionAdapter<T, TValue>(SortedDictionary<T, TValue>.KeyCollection collection)
            : IReadOnlyCollection<T>
        {
            public SortedDictionary<T, TValue>.KeyCollection.Enumerator GetEnumerator()
            {
                return collection.GetEnumerator();
            }

            IEnumerator<T> IEnumerable<T>.GetEnumerator()
            {
                return GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public int Count => collection.Count;
        }
#endif
    }
}