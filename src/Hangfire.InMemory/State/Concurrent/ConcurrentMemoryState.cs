// This file is part of Hangfire.InMemory. Copyright © 2025 Hangfire OÜ.
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
using System.Threading;
using System.Threading.Tasks;
using Hangfire.InMemory.Entities;

namespace Hangfire.InMemory.State.Concurrent
{
    internal sealed class ConcurrentMemoryState<TKey> : IMemoryState<TKey>, IDisposable
        where TKey : IComparable<TKey>
    {
        private readonly BlockingCollection<Tuple<TKey, string?, string?>> _jobStateIndexQueue = new();
        private readonly BlockingCollection<Tuple<TKey, MonotonicTime?, MonotonicTime?>> _jobExpireQueue = new();
        private readonly BlockingCollection<Tuple<string, MonotonicTime?, MonotonicTime?>> _hashExpireQueue = new();
        private readonly BlockingCollection<Tuple<string, MonotonicTime?, MonotonicTime?>> _setExpireQueue = new();
        private readonly BlockingCollection<Tuple<string, MonotonicTime?, MonotonicTime?>> _listExpireQueue = new();
        private readonly BlockingCollection<Tuple<string, MonotonicTime?, MonotonicTime?>> _counterExpireQueue = new();

        public ConcurrentMemoryState(StringComparer stringComparer, IComparer<TKey>? keyComparer)
        {
            Queues = new ConcurrentDictionary<string, QueueEntry<TKey>>(stringComparer);

            Jobs = new ConcurrentDictionary<TKey, JobEntry<TKey>>(EqualityComparer<TKey>.Default);

            Hashes = new ConcurrentDictionary<string, HashEntry>(stringComparer);
            Lists = new ConcurrentDictionary<string, ListEntry>(stringComparer);
            Sets = new ConcurrentDictionary<string, SetEntry>(stringComparer);
            Counters = new ConcurrentDictionary<string, CounterEntry>(stringComparer);
            Servers = new ConcurrentDictionary<string, ServerEntry>(stringComparer);

            ExpiringJobsIndex = new SortedSet<JobEntry<TKey>>(new ExpirableEntryComparer<TKey>(keyComparer));

            var expirableEntryComparer = new ExpirableEntryComparer<string>(stringComparer);
            ExpiringCountersIndex = new SortedSet<CounterEntry>(expirableEntryComparer);
            ExpiringHashesIndex = new SortedSet<HashEntry>(expirableEntryComparer);
            ExpiringListsIndex = new SortedSet<ListEntry>(expirableEntryComparer);
            ExpiringSetsIndex = new SortedSet<SetEntry>(expirableEntryComparer);

            StringComparer = stringComparer;
            Task.Factory.StartNew(ProcessIndexChangesLoop, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            Task.Factory.StartNew(
                ProcessExpireQueueLoop<TKey>,
                Tuple.Create(_jobExpireQueue, ExpiringJobsIndex),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            Task.Factory.StartNew(
                ProcessExpireQueueLoop<string>,
                Tuple.Create(_hashExpireQueue, ExpiringHashesIndex),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            Task.Factory.StartNew(
                ProcessExpireQueueLoop<string>,
                Tuple.Create(_setExpireQueue, ExpiringSetsIndex),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            Task.Factory.StartNew(
                ProcessExpireQueueLoop<string>,
                Tuple.Create(_listExpireQueue, ExpiringListsIndex),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            Task.Factory.StartNew(
                ProcessExpireQueueLoop<string>,
                Tuple.Create(_counterExpireQueue, ExpiringCountersIndex),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        public StringComparer StringComparer { get; }

        public ConcurrentDictionary<string, QueueEntry<TKey>> Queues { get; }

        public ConcurrentDictionary<TKey, JobEntry<TKey>> Jobs { get; }
        public ConcurrentDictionary<string, HashEntry> Hashes { get; }
        public ConcurrentDictionary<string, ListEntry> Lists { get; }
        public ConcurrentDictionary<string, SetEntry> Sets { get; }
        public ConcurrentDictionary<string, CounterEntry> Counters { get; }
        public ConcurrentDictionary<string, ServerEntry> Servers { get; }

        // State index uses case-insensitive comparisons, despite the current settings. SQL Server
        // uses case-insensitive by default, and Redis doesn't use state index that's based on user values.
        public ConcurrentDictionary<string, ConcurrentDictionary<TKey, bool>> JobStateIndex { get; } = new(StringComparer.OrdinalIgnoreCase);

        public SortedSet<JobEntry<TKey>> ExpiringJobsIndex { get; }
        public SortedSet<CounterEntry> ExpiringCountersIndex { get; }
        public SortedSet<HashEntry> ExpiringHashesIndex { get; }
        public SortedSet<ListEntry> ExpiringListsIndex { get; }
        public SortedSet<SetEntry> ExpiringSetsIndex { get; }

        public void Dispose()
        {
            _jobStateIndexQueue.Dispose();
            _jobExpireQueue.Dispose();
            _hashExpireQueue.Dispose();
            _setExpireQueue.Dispose();
            _listExpireQueue.Dispose();
            _counterExpireQueue.Dispose();
        }

        public IPagedIndex<string> QueueGetIndex()
        {
            return new CollectionPagedIndexAdapter<string>(Queues.Keys);
        }

        public bool QueueTryGet(string name, out QueueEntry<TKey> entry)
        {
            return Queues.TryGetValue(name, out entry);
        }

        public QueueEntry<TKey> QueueGetOrAdd(string name)
        {
            return Queues.GetOrAdd(name, static _ => new QueueEntry<TKey>());
        }

        public bool JobTryGet(TKey key, out JobEntry<TKey> entry)
        {
            return Jobs.TryGetValue(key, out entry);
        }

        public bool JobTryGetStateIndex(string name, out IPagedIndex<TKey> indexEntry)
        {
            if (JobStateIndex.TryGetValue(name, out var entry))
            {
                indexEntry = new ConcurrentDictionaryPagedIndexAdapter<TKey>(entry);
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
            if (EntryExpire(_jobExpireQueue, entry, entry.CreatedAt, expireIn, maxExpiration: null))
            {
                JobDelete(entry);
            }
        }

        public void JobSetState(JobEntry<TKey> entry, StateRecord state)
        {
            _jobStateIndexQueue.Add(Tuple.Create<TKey, string?, string?>(entry.Key, entry.State?.Name, state.Name));
            entry.State = state;
        }

        private static void ProcessExpireQueueLoop<T>(object state)
        {
            var stateTuple = (Tuple<BlockingCollection<Tuple<T, MonotonicTime?, MonotonicTime?>>, SortedSet<T>>)state;
            var queue = stateTuple.Item1;
            var index = stateTuple.Item2;

            // TODO: Shutdown logic
            while (true)
            {
                var tuple = queue.Take(CancellationToken.None);
                var key = tuple.Item1;
                var oldExpireAt = tuple.Item2;
                var newExpireAt = tuple.Item3;

                lock (index)
                {
                    if (oldExpireAt != null)
                    {
                        index.Remove(key);
                    }

                    if (newExpireAt != null)
                    {
                        index.Add(key);
                    }
                }
            }
        }

        private void ProcessIndexChangesLoop()
        {
            // TODO: Shutdown logic
            while (true)
            {
                var tuple = _jobStateIndexQueue.Take(CancellationToken.None);
                var jobKey = tuple.Item1;
                var oldState = tuple.Item2;
                var newState = tuple.Item3;

                if (oldState != null &&
                    JobStateIndex.TryGetValue(oldState, out var indexEntry) &&
                    indexEntry.TryRemove(jobKey, out _) &&
                    indexEntry.IsEmpty)
                {
                    JobStateIndex.TryRemove(oldState, out _);
                }

                if (newState != null)
                {
                    JobStateIndex
                        .GetOrAdd(newState, static _ => new ConcurrentDictionary<TKey, bool>())
                        .TryAdd(jobKey, true);
                }
            }
        }

        public void JobExpire(JobEntry<TKey> entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire(_jobExpireQueue, entry, now, expireIn, maxExpiration))
            {
                JobDelete(entry);
            }
        }

        public void JobDelete(JobEntry<TKey> entry)
        {
            EntryRemove(entry, Jobs, _jobExpireQueue);
            _jobStateIndexQueue.Add(Tuple.Create<TKey, string?, string?>(entry.Key, entry.State?.Name, null));
        }

        public bool HashTryGet(string key, out HashEntry entry)
        {
            return Hashes.TryGetValue(key, out entry);
        }

        public HashEntry HashGetOrAdd(string key)
        {
            return Hashes.GetOrAdd(key, new HashEntry(key, StringComparer));
        }

        public void HashExpire(HashEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire(_hashExpireQueue, entry, now, expireIn, maxExpiration))
            {
                HashDelete(entry);
            }
        }

        public void HashDelete(HashEntry entry)
        {
            EntryRemove(entry, Hashes, _hashExpireQueue);
        }

        public bool SetTryGet(string key, out SetEntry entry)
        {
            return Sets.TryGetValue(key, out entry);
        }

        public SetEntry SetGetOrAdd(string key)
        {
            return Sets.GetOrAdd(key, new SetEntry(key, StringComparer));
        }

        public void SetExpire(SetEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire(_setExpireQueue, entry, now, expireIn, maxExpiration))
            {
                SetDelete(entry);
            }
        }

        public void SetDelete(SetEntry entry)
        {
            EntryRemove(entry, Sets, _setExpireQueue);
        }

        public bool ListTryGet(string key, out ListEntry entry)
        {
            return Lists.TryGetValue(key, out entry);
        }

        public ListEntry ListGetOrAdd(string key)
        {
            return Lists.GetOrAdd(key, new ListEntry(key));
        }

        public void ListExpire(ListEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
        {
            if (EntryExpire(_listExpireQueue, entry, now, expireIn, maxExpiration))
            {
                ListDelete(entry);
            }
        }

        public void ListDelete(ListEntry entry)
        {
            EntryRemove(entry, Lists, _listExpireQueue);
        }

        public bool CounterTryGet(string key, out CounterEntry entry)
        {
            return Counters.TryGetValue(key, out entry);
        }

        public CounterEntry CounterGetOrAdd(string key)
        {
            return Counters.GetOrAdd(key, new CounterEntry(key));
        }

        public void CounterExpire(CounterEntry entry, MonotonicTime? now, TimeSpan? expireIn)
        {
            // We don't apply MaxExpirationTime rules for counters, because they
            // usually have fixed size, and because statistics should be kept for
            // days.
            if (EntryExpire(_counterExpireQueue, entry, now, expireIn, maxExpiration: null))
            {
                CounterDelete(entry);
            }
        }

        public void CounterDelete(CounterEntry entry)
        {
            EntryRemove(entry, Counters, _counterExpireQueue);
        }

        public IPagedIndex<string> ServerGetIndex()
        {
            return new CollectionPagedIndexAdapter<string>(Servers.Keys);
        }

        public bool ServerTryGet(string serverId, out ServerEntry entry)
        {
            return Servers.TryGetValue(serverId, out entry);
        }

        public bool ServerTryAdd(string serverId, ServerEntry entry)
        {
            return Servers.TryAdd(serverId, entry);
        }

        public bool ServerRemove(string serverId)
        {
            return Servers.TryRemove(serverId, out _);
        }

        public int ServerRemoveInactive(TimeSpan timeout, MonotonicTime now)
        {
            var serversToRemove = new List<string>();

            foreach (var entry in Servers)
            {
                if (now > entry.Value.HeartbeatAt.Add(timeout))
                {
                    // Adding for removal first, to avoid breaking the iterator
                    serversToRemove.Add(entry.Key);
                }
            }

            foreach (var serverId in serversToRemove)
            {
                ServerRemove(serverId);
            }

            return serversToRemove.Count;
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
            lock (index)
            {
                TEntry entry;
                while (index.Count > 0 && (entry = index.Min).ExpireAt.HasValue && now >= entry.ExpireAt)
                {
                    action(entry);
                }
            }
        }

        private static void EntryRemove<TEntryKey, TEntry>(TEntry entry, ConcurrentDictionary<TEntryKey, TEntry> index, BlockingCollection<Tuple<TEntryKey, MonotonicTime?, MonotonicTime?>> expirationQueue)
            where TEntry : IExpirableEntry<TEntryKey>
        {
            index.TryRemove(entry.Key, out _);

            if (entry.ExpireAt.HasValue)
            {
                expirationQueue.Add(Tuple.Create(entry.Key, entry.ExpireAt, (MonotonicTime?)null));
            }
        }

        private static bool EntryExpire<TEntryKey, TEntry>(BlockingCollection<Tuple<TEntryKey, MonotonicTime?, MonotonicTime?>> queue, TEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration)
            where TEntry : IExpirableEntry<TEntryKey>
        {
            MonotonicTime? oldExpireAt = null;

            if (entry.ExpireAt.HasValue)
            {
                oldExpireAt = entry.ExpireAt.Value;
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
                    if (oldExpireAt.HasValue) queue.Add(Tuple.Create(entry.Key, oldExpireAt, (MonotonicTime?)null));
                    return true;
                }

                var newExpireAt = entry.ExpireAt = now.Value.Add(expireIn.Value);
                queue.Add(Tuple.Create(entry.Key, oldExpireAt, newExpireAt));
                return false;
            }

            entry.ExpireAt = null;
            if (oldExpireAt.HasValue) queue.Add(Tuple.Create(entry.Key, oldExpireAt, (MonotonicTime?)null));
            return false;
        }
    }
}