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
using System.Collections.Generic;
using Hangfire.InMemory.Entities;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal abstract class InMemoryDispatcherBase
    {
        private readonly Func<MonotonicTime> _timeResolver;
        private readonly InMemoryState _state;

        protected InMemoryDispatcherBase(Func<MonotonicTime> timeResolver, InMemoryState state)
        {
            _timeResolver = timeResolver ?? throw new ArgumentNullException(nameof(timeResolver));
            _state = state ?? throw new ArgumentNullException(nameof(state));
        }

        public MonotonicTime GetMonotonicTime()
        {
            return _timeResolver();
        }

        // Unsafe methods expose entries directly for callers, without using a
        // dispatcher thread. Consumers should ensure each data structure is
        // safe for a possible concurrent access.
        public KeyValuePair<string, QueueEntry>[] GetOrAddQueuesUnsafe(IReadOnlyCollection<string> queueNames)
        {
            var entries = new KeyValuePair<string, QueueEntry>[queueNames.Count];
            var index = 0;

            foreach (var queueName in queueNames)
            {
                entries[index++] = new KeyValuePair<string, QueueEntry>(
                    queueName,
                    _state.QueueGetOrCreate(queueName));
            }

            return entries;
        }

        public bool TryGetJobDataUnsafe(string jobId, out JobEntry entry)
        {
            return _state.Jobs.TryGetValue(jobId, out entry);
        }

        public string GetJobParameter(string jobId, string name)
        {
            if (_state.Jobs.TryGetValue(jobId, out var jobEntry) && jobEntry.Parameters.TryGetValue(name, out var result))
            {
                return result;
            }

            return null;
        }

        public bool TryAcquireLockEntry(JobStorageConnection owner, string resource, out LockEntry<JobStorageConnection> entry)
        {
            var acquired = false;

            lock (_state.Locks)
            {
                if (!_state.Locks.TryGetValue(resource, out entry))
                {
                    _state.Locks.Add(resource, entry = new LockEntry<JobStorageConnection>(owner));
                    acquired = true;
                }
                else
                {
                    entry.TryAcquire(owner, ref acquired);
                }
            }

            return acquired;
        }

        public void CancelLockEntry(string resource, LockEntry<JobStorageConnection> entry)
        {
            lock (_state.Locks)
            {
                if (!_state.Locks.TryGetValue(resource, out var current) || !ReferenceEquals(current, entry))
                {
                    throw new InvalidOperationException("Precondition failed when decrementing a lock");
                }

                entry.Cancel();

                if (entry.Finalized)
                {
                    _state.Locks.Remove(resource);
                }
            }
        }

        public void ReleaseLockEntry(JobStorageConnection owner, string resource, LockEntry<JobStorageConnection> entry)
        {
            lock (_state.Locks)
            {
                if (!_state.Locks.TryGetValue(resource, out var current)) throw new InvalidOperationException("Does not contain a lock");
                if (!ReferenceEquals(current, entry)) throw new InvalidOperationException("Does not contain a correct lock entry");
                
                entry.Release(owner);

                if (entry.Finalized)
                {
                    _state.Locks.Remove(resource);
                }
            }
        }

        protected virtual object QueryAndWait(Func<MonotonicTime, InMemoryState, object> query)
        {
            return query(GetMonotonicTime(), _state);
        }

        public T QueryAndWait<T>(Func<MonotonicTime, InMemoryState, T> query)
        {
            object Callback(MonotonicTime now, InMemoryState state) => query(now, state);
            return (T)QueryAndWait(Callback);
        }

        public void QueryAndWait(Action<MonotonicTime, InMemoryState> query)
        {
            QueryAndWait((now, state) =>
            {
                query(now, state);
                return true;
            });
        }
        
        public T QueryAndWait<T>(Func<InMemoryState, T> query)
        {
            T Callback(MonotonicTime _, InMemoryState state) => query(state);
            return QueryAndWait(Callback);
        }

        protected void EvictEntries()
        {
            var now = GetMonotonicTime();

            EvictFromIndex(now, _state.ExpiringJobsIndex, entry => _state.JobDelete(entry));
            EvictFromIndex(now, _state.ExpiringHashesIndex, entry => _state.HashDelete(entry));
            EvictFromIndex(now, _state.ExpiringListsIndex, entry => _state.ListDelete(entry));
            EvictFromIndex(now, _state.ExpiringSetsIndex, entry => _state.SetDelete(entry));

            EvictFromIndex(now, _state.ExpiringCountersIndex, entry => _state.CounterDelete(entry));
        }

        private static void EvictFromIndex<T>(MonotonicTime now, SortedSet<T> index, Action<T> action)
            where T : IExpirableEntry
        {
            T entry;

            while (index.Count > 0 && (entry = index.Min).ExpireAt.HasValue && now >= entry.ExpireAt)
            {
                action(entry);
            }
        }
    }
}