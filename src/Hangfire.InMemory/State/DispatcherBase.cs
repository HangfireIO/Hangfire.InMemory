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

namespace Hangfire.InMemory.State
{
    internal abstract class DispatcherBase<TKey>
        where TKey : IComparable<TKey>
    {
        private readonly Func<MonotonicTime> _timeResolver;
        private readonly MemoryState<TKey> _state;

        protected DispatcherBase(Func<MonotonicTime> timeResolver, MemoryState<TKey> state)
        {
            _timeResolver = timeResolver ?? throw new ArgumentNullException(nameof(timeResolver));
            _state = state ?? throw new ArgumentNullException(nameof(state));
        }

        protected MemoryState<TKey> State => _state;

        public MonotonicTime GetMonotonicTime()
        {
            return _timeResolver();
        }

        // Unsafe methods expose entries directly for callers, without using a
        // dispatcher thread. Consumers should ensure each data structure is
        // safe for a possible concurrent access.
        public KeyValuePair<string, QueueEntry<TKey>>[] GetOrAddQueuesUnsafe(IReadOnlyCollection<string> queueNames)
        {
            var entries = new KeyValuePair<string, QueueEntry<TKey>>[queueNames.Count];
            var index = 0;

            foreach (var queueName in queueNames)
            {
                entries[index++] = new KeyValuePair<string, QueueEntry<TKey>>(
                    queueName,
                    _state.QueueGetOrCreate(queueName));
            }

            return entries;
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

        public T QueryWriteAndWait<T>(ICommand<TKey, ValueCommand<TKey, T>> query)
            where T : struct
        {
            return QueryWriteAndWait<ValueCommand<TKey, T>>(query)?.Result ?? default;
        }

        public T QueryWriteAndWait<T>(ICommand<TKey, T> query)
            where T : class
        {
            return (T)QueryWriteAndWait(query as ICommand<TKey, object>);
        }

        protected virtual object QueryWriteAndWait(ICommand<TKey, object> query)
        {
            return query.Execute(_state);
        }
        
        public T QueryReadAndWait<T>(ICommand<TKey, ValueCommand<TKey, T>> query)
        {
            return QueryReadAndWait<ValueCommand<TKey, T>>(query).Result ?? default;
        }

        public T QueryReadAndWait<T>(ICommand<TKey, T> query)
            where T : class
        {
            return (T)QueryReadAndWait(query as ICommand<TKey, object>);
        }

        protected virtual object QueryReadAndWait(ICommand<TKey, object> query)
        {
            return QueryWriteAndWait(query);
        }

        protected void EvictExpiredEntries()
        {
            _state.EvictExpiredEntries(GetMonotonicTime());
        }
    }
}