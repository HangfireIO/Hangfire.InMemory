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
using System.Threading;
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

        public KeyValuePair<string, QueueEntry<TKey>>[] GetOrAddQueues(IReadOnlyCollection<string> queueNames)
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

        public bool TryAcquireLockEntry(JobStorageConnection owner, string resource, TimeSpan timeout, out LockEntry<JobStorageConnection>? entry)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));
            if (resource == null) throw new ArgumentNullException(nameof(resource));

            var acquired = false;
            var spinWait = new SpinWait();
            var started = Environment.TickCount;

            entry = null;

            do
            {
                // It is possible to get a lock that has already been finalized, since there's a time gap
                // between finalization and removal from the collection to avoid additional synchronization.
                // So we'll just wait for a few moments and retry once again, since finalized locks should
                // be removed soon.
                entry = _state.Locks.GetOrAdd(resource, static _ => new LockEntry<JobStorageConnection>());
                entry.TryAcquire(owner, ref acquired, out var finalized);

                if (!finalized)
                {
                    break;
                }

                entry = null;
                spinWait.SpinOnce();
            } while (Environment.TickCount - started < timeout.TotalMilliseconds);

            return acquired;
        }

        public void CancelLockEntry(string resource, LockEntry<JobStorageConnection> entry)
        {
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (entry == null) throw new ArgumentNullException(nameof(entry));

            if (_state.Locks.TryGetValue(resource, out var current))
            {
                if (!ReferenceEquals(current, entry)) throw new InvalidOperationException("Does not contain a correct lock entry");

                entry.Cancel(out var finalized);

                if (finalized)
                {
                    // Our lock entry has finalized, we should remove the entry to clean up things.
                    if (!_state.Locks.TryRemove(resource, out var removed))
                    {
                        throw new InvalidOperationException("Wasn't able to remove a lock entry");
                    }

                    if (!ReferenceEquals(entry, removed))
                    {
                        throw new InvalidOperationException("Removed entry isn't the same as the requested one");
                    }
                }
            }
            else
            {
                throw new InvalidOperationException("Does not contain a lock");
            }
        }

        public void ReleaseLockEntry(JobStorageConnection owner, string resource, LockEntry<JobStorageConnection> entry)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (entry == null) throw new ArgumentNullException(nameof(entry));

            if (_state.Locks.TryGetValue(resource, out var current))
            {
                if (!ReferenceEquals(current, entry)) throw new InvalidOperationException("Does not contain a correct lock entry");

                entry.Release(owner, out var finalized);

                if (finalized)
                {
                    // Our lock entry has finalized, we should remove the entry to clean up things.
                    if (!_state.Locks.TryRemove(resource, out var removed))
                    {
                        throw new InvalidOperationException("Wasn't able to remove a lock entry");
                    }

                    if (!ReferenceEquals(entry, removed))
                    {
                        throw new InvalidOperationException("Removed entry isn't the same as the requested one");
                    }
                }
            }
            else
            {
                throw new InvalidOperationException("Does not contain a lock");
            }
        }

        public T? QueryWriteAndWait<T>(ICommand<TKey, ValueCommand<TKey, T?>> query)
            where T : struct
        {
            return QueryWriteAndWait<ValueCommand<TKey, T?>>(query).Result;
        }

        public T QueryWriteAndWait<T>(ICommand<TKey, ValueCommand<TKey, T>> query)
            where T : struct
        {
            return QueryWriteAndWait<ValueCommand<TKey, T>>(query).Result;
        }

        public T QueryWriteAndWait<T>(ICommand<TKey, T> query)
            where T : class?
        {
            return (T)QueryWriteAndWait(query as ICommand<TKey, object>);
        }

        protected virtual object QueryWriteAndWait(ICommand<TKey, object> query)
        {
            return query.Execute(_state);
        }

        public T? QueryReadAndWait<T>(ICommand<TKey, ValueCommand<TKey, T?>> query)
            where T : struct
        {
            return QueryReadAndWait<ValueCommand<TKey, T?>>(query).Result;
        }

        public T QueryReadAndWait<T>(ICommand<TKey, ValueCommand<TKey, T>> query)
            where T : struct
        {
            return QueryReadAndWait<ValueCommand<TKey, T>>(query).Result;
        }

        public T QueryReadAndWait<T>(ICommand<TKey, T> query)
            where T : class?
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