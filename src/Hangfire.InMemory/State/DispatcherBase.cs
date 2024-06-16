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
                    _state.QueueGetOrAdd(queueName));
            }

            return entries;
        }

        public bool TryAcquireLockEntry(JobStorageConnection owner, string resource, TimeSpan timeout, out LockEntry<JobStorageConnection>? entry)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));
            if (resource == null) throw new ArgumentNullException(nameof(resource));

            var spinWait = new SpinWait();

            while (true)
            {
                entry = _state.Locks.GetOrAdd(resource, static _ => new LockEntry<JobStorageConnection>());
                if (entry.TryAcquire(owner, timeout, out var retry, out var cleanUp))
                {
                    return true;
                }

                if (cleanUp) CleanUpLockEntry(resource, entry);
                if (!retry) break;

                spinWait.SpinOnce();
            }

            entry = null;
            return false;
        }

        public void ReleaseLockEntry(JobStorageConnection owner, string resource, LockEntry<JobStorageConnection> entry)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (entry == null) throw new ArgumentNullException(nameof(entry));

            entry.Release(owner, out var cleanUp);

            if (cleanUp) CleanUpLockEntry(resource, entry);
        }

        private void CleanUpLockEntry(string resource, LockEntry<JobStorageConnection> entry)
        {
            var hasRemoved = _state.Locks.TryRemove(resource, out var removed);
            try
            {
                if (!hasRemoved)
                {
                    throw new InvalidOperationException("Wasn't able to remove a lock entry");
                }

                if (!ReferenceEquals(entry, removed))
                {
                    throw new InvalidOperationException("Removed entry isn't the same as the requested one");
                }
            }
            finally
            {
                removed?.Dispose();
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