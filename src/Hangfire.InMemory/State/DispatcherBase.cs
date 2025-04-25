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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Hangfire.InMemory.Entities;

namespace Hangfire.InMemory.State
{
    internal abstract class DispatcherBase<TKey, TLockOwner>
        where TKey : IComparable<TKey>
        where TLockOwner : class
    {
        private readonly Func<MonotonicTime> _timeResolver;

        protected DispatcherBase(Func<MonotonicTime> timeResolver, IMemoryState<TKey> state)
        {
            _timeResolver = timeResolver ?? throw new ArgumentNullException(nameof(timeResolver));
            State = state ?? throw new ArgumentNullException(nameof(state));
        }

        protected IMemoryState<TKey> State { get; }
        internal ConcurrentDictionary<string, LockEntry<TLockOwner>> Locks { get; } = new();

        public virtual T QueryWriteAndWait<TCommand, T>(TCommand query, Func<TCommand, IMemoryState<TKey>, T> func)
        {
            return func(query, State);
        }

        public virtual T QueryReadAndWait<TCommand, T>(TCommand query, Func<TCommand, IMemoryState<TKey>, T> func)
        {
            return QueryWriteAndWait(query, func);
        }

        public MonotonicTime GetMonotonicTime()
        {
            return _timeResolver();
        }

        public KeyValuePair<string, QueueEntry<TKey>>[] GetOrAddQueues(string[] queueNames)
        {
            var entries = new KeyValuePair<string, QueueEntry<TKey>>[queueNames.Length];
            var index = 0;

            foreach (var queueName in queueNames)
            {
                entries[index++] = new KeyValuePair<string, QueueEntry<TKey>>(
                    queueName,
                    State.QueueGetOrAdd(queueName));
            }

            return entries;
        }

        public bool TryAcquireLockEntry(TLockOwner owner, string resource, TimeSpan timeout, [MaybeNullWhen(false)] out LockEntry<TLockOwner> entry)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));
            if (resource == null) throw new ArgumentNullException(nameof(resource));

            var spinWait = new SpinWait();

            while (true)
            {
                entry = Locks.GetOrAdd(resource, static _ => new LockEntry<TLockOwner>());
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

        public void ReleaseLockEntry(TLockOwner owner, string resource, LockEntry<TLockOwner> entry)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (entry == null) throw new ArgumentNullException(nameof(entry));

            entry.Release(owner, out var cleanUp);

            if (cleanUp) CleanUpLockEntry(resource, entry);
        }

        private void CleanUpLockEntry(string resource, LockEntry<TLockOwner> entry)
        {
            var hasRemoved = Locks.TryRemove(resource, out var removed);

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

        protected void EvictExpiredEntries()
        {
            State.EvictExpiredEntries(GetMonotonicTime());
        }
    }
}