// This file is part of Hangfire.InMemory. Copyright © 2024 Hangfire OÜ.
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
using System.Linq;
using Hangfire.InMemory.Entities;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.InMemory.State
{
    internal static class Commands<TKey> where TKey : IComparable<TKey>
    {
        public sealed class JobCreate(TKey key, InvocationData data, KeyValuePair<string, string?>[] parameters, MonotonicTime now, TimeSpan expireIn) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                state.JobCreate(new JobEntry<TKey>(key, data, parameters, now), expireIn);
            }
        }

        public sealed class JobSetParameter(TKey key, string name, string? value) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.JobTryGet(key, out var entry))
                {
                    entry.SetParameter(name, value, state.StringComparer);
                }
            }
        }

        public sealed class JobExpire(TKey key, MonotonicTime now, TimeSpan expireIn, TimeSpan? maxExpiration) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.JobTryGet(key, out var entry))
                {
                    state.JobExpire(entry, now, expireIn, maxExpiration);
                }
            }
        }

        public sealed class JobPersist(TKey key) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.JobTryGet(key, out var entry))
                {
                    state.JobExpire(entry, now: null, expireIn: null, maxExpiration: null);
                }
            }
        }

        public sealed class JobAddState(
            TKey key, string name, string? reason, KeyValuePair<string, string>[] data, MonotonicTime now, int maxHistory) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                var record = new StateRecord(name, reason, data, now);

                if (state.JobTryGet(key, out var entry))
                {
                    entry.AddHistoryEntry(record, maxHistory);
                }
            }
        }

        public sealed class JobSetState(
            TKey key, string name, string? reason, KeyValuePair<string, string>[] data, MonotonicTime now, int maxHistory) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                var record = new StateRecord(name, reason, data, now);

                if (state.JobTryGet(key, out var entry))
                {
                    entry.AddHistoryEntry(record, maxHistory);
                    state.JobSetState(entry, record);
                }
            }
        }

        public sealed class QueueEnqueue(string queue, TKey key) : ICommand<TKey>
        {
            public QueueEntry<TKey> Execute(IMemoryState<TKey> state)
            {
                var entry = state.QueueGetOrAdd(queue);
                entry.Queue.Enqueue(key);

                return entry;
            }

            void ICommand<TKey>.Execute(IMemoryState<TKey> state) => Execute(state);
        }

        public sealed class CounterIncrement(string key, long value) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                var entry = state.CounterGetOrAdd(key);
                entry.Value += value;

                if (entry.Value == 0)
                {
                    state.CounterDelete(entry);
                }
            }
        }

        public sealed class CounterIncrementAndExpire(string key, long value, MonotonicTime now, TimeSpan expireIn) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                var entry = state.CounterGetOrAdd(key);
                entry.Value += value;

                if (entry.Value != 0)
                {
                    state.CounterExpire(entry, now, expireIn);
                }
                else
                {
                    state.CounterDelete(entry);
                }
            }
        }

        public sealed class SortedSetAdd(string key, string value, double score) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                state.SetGetOrAdd(key).Add(value, score);
            }
        }

        public sealed class SortedSetAddRange(string key, IEnumerable<string> items) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                var entry = state.SetGetOrAdd(key);

                foreach (var item in items)
                {
                    entry.Add(item, 0.0D);
                }
            }
        }

        public sealed class SortedSetRemove(string key, string value) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.SetTryGet(key, out var entry))
                {
                    entry.Remove(value);
                    if (entry.Count == 0) state.SetDelete(entry);
                }
            }
        }

        public sealed class SortedSetDelete(string key) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.SetTryGet(key, out var entry)) state.SetDelete(entry);
            }
        }

        public sealed class SortedSetExpire(string key, MonotonicTime now, TimeSpan expireIn, TimeSpan? maxExpiration) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.SetTryGet(key, out var entry)) state.SetExpire(entry, now, expireIn, maxExpiration);
            }
        }

        public sealed class SortedSetPersist(string key) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.SetTryGet(key, out var entry)) state.SetExpire(entry, now: null, expireIn: null, maxExpiration: null);
            }
        }

        public sealed class ListInsert(string key, string value) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                state.ListGetOrAdd(key).Add(value);
            }
        }

        public sealed class ListRemoveAll(string key, string value) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.ListTryGet(key, out var entry))
                {
                    if (entry.RemoveAll(value, state.StringComparer) == 0)
                    {
                        state.ListDelete(entry);
                    }
                }
            }
        }

        public sealed class ListTrim(string key, int keepStartingFrom, int keepEndingAt) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.ListTryGet(key, out var entry) && entry.Trim(keepStartingFrom, keepEndingAt) == 0)
                {
                    state.ListDelete(entry);
                }
            }
        }

        public sealed class ListExpire(string key, MonotonicTime now, TimeSpan expireIn, TimeSpan? maxExpiration) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.ListTryGet(key, out var entry)) state.ListExpire(entry, now, expireIn, maxExpiration);
            }
        }

        public sealed class ListPersist(string key) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.ListTryGet(key, out var entry)) state.ListExpire(entry, now: null, expireIn: null, maxExpiration: null);
            }
        }

        public sealed class HashSetRange(string key, IEnumerable<KeyValuePair<string, string>> items) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                var entry = state.HashGetOrAdd(key);

                foreach (var item in items)
                {
                    entry.Value[item.Key] = item.Value;
                }

                if (entry.Value.Count == 0)
                {
                    state.HashDelete(entry);
                }
            }
        }

        public sealed class HashExpire(string key, MonotonicTime now, TimeSpan expireIn,  TimeSpan? maxExpiration) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.HashTryGet(key, out var entry)) state.HashExpire(entry, now, expireIn, maxExpiration);
            }
        }

        public sealed class HashPersist(string key) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.HashTryGet(key, out var entry)) state.HashExpire(entry, now: null, expireIn: null, maxExpiration: null);
            }
        }

        public sealed class HashRemove(string key) : ICommand<TKey>
        {
            public void Execute(IMemoryState<TKey> state)
            {
                if (state.HashTryGet(key, out var entry)) state.HashDelete(entry);
            }
        }

        public sealed class ServerAnnounce(string serverId, ServerContext context, MonotonicTime now)
        {
            public bool Execute(IMemoryState<TKey> state)
            {
                return state.ServerTryAdd(serverId, new ServerEntry(
                    new ServerContext { Queues = context.Queues?.ToArray(), WorkerCount = context.WorkerCount },
                    now));
            }
        }

        public sealed class ServerHeartbeat(string serverId, MonotonicTime now)
        {
            public bool Execute(IMemoryState<TKey> state)
            {
                if (state.ServerTryGet(serverId, out var entry))
                {
                    entry.HeartbeatAt = now;
                    return true;
                }

                return false;
            }
        }

        public sealed class ServerDelete(string serverId)
        {
            public bool Execute(IMemoryState<TKey> state)
            {
                return state.ServerRemove(serverId);
            }
        }

        public sealed class ServerDeleteInactive(TimeSpan timeout, MonotonicTime now)
        {
            public int Execute(IMemoryState<TKey> state)
            {
                var serversToRemove = new List<string>();
                var index = state.ServerGetIndex();

                foreach (var serverId in index)
                {
                    if (state.ServerTryGet(serverId, out var entry))
                    {
                        if (now > entry.HeartbeatAt.Add(timeout))
                        {
                            // Adding for removal first, to avoid breaking the iterator
                            serversToRemove.Add(serverId);
                        }
                    }
                }

                foreach (var serverId in serversToRemove)
                {
                    state.ServerRemove(serverId);
                }

                return serversToRemove.Count;
            }
        }
    }
}