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

namespace Hangfire.InMemory.State
{
    internal static class Commands
    {
        public sealed class JobCreate<TKey>(JobEntry<TKey> entry, TimeSpan expireIn) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                // Background job is not yet initialized after calling this method, and
                // transaction is expected a few moments later that will initialize this
                // job. To prevent early, non-expected eviction when max expiration time
                // limit is low or close to zero, that can lead to exceptions, we just
                // ignoring this limit in very rare occasions when background job is not
                // initialized for reasons I can't even realize with an in-memory storage.
                state.JobCreate(entry, expireIn, ignoreMaxExpirationTime: true);
                return null;
            }
        }

        public sealed class JobSetParameter<TKey>(TKey key, string name, string value) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var jobEntry))
                {
                    jobEntry.SetParameter(name, value, state.Options.StringComparer);
                }
                return null;
            }
        }

        public sealed class JobExpire<TKey>(TKey key, TimeSpan? expireIn, MonotonicTime? now) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var job))
                {
                    state.JobExpire(job, now, expireIn);
                }

                return null;
            }
        }

        public sealed class JobAddState<TKey>(TKey key, StateEntry entry, bool setAsCurrent) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var job))
                {
                    job.AddHistoryEntry(entry, state.Options.MaxStateHistoryLength);
                    if (setAsCurrent)
                    {
                        state.JobSetState(job, entry);
                    }
                }

                return null;
            }
        }

        public sealed class QueueEnqueue<TKey>(string queue, TKey key, HashSet<QueueEntry<TKey>> enqueued) : ICommand<TKey, QueueEntry<TKey>>
            where TKey : IComparable<TKey>
        {
            public QueueEntry<TKey> Execute(MemoryState<TKey> state)
            {
                var entry = state.QueueGetOrCreate(queue);
                entry.Queue.Enqueue(key);

                enqueued?.Add(entry);
                return entry;
            }
        }

        public sealed class CounterIncrement<TKey>(string key, long value, TimeSpan? expireIn, MonotonicTime? now) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                var counter = state.CounterGetOrAdd(key);
                counter.Value += value;

                if (counter.Value != 0)
                {
                    if (expireIn.HasValue)
                    {
                        state.CounterExpire(counter, now, expireIn);
                    }
                }
                else
                {
                    state.CounterDelete(counter);
                }

                return null;
            }
        }

        public sealed class SortedSetAdd<TKey>(string key, string value, double score) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                state.SetGetOrAdd(key).Add(value, score);
                return null;
            }
        }

        public sealed class SortedSetAddRange<TKey>(string key, IEnumerable<string> items) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                var set = state.SetGetOrAdd(key);

                foreach (var value in items)
                {
                    set.Add(value, 0.0D);
                }

                return null;
            }
        }

        public sealed class SortedSetRemove<TKey>(string key, string value) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    set.Remove(value);
                    if (set.Count == 0) state.SetDelete(set);
                }

                return null;
            }
        }

        public sealed class SortedSetDelete<TKey>(string key) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetDelete(set);
                return null;
            }
        }

        public sealed class SortedSetExpire<TKey>(string key, TimeSpan? expireIn, MonotonicTime? now) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, now, expireIn);
                return null;
            }
        }

        public sealed class ListInsert<TKey>(string key, string value) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                state.ListGetOrAdd(key).Add(value);
                return null;
            }
        }

        public sealed class ListRemoveAll<TKey>(string key, string value) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                var list = state.ListGetOrAdd(key);
                if (list.RemoveAll(value, state.Options.StringComparer) == 0)
                {
                    state.ListDelete(list);
                }

                return null;
            }
        }

        public sealed class ListTrim<TKey>(string key, int keepStartingFrom, int keepEndingAt) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Lists.TryGetValue(key, out var list))
                {
                    if (list.Trim(keepStartingFrom, keepEndingAt) == 0)
                    {
                        state.ListDelete(list);
                    }
                }

                return null;
            }
        }

        public sealed class ListExpire<TKey>(string key, TimeSpan? expireIn, MonotonicTime? now) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, now, expireIn);
                return null;
            }
        }

        public sealed class HashSetRange<TKey>(string key, IEnumerable<KeyValuePair<string, string>> items) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                var hash = state.HashGetOrAdd(key);

                foreach (var item in items)
                {
                    hash.Value[item.Key] = item.Value;
                }

                if (hash.Value.Count == 0)
                {
                    state.HashDelete(hash);
                }

                return null;
            }
        }

        public sealed class HashExpire<TKey>(string key, TimeSpan? expireIn, MonotonicTime? now) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, now, expireIn);
                return null;
            }
        }

        public sealed class HashRemove<TKey>(string key) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashDelete(hash);
                return null;
            }
        }

        public sealed class ServerAnnounce<TKey>(string serverId, ServerContext context, MonotonicTime now) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                if (!state.Servers.ContainsKey(serverId))
                {
                    state.ServerAdd(serverId, new ServerEntry(
                        new ServerContext { Queues = context.Queues?.ToArray(), WorkerCount = context.WorkerCount },
                        now));
                }

                return null;
            }
        }

        public sealed class ServerHeartbeat<TKey>(string serverId, MonotonicTime now) : ValueCommand<TKey, bool>
            where TKey : IComparable<TKey>
        {
            protected override bool Execute(MemoryState<TKey> state)
            {
                if (state.Servers.TryGetValue(serverId, out var server))
                {
                    server.HeartbeatAt = now;
                    return true;
                }

                return false;
            }
        }

        public sealed class ServerDelete<TKey>(string serverId) : ICommand<TKey>
            where TKey : IComparable<TKey>
        {
            public object Execute(MemoryState<TKey> state)
            {
                state.ServerRemove(serverId);
                return null;
            }
        }

        public sealed class ServerDeleteInactive<TKey>(TimeSpan timeout, MonotonicTime now) : ValueCommand<TKey, int>
            where TKey : IComparable<TKey>
        {
            protected override int Execute(MemoryState<TKey> state)
            {
                var serversToRemove = new List<string>();

                foreach (var server in state.Servers)
                {
                    if (now > server.Value.HeartbeatAt.Add(timeout))
                    {
                        // Adding for removal first, to avoid breaking the iterator
                        serversToRemove.Add(server.Key);
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