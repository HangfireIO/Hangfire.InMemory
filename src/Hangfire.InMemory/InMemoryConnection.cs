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
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.InMemory.Entities;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryConnection : JobStorageConnection
    {
        private readonly HashSet<IDisposable> _acquiredLocks = new HashSet<IDisposable>();

        public InMemoryConnection([NotNull] InMemoryDispatcherBase dispatcher)
        {
            Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        }

        public InMemoryDispatcherBase Dispatcher { get; }

        public override void Dispose()
        {
            if (_acquiredLocks.Count > 0)
            {
                foreach (var acquiredLock in _acquiredLocks.ToArray())
                {
                    acquiredLock.Dispose();
                }
            }

            base.Dispose();
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new InMemoryTransaction(this);
        }

        public override IDisposable AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            if (resource == null) throw new ArgumentNullException(nameof(resource));

            if (Dispatcher.TryAcquireLockEntry(this, resource, out var entry))
            {
                return new LockDisposable(this, resource, entry);
            }

            try
            {
                if (!entry.WaitUntilAcquired(this, timeout))
                {
                    throw new DistributedLockTimeoutException(resource);
                }

                return new LockDisposable(this, resource, entry);
            }
            catch (DistributedLockTimeoutException)
            {
                Dispatcher.CancelLockEntry(resource, entry);
                throw;
            }
        }

        public override string CreateExpiredJob(
            [NotNull] Job job,
            [NotNull] IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var key = Guid.NewGuid().ToString();
            var data = InvocationData.SerializeJob(job);

            Dispatcher.QueryAndWait((now, state) =>
            {
                var jobEntry = new JobEntry(
                    key,
                    data,
                    parameters,
                    now,
                    state.Options.StringComparer);

                // Background job is not yet initialized after calling this method, and
                // transaction is expected a few moments later that will initialize this
                // job. To prevent early, non-expected eviction when max expiration time
                // limit is low or close to zero, that can lead to exceptions, we just
                // ignoring this limit in very rare occasions when background job is not
                // initialized for reasons I can't even realize with an in-memory storage.
                state.JobCreate(jobEntry, now, expireIn, ignoreMaxExpirationTime: true);
            });

            return key;
        }

        public override IFetchedJob FetchNextJob([NotNull] string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                var entries = Dispatcher.GetOrAddQueuesUnsafe(queues);
                var readyEvents = new WaitHandle[entries.Length + 1];
                var waitAdded = new bool[entries.Length];

                try
                {
                    for (var i = 0; i < entries.Length; i++)
                    {
                        readyEvents[i] = new AutoResetEvent(false);
                    }

                    readyEvents[entries.Length] = cancellationEvent.WaitHandle;

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        foreach (var entry in entries)
                        {
                            if (entry.Value.Queue.TryDequeue(out var jobId))
                            {
                                entry.Value.SignalOneWaitNode();
                                return new InMemoryFetchedJob(Dispatcher, entry.Key, jobId);
                            }
                        }

                        for (var i = 0; i < entries.Length; i++)
                        {
                            if (!waitAdded[i])
                            {
                                entries[i].Value.AddWaitNode(new InMemoryQueueWaitNode((AutoResetEvent)readyEvents[i]));
                                waitAdded[i] = true;
                            }
                        }

                        var ready = WaitHandle.WaitAny(readyEvents, TimeSpan.FromSeconds(1));
                        if (ready != WaitHandle.WaitTimeout && ready < entries.Length)
                        {
                            waitAdded[ready] = false;
                        }
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    return null;
                }
                finally
                {
                    for (var i = 0; i < entries.Length; i++)
                    {
                        readyEvents[i]?.Dispose();
                    }
                }
            }
        }

        public override void SetJobParameter(
            [NotNull] string id,
            [NotNull] string name,
            [CanBeNull] string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            Dispatcher.QueryAndWait(state =>
            {
                if (state.Jobs.TryGetValue(id, out var jobEntry))
                {
                    jobEntry.Parameters[name] = value;
                }

                return true;
            });
        }

        public override string GetJobParameter([NotNull] string id, [NotNull] string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Dispatcher.QueryAndWait(state =>
            {
                if (state.Jobs.TryGetValue(id, out var entry) && entry.Parameters.TryGetValue(name, out var value))
                {
                    return value;
                }

                return null;
            });
        }

        public override JobData GetJobData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var data = Dispatcher.QueryAndWait(state =>
            {
                if (!state.Jobs.TryGetValue(jobId, out var entry))
                {
                    return null;
                }

                return new
                {
                    entry.InvocationData,
                    State = entry.State?.Name,
                    entry.CreatedAt,
                    ParametersSnapshot = entry.Parameters.ToDictionary(x => x.Key, x => x.Value, state.Options.StringComparer)
                };
            });

            if (data == null) return null;

            return new JobData
            {
                Job = data.InvocationData.TryGetJob(out var loadException),
                LoadException = loadException,
                CreatedAt = data.CreatedAt.ToUtcDateTime(),
                State = data.State,
                InvocationData = data.InvocationData,
                ParametersSnapshot = data.ParametersSnapshot
            };
        }

        public override StateData GetStateData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            return Dispatcher.QueryAndWait(state =>
            {
                if (!state.Jobs.TryGetValue(jobId, out var jobEntry) || jobEntry.State == null)
                {
                    return null;
                }

                return new StateData
                {
                    Name = jobEntry.State.Name,
                    Reason = jobEntry.State.Reason,
                    Data = jobEntry.State.GetData()
                };
            });
        }

        public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            Dispatcher.QueryAndWait((now, state) =>
            {
                if (!state.Servers.ContainsKey(serverId))
                {
                    state.ServerAdd(serverId, new ServerEntry(
                        new ServerContext { Queues = context.Queues?.ToArray(), WorkerCount = context.WorkerCount },
                        now));
                }

                return true;
            });
        }

        public override void RemoveServer([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Dispatcher.QueryAndWait(state =>
            {
                state.ServerRemove(serverId);
                return true;
            });
        }

        public override void Heartbeat([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var result = Dispatcher.QueryAndWait((now, state) =>
            {
                if (state.Servers.TryGetValue(serverId, out var server))
                {
                    server.HeartbeatAt = now;
                    return true;
                }

                return false;
            });

            if (!result)
            {
                throw new BackgroundServerGoneException();
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeout)
        {
            if (timeout.Duration() != timeout || timeout == TimeSpan.Zero)
            {
                throw new ArgumentException("The `timeout` value must be positive.", nameof(timeout));
            }

            return Dispatcher.QueryAndWait((now, state) =>
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
            });
        }

        public override DateTime GetUtcDateTime()
        {
            return Dispatcher.GetMonotonicTime().ToUtcDateTime();
        }

        public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait(state =>
            {
                var result = new HashSet<string>(state.Options.StringComparer);

                if (state.Sets.TryGetValue(key, out var set))
                {
                    foreach (var entry in set)
                    {
                        result.Add(entry.Value);
                    }
                }

                return result;
            });
        }

        public override string GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be greater or equal to the `fromScore` value.", nameof(toScore));

            return Dispatcher.QueryAndWait(state =>
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    return set.GetFirstBetween(fromScore, toScore);
                }

                return null;
            });
        }

        public override List<string> GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore, int count)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be greater or equal to the `fromScore` value.", nameof(toScore));
            if (count <= 0) throw new ArgumentException("The value must be a positive number", nameof(count));

            return Dispatcher.QueryAndWait(state =>
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    return set.GetViewBetween(fromScore, toScore, count);
                }

                return new List<string>();
            });
        }

        public override bool GetSetContains([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            return Dispatcher.QueryAndWait(state =>
            {
                return state.Sets.TryGetValue(key, out var set) && set.Contains(value);
            });
        }

        public override long GetSetCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait(
                state => state.Sets.TryGetValue(key, out var set)
                    ? set.Count
                    : 0);
        }

        public override long GetSetCount([NotNull] IEnumerable<string> keys, int limit)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit), "Value must be greater or equal to 0.");

            return Dispatcher.QueryAndWait(state =>
            {
                var count = 0;

                foreach (var key in keys)
                {
                    if (count >= limit) break;
                    count += state.Sets.TryGetValue(key, out var set)
                        ? set.Count
                        : 0;
                }

                return Math.Min(count, limit);
            });
        }

        public override long GetListCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait(
                state => state.Lists.TryGetValue(key, out var list)
                    ? list.Count
                    : 0);
        }

        public override long GetCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait(
                state => state.Counters.TryGetValue(key, out var counter)
                    ? counter.Value
                    : 0);
        }

        public override long GetHashCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait(
                state => state.Hashes.TryGetValue(key, out var hash)
                    ? hash.Value.Count 
                    : 0);
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait((now, state) =>
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.ExpireAt.HasValue)
                {
                    var expireIn = hash.ExpireAt.Value - now;
                    return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
                }

                return Timeout.InfiniteTimeSpan;
            });
        }


        public override TimeSpan GetListTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait((now, state) =>
            {
                if (state.Lists.TryGetValue(key, out var list) && list.ExpireAt.HasValue)
                {
                    var expireIn = list.ExpireAt.Value - now;
                    return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
                }

                return Timeout.InfiniteTimeSpan;
            });
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait((now, state) =>
            {
                if (state.Sets.TryGetValue(key, out var set) && set.ExpireAt.HasValue)
                {
                    var expireIn = set.ExpireAt.Value - now;
                    return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
                }

                return Timeout.InfiniteTimeSpan;
            });
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            Dispatcher.QueryAndWait(state =>
            {
                var hash = state.HashGetOrAdd(key);

                foreach (var valuePair in keyValuePairs)
                {
                    hash.Value[valuePair.Key] = valuePair.Value;
                }

                if (hash.Value.Count == 0)
                {
                    state.HashDelete(hash);
                }

                return true;
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash))
                {
                    return hash.Value.ToDictionary(x => x.Key, x => x.Value, state.Options.StringComparer);
                }

                return null;
            });
        }

        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Dispatcher.QueryAndWait(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.Value.TryGetValue(name, out var result))
                {
                    return result;
                }

                return null;
            });
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryAndWait(state =>
            {
                var result = new List<string>();

                if (state.Lists.TryGetValue(key, out var list))
                {
                    for (var i = 0; i < list.Count; i++)
                    {
                        result.Add(list[i]);
                    }
                }

                return result;
            });
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (startingFrom < 0) throw new ArgumentException("The value must be greater than or equal to zero.", nameof(startingFrom));
            if (endingAt < startingFrom) throw new ArgumentException($"The `{nameof(endingAt)}` value must be greater or equal to the `{nameof(startingFrom)}` value.", nameof(endingAt));

            return Dispatcher.QueryAndWait(state =>
            {
                var result = new List<string>();

                if (state.Lists.TryGetValue(key, out var list))
                {
                    for (var index = 0; index < list.Count; index++)
                    {
                        if (index < startingFrom) continue;
                        if (index > endingAt) break;

                        result.Add(list[index]);
                    }
                }

                return result;
            });
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (startingFrom < 0) throw new ArgumentException("The value must be greater than or equal to zero.", nameof(startingFrom));
            if (endingAt < startingFrom) throw new ArgumentException($"The `{nameof(endingAt)}` value must be greater or equal to the `{nameof(startingFrom)}` value.", nameof(endingAt));

            return Dispatcher.QueryAndWait(state =>
            {
                var result = new List<string>();

                if (state.Sets.TryGetValue(key, out var set))
                {
                    var counter = 0;

                    foreach (var entry in set)
                    {
                        if (counter < startingFrom) { counter++; continue; }
                        if (counter > endingAt) break;

                        result.Add(entry.Value);

                        counter++;
                    }
                }

                return result;
            });
        }

        private sealed class LockDisposable : IDisposable
        {
            private readonly InMemoryConnection _reference;
            private readonly string _resource;
            private readonly LockEntry<JobStorageConnection> _entry;
            private bool _disposed;

            public LockDisposable(InMemoryConnection reference, string resource, LockEntry<JobStorageConnection> entry)
            {
                _reference = reference ?? throw new ArgumentNullException(nameof(reference));
                _resource = resource ?? throw new ArgumentNullException(nameof(resource));
                _entry = entry ?? throw new ArgumentNullException(nameof(entry));
                _reference._acquiredLocks.Add(this);
            }

            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;

                try
                {
                    _reference.Dispatcher.ReleaseLockEntry(_reference, _resource, _entry);
                }
                finally
                {
                    _reference._acquiredLocks.Remove(this);
                }
            }
        }
    }
}