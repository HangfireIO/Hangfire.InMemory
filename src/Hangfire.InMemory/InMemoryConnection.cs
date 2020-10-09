using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryConnection : JobStorageConnection
    {
        private readonly InMemoryDispatcherBase _dispatcher;

        public InMemoryConnection([NotNull] InMemoryDispatcherBase dispatcher)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new InMemoryTransaction(_dispatcher);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            // TODO: Track acquired lock at a connection level and release them on dispose
            if (_dispatcher.TryAcquireLockEntry(this, resource, out var entry))
            {
                return new LockDisposable(_dispatcher, this, resource, entry);
            }

            var timeoutMs = (int)timeout.TotalMilliseconds;
            var started = Environment.TickCount;

            try
            {
                lock (entry)
                {
                    while (entry.Owner != null)
                    {
                        var remaining = timeoutMs - unchecked(Environment.TickCount - started);
                        if (remaining < 0)
                        {
                            throw new DistributedLockTimeoutException(resource);
                        }

                        Monitor.Wait(entry, remaining);
                    }

                    entry.Owner = this;
                    entry.Level = 1;
                    return new LockDisposable(_dispatcher, this, resource, entry);
                }
            }
            catch (DistributedLockTimeoutException)
            {
                _dispatcher.CancelLockEntry(resource, entry);
                throw;
            }
        }

        private sealed class LockDisposable : IDisposable
        {
            private readonly InMemoryDispatcherBase _dispatcher;
            private readonly InMemoryConnection _reference;
            private readonly string _resource;
            private readonly LockEntry _entry;
            private bool _disposed;

            public LockDisposable(InMemoryDispatcherBase dispatcher, InMemoryConnection reference, string resource, LockEntry entry)
            {
                _dispatcher = dispatcher;
                _reference = reference;
                _resource = resource;
                _entry = entry ?? throw new ArgumentNullException(nameof(entry));
            }

            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;

                _dispatcher.ReleaseLockEntry(_reference, _resource, _entry);
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

            var backgroundJob = new BackgroundJobEntry
            {
                Key = Guid.NewGuid().ToString(), // TODO: Change with Long type
                InvocationData = InvocationData.SerializeJob(job),
                Parameters = new ConcurrentDictionary<string, string>(parameters, StringComparer.Ordinal), // TODO: case sensitivity
                CreatedAt = createdAt
            };

            // TODO: Precondition: jobId does not exist
            _dispatcher.QueryAndWait(state =>
            {
                // TODO: We need somehow to ensure that this entry isn't removed before initialization
                backgroundJob.ExpireAt = state.TimeResolver().Add(expireIn);
                state.JobCreate(backgroundJob);
            });

            return backgroundJob.Key;
        }

        public override IFetchedJob FetchNextJob(string[] queueNames, CancellationToken cancellationToken)
        {
            using (var ready = new SemaphoreSlim(0, queueNames.Length))
            {
                var waitNode = new InMemoryQueueWaitNode(ready);
                var waitAdded = false;

                while (!cancellationToken.IsCancellationRequested)
                {
                    // TODO: Ensure duplicate queue names do not fail everything
                    var entries = _dispatcher.GetOrAddQueues(queueNames);

                    foreach (var entry in entries)
                    {
                        if (entry.Value.Queue.TryDequeue(out var jobId))
                        {
                            // TODO: Looks like we should signal all the remaining queues as well
                            _dispatcher.SignalOneQueueWaitNode(entry.Value);
                            return new InMemoryFetchedJob(_dispatcher, entry.Key, jobId);
                        }
                    }

                    if (!waitAdded && ready.CurrentCount == 0)
                    {
                        foreach (var entry in entries)
                        {
                            _dispatcher.AddQueueWaitNode(entry.Value, waitNode);
                        }

                        waitAdded = true;
                        continue;
                    }

                    ready.Wait(cancellationToken);
                    waitAdded = false;
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            return null;
        }

        public override void SetJobParameter(
            [NotNull] string id,
            [NotNull] string name,
            [CanBeNull] string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            _dispatcher.QueryAndWait(state =>
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

            return _dispatcher.GetJobParameter(id, name);
        }

        public override JobData GetJobData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_dispatcher.TryGetJobData(jobId, out var entry))
            {
                return null;
            }

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = entry.InvocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                LoadException = loadException,
                CreatedAt = entry.CreatedAt,
                State = entry.State?.Name
            };
        }

        public override StateData GetStateData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            return _dispatcher.QueryAndWait(state =>
            {
                if (!state.Jobs.TryGetValue(jobId, out var jobEntry) || jobEntry.State == null)
                {
                    return null;
                }

                return new StateData
                {
                    Name = jobEntry.State.Name,
                    Reason = jobEntry.State.Reason,
                    // TODO Case sensitivity
                    Data = jobEntry.State.Data.ToDictionary(x => x.Key, x => x.Value)
                };
            });
        }

        public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            _dispatcher.QueryAndWait(state =>
            {
                if (!state.Servers.ContainsKey(serverId))
                {
                    var now = state.TimeResolver();

                    state.ServerAdd(serverId, new ServerEntry
                    {
                        Context = new ServerContext { Queues = context.Queues?.ToArray(), WorkerCount = context.WorkerCount },
                        StartedAt = now,
                        HeartbeatAt = now,
                    });
                }

                return true;
            });
        }

        public override void RemoveServer(string serverId)
        {
            _dispatcher.QueryAndWait(state =>
            {
                state.ServerRemove(serverId);
                return true;
            });
        }

        public override void Heartbeat(string serverId)
        {
            _dispatcher.QueryAndWait(state =>
            {
                if (state.Servers.TryGetValue(serverId, out var server))
                {
                    server.HeartbeatAt = state.TimeResolver();
                }

                return true;
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var serversToRemove = new List<string>();
                var now = state.TimeResolver();

                foreach (var server in state.Servers)
                {
                    // TODO: Check this logic
                    if (server.Value.HeartbeatAt + timeOut < now)
                    {
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

        public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(state =>
            {
                var result = new HashSet<string>();

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

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (!state.Sets.TryGetValue(key, out var set)) return null;
                foreach (var entry in set)
                {
                    if (entry.Score < fromScore) continue;
                    if (entry.Score > toScore) break;

                    return entry.Value;
                }

                return null;
            });
        }

        public override long GetSetCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(
                state => state.Sets.TryGetValue(key, out var set)
                    ? set.Count
                    : 0);
        }

        public override long GetListCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(
                state => state.Lists.TryGetValue(key, out var list)
                    ? list.Count
                    : 0);
        }

        public override long GetCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(
                state => state.Counters.TryGetValue(key, out var counter)
                    ? counter.Value
                    : 0);
        }

        public override long GetHashCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(
                state => state.Hashes.TryGetValue(key, out var hash)
                    ? hash.Value.Count 
                    : 0);
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.ExpireAt.HasValue)
                {
                    var expireIn = hash.ExpireAt.Value - state.TimeResolver();
                    return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
                }

                return Timeout.InfiniteTimeSpan;
            });
        }


        public override TimeSpan GetListTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Lists.TryGetValue(key, out var list) && list.ExpireAt.HasValue)
                {
                    var expireIn = list.ExpireAt.Value - state.TimeResolver();
                    return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
                }

                return Timeout.InfiniteTimeSpan;
            });
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Sets.TryGetValue(key, out var set) && set.ExpireAt.HasValue)
                {
                    var expireIn = set.ExpireAt.Value - state.TimeResolver();
                    return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
                }

                return Timeout.InfiniteTimeSpan;
            });
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            // TODO Return early when keyValuePairs empty, can remove comparison and deletion when empty

            _dispatcher.QueryAndWait(state =>
            {
                // TODO: Avoid creating a hash when values are empty
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

            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash))
                {
                    // TODO: Don't forget about case sensitivity
                    return hash.Value.ToDictionary(x => x.Key, x => x.Value);
                }

                return null;
            });
        }

        public override string GetValueFromHash(string key, string name)
        {
            return _dispatcher.QueryAndWait(state =>
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

            return _dispatcher.QueryAndWait(state =>
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

            return _dispatcher.QueryAndWait(state =>
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

            return _dispatcher.QueryAndWait(state =>
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
    }
}