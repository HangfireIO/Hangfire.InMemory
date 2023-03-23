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
        private readonly InMemoryStorageOptions _options;

        public InMemoryConnection(
            [NotNull] InMemoryDispatcherBase dispatcher,
            [NotNull] InMemoryStorageOptions options)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new InMemoryTransaction(_dispatcher);
        }

        public override IDisposable AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            if (resource == null) throw new ArgumentNullException(nameof(resource));

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
                InvocationData = _options.DisableJobSerialization == false ? InvocationData.SerializeJob(job) : null,
                Job = _options.DisableJobSerialization ? new Job(job.Type, job.Method, job.Args.ToArray(), job.Queue) : null,
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

        public override IFetchedJob FetchNextJob([NotNull] string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                var entries = _dispatcher.GetOrAddQueues(queues).ToArray();
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
                                _dispatcher.SignalOneQueueWaitNode(entry.Value);
                                return new InMemoryFetchedJob(_dispatcher, entry.Key, jobId);
                            }
                        }

                        for (var i = 0; i < entries.Length; i++)
                        {
                            if (!waitAdded[i])
                            {
                                _dispatcher.AddQueueWaitNode(entries[i].Value, new InMemoryQueueWaitNode((AutoResetEvent)readyEvents[i]));
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

            return new JobData
            {
                Job = entry.TryGetJob(out var loadException),
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

        public override void RemoveServer([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _dispatcher.QueryAndWait(state =>
            {
                state.ServerRemove(serverId);
                return true;
            });
        }

        public override void Heartbeat([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var result = _dispatcher.QueryAndWait(state =>
            {
                if (state.Servers.TryGetValue(serverId, out var server))
                {
                    server.HeartbeatAt = state.TimeResolver();
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

            return _dispatcher.QueryAndWait(state =>
            {
                var serversToRemove = new List<string>();
                var now = state.TimeResolver();

                foreach (var server in state.Servers)
                {
                    if (now > server.Value.HeartbeatAt + timeout)
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

        public override string GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    foreach (var entry in set)
                    {
                        if (entry.Score < fromScore) continue;
                        if (entry.Score > toScore) break;

                        return entry.Value;
                    }
                }

                return null;
            });
        }

        public override List<string> GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore, int count)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _dispatcher.QueryAndWait(state =>
            {
                var result = new List<string>();

                if (state.Sets.TryGetValue(key, out var set))
                {
                    foreach (var entry in set)
                    {
                        if (entry.Score < fromScore) continue;
                        if (entry.Score > toScore) break;

                        result.Add(entry.Value);

                        if (result.Count >= count) break;
                    }
                }

                return result;
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

        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

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