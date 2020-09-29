using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Memory
{
    internal sealed class MemoryConnection : JobStorageConnection
    {
        private readonly IMemoryDispatcher _dispatcher;

        public MemoryConnection(IMemoryDispatcher dispatcher)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new MemoryTransaction(_dispatcher);
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
            private readonly IMemoryDispatcher _dispatcher;
            private readonly MemoryConnection _reference;
            private readonly string _resource;
            private readonly LockEntry _entry;
            private bool _disposed;

            public LockDisposable(IMemoryDispatcher dispatcher, MemoryConnection reference, string resource, LockEntry entry)
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

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            var backgroundJob = new BackgroundJobEntry
            {
                Key = Guid.NewGuid().ToString(), // TODO: Change with Long type
                InvocationData = InvocationData.SerializeJob(job),
                Parameters = new ConcurrentDictionary<string, string>(parameters, StringComparer.Ordinal),
                CreatedAt = createdAt,
                ExpireAt = DateTime.UtcNow.Add(expireIn) // TODO: Use time factory
            };

            // TODO: Precondition: jobId does not exist
            _dispatcher.QueryAndWait(state => state.JobCreate(backgroundJob));

            return backgroundJob.Key;
        }

        public override IFetchedJob FetchNextJob(string[] queueNames, CancellationToken cancellationToken)
        {
            using (var ready = new SemaphoreSlim(0, queueNames.Length))
            {
                var waitNode = new MemoryQueueWaitNode(ready);
                var waitAdded = false;

                while (!cancellationToken.IsCancellationRequested)
                {
                    // TODO: Ensure duplicate queue names do not fail everything
                    var entries = _dispatcher.GetOrAddQueues(queueNames);

                    foreach (var entry in entries)
                    {
                        if (entry.Value.Queue.TryDequeue(out var jobId))
                        {
                            _dispatcher.SignalOneQueueWaitNode(entry.Value);
                            return new MemoryFetchedJob(_dispatcher, entry.Key, jobId);
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

        public override void SetJobParameter(string id, string name, string value)
        {
            _dispatcher.QueryAndWait(state =>
            {
                if (state.Jobs.TryGetValue(id, out var jobEntry))
                {
                    jobEntry.Parameters[name] = value;
                }

                return true;
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            return _dispatcher.GetJobParameter(id, name);
        }

        public override JobData GetJobData(string jobId)
        {
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

        public override StateData GetStateData(string jobId)
        {
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
                    Data = jobEntry.State.Data.ToDictionary(x => x.Key, x => x.Value)
                };
            });
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            _dispatcher.QueryAndWait(state =>
            {
                // TODO: What if we add a duplicate server?
                // TODO: Change with time factory
                var now = DateTime.UtcNow;

                state.ServerAdd(serverId, new ServerEntry
                {
                    Context = new ServerContext { Queues = context.Queues.ToArray(), WorkerCount = context.WorkerCount },
                    StartedAt = now,
                    HeartbeatAt = now,
                });

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
                    server.HeartbeatAt = DateTime.UtcNow; // TODO: Use a time factory
                }

                return true;
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var serversToRemove = new List<string>();
                var now = DateTime.UtcNow; // TODO: Use a time factory

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

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                // TODO: Null or empty when doesn't exists?
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

        public override long GetSetCount(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    return set.Count;
                }

                return 0;
            });
        }

        public override long GetListCount(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Lists.TryGetValue(key, out var list))
                {
                    return list.Count;
                }

                return 0;
            });
        }

        public override long GetCounter(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Counters.TryGetValue(key, out var counter))
                {
                    return counter.Value;
                }

                return 0;
            });
        }

        public override long GetHashCount(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash))
                {
                    return hash.Value.Count;
                }

                return 0;
            });
        }

        public override TimeSpan GetHashTtl(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.ExpireAt.HasValue)
                {
                    // TODO: Change with time factory
                    return hash.ExpireAt.Value - DateTime.UtcNow;
                }

                // TODO: Really?
                return Timeout.InfiniteTimeSpan;
            });
        }


        public override TimeSpan GetListTtl(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Lists.TryGetValue(key, out var list) && list.ExpireAt.HasValue)
                {
                    // TODO: Change with time factory
                    return list.ExpireAt.Value - DateTime.UtcNow;
                }

                // TODO: Really?
                return Timeout.InfiniteTimeSpan;
            });
        }

        public override TimeSpan GetSetTtl(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Sets.TryGetValue(key, out var set) && set.ExpireAt.HasValue)
                {
                    // TODO: Change with time factory
                    return set.ExpireAt.Value - DateTime.UtcNow;
                }

                // TODO: Really?
                return Timeout.InfiniteTimeSpan;
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            _dispatcher.QueryAndWait(state =>
            {
                // TODO: return when keyValuePairs is empty

                var hash = state.HashGetOrAdd(key);

                foreach (var valuePair in keyValuePairs)
                {
                    hash.Value[valuePair.Key] = valuePair.Value;
                }

                return true;
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash))
                {
                    return hash.Value.ToDictionary(x => x.Key, x => x.Value);
                }

                // TODO: Null or empty?
                return new Dictionary<string, string>(0);
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

        public override List<string> GetAllItemsFromList(string key)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (!state.Lists.TryGetValue(key, out var list))
                {
                    // TODO: Or null?
                    return new List<string>(0);
                }

                var result = new List<string>(list.Count);
                for (var i = 0; i < list.Count; i++)
                {
                    result.Add(list[i]);
                }

                return result;
            });
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (!state.Lists.TryGetValue(key, out var list))
                {
                    // TODO: Or null?
                    return new List<string>(0);
                }

                var result = new List<string>(list.Count);

                for (var index = 0; index < list.Count; index++)
                {
                    if (index < startingFrom) continue;
                    if (index > endingAt) break;

                    result.Add(list[index]);
                }

                return result;
            });
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (!state.Sets.TryGetValue(key, out var set))
                {
                    // TODO: Or null?
                    return new List<string>(0);
                }

                var result = new List<string>(set.Count);
                var counter = 0;

                foreach (var entry in set)
                {
                    if (counter < startingFrom) { counter++; continue; }
                    if (counter > endingAt) break;

                    result.Add(entry.Value);

                    counter++;
                }

                return result;
            });
        }
    }
}