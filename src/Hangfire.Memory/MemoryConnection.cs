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
            var tuple = _dispatcher.QueryAndWait(state =>
            {
                var acq = false;
                var ownd = false;

                if (!state._locks.TryGetValue(resource, out var lockEntry))
                {
                    state._locks.Add(resource, lockEntry = new LockEntry { Owner = this, ReferenceCount = 1, Level = 1 });
                    acq = true;
                }
                else if (lockEntry.Owner == this)
                {
                    ownd = true;
                    lockEntry.Level++;
                }
                else
                {
                    lockEntry.ReferenceCount++;
                }

                return Tuple.Create(lockEntry, acq, ownd);
            });

            if (tuple.Item2 || tuple.Item3)
            {
                return new LockDisposable(_dispatcher, this, resource, tuple.Item1);
            }

            var timeoutMs = (int)timeout.TotalMilliseconds;
            var started = Environment.TickCount;

            try
            {
                lock (tuple.Item1)
                {
                    while (tuple.Item1.Owner != null)
                    {
                        var remaining = timeoutMs - unchecked(Environment.TickCount - started);
                        if (remaining < 0)
                        {
                            throw new DistributedLockTimeoutException(resource);
                        }

                        Monitor.Wait(tuple.Item1, remaining);
                    }

                    tuple.Item1.Owner = this;
                    tuple.Item1.Level = 1;
                    return new LockDisposable(_dispatcher, this, resource, tuple.Item1);
                }
            }
            catch (DistributedLockTimeoutException)
            {
                // TODO: Can implement this as a fire-and-forget operation
                _dispatcher.QueryAndWait(state =>
                {
                    if (!state._locks.TryGetValue(resource, out var current2) ||
                        !ReferenceEquals(current2, tuple.Item1))
                    {
                        throw new InvalidOperationException("Precondition failed when decrementing a lock");
                    }

                    tuple.Item1.ReferenceCount--;

                    if (tuple.Item1.ReferenceCount == 0)
                    {
                        state._locks.Remove(resource);
                    }

                    return true;
                });

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

                // TODO: Can implement this as a fire-and-forget operation, but ensure Monitor.Pulse is called after it (inside dispatcher) in this case
                _dispatcher.QueryNoWait(state =>
                {
                    if (!state._locks.TryGetValue(_resource, out var current)) throw new InvalidOperationException("Does not contain a lock");
                    if (!ReferenceEquals(current, _entry)) throw new InvalidOperationException("Does not contain a correct lock entry");

                    lock (_entry)
                    {
                        if (!ReferenceEquals(_entry.Owner, _reference)) throw new InvalidOperationException("Wrong entry owner");
                        if (_entry.Level <= 0) throw new InvalidOperationException("Wrong level");

                        _entry.Level--;

                        if (_entry.Level == 0)
                        {
                            _entry.Owner = null;
                            _entry.ReferenceCount--;

                            if (_entry.ReferenceCount == 0)
                            {
                                state._locks.Remove(_resource);
                            }
                            else
                            {
                                Monitor.Pulse(_entry);
                            }
                        }
                    }
                });
            }
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            var backgroundJob = new BackgroundJobEntry
            {
                Key = Guid.NewGuid().ToString(), // TODO: Change with Long type
                InvocationData = InvocationData.SerializeJob(job),
                Parameters = parameters.ToDictionary(x => x.Key, x => x.Value, StringComparer.Ordinal),
                CreatedAt = createdAt,
                ExpireAt = DateTime.UtcNow.Add(expireIn) // TODO: Use time factory
            };

            // TODO: Precondition: jobId does not exist
            _dispatcher.QueryNoWait(state => state.JobCreate(backgroundJob));

            return backgroundJob.Key;
        }

        public override IFetchedJob FetchNextJob(string[] queueNames, CancellationToken cancellationToken)
        {
            using (var waiter = new ManualResetEventSlim(false))
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    // TODO: Simplify this by adding a special method to dispatcher
                    // DONE: Try to read queues on each failed fetch iteration.
                    // TODO: Ensure duplicate queue names do not fail everything
                    var queues = _dispatcher.TryGetQueues(queueNames);

                    if (queues.Count == 1)
                    {
                        var queue = queues.First();
                        var jobId = queue.Value.Take(cancellationToken);
                        return new MemoryFetchedJob(_dispatcher, queue.Key, jobId);
                    }

                    foreach (var queue in queues)
                    {
                        if (queue.Value.TryTake(out var jobId))
                        {
                            return new MemoryFetchedJob(_dispatcher, queue.Key, jobId);
                        }
                    }

                    // TODO: Improve waiting
                    waiter.Wait(TimeSpan.FromSeconds(1), cancellationToken);
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
            return _dispatcher.QueryAndWait(state =>
            {
                if (state.Jobs.TryGetValue(id, out var jobEntry) && jobEntry.Parameters.TryGetValue(name, out var result))
                {
                    return result;
                }

                return null;
            });
        }

        public override JobData GetJobData(string jobId)
        {
            var result = _dispatcher.QueryAndWait(state =>
            {
                if (!state.Jobs.TryGetValue(jobId, out var jobEntry))
                {
                    return null;
                }

                return Tuple.Create(
                    new InvocationData(jobEntry.InvocationData.Type, jobEntry.InvocationData.Method,
                        jobEntry.InvocationData.ParameterTypes, jobEntry.InvocationData.Arguments),
                    jobEntry.CreatedAt,
                    jobEntry.State?.Name);
            });

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = result.Item1.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                LoadException = loadException,
                CreatedAt = result.Item2,
                State = result.Item3
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
                    if (counter < startingFrom) continue;
                    if (counter > endingAt) break;

                    result.Add(entry.Value);

                    counter++;
                }

                return result;
            });
        }
    }
}