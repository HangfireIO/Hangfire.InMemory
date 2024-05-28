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
    internal sealed class InMemoryConnection<TKey> : JobStorageConnection
        where TKey : IComparable<TKey>
    {
        private readonly HashSet<LockDisposable> _acquiredLocks = new HashSet<LockDisposable>();

        public InMemoryConnection(
            [NotNull] InMemoryDispatcherBase<TKey> dispatcher,
            [NotNull] IKeyProvider<TKey> keyProvider)
        {
            Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
            KeyProvider = keyProvider ?? throw new ArgumentNullException(nameof(keyProvider));
        }

        public InMemoryDispatcherBase<TKey> Dispatcher { get; }
        public IKeyProvider<TKey> KeyProvider { get; }

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
            return new InMemoryTransaction<TKey>(this);
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

            var jobEntry = new JobEntry<TKey>(
                KeyProvider.GetUniqueKey(),
                InvocationData.SerializeJob(job),
                parameters,
                Dispatcher.GetMonotonicTime());

            Dispatcher.QueryWriteAndWait(new InMemoryTransaction<TKey>.CreateJobCommand(jobEntry, expireIn));
            return KeyProvider.ToString(jobEntry.Key);
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
                                return new InMemoryFetchedJob<TKey>(this, entry.Key, KeyProvider.ToString(jobId));
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

            if (!KeyProvider.TryParse(id, out var key))
            {
                return;
            }

            Dispatcher.QueryWriteAndWait(new InMemoryTransaction<TKey>.SetJobParameterCommand(key, name, value));
        }

        public override string GetJobParameter([NotNull] string id, [NotNull] string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            if (!KeyProvider.TryParse(id, out var key))
            {
                return null;
            }

            return Dispatcher.QueryReadAndWait(new GetJobParameterQuery(key, name));
        }

        private sealed class GetJobParameterQuery(TKey key, string name) : InMemoryCommand<TKey, string>
        {
            protected override string Execute(InMemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var entry))
                {
                    return entry.GetParameter(name, state.Options.StringComparer);
                }

                return null;
            }
        }

        public override JobData GetJobData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!KeyProvider.TryParse(jobId, out var key))
            {
                return null;
            }

            var data = Dispatcher.QueryWriteAndWait(new GetJobDataQuery(key));
            if (data == null) return null;

            return new JobData
            {
                Job = data.InvocationData.TryGetJob(out var loadException),
                LoadException = loadException,
                CreatedAt = data.CreatedAt.ToUtcDateTime(),
                State = data.State,
                InvocationData = data.InvocationData,
                ParametersSnapshot = data.Parameters.ToDictionary(x => x.Key, x => x.Value, data.StringComparer)
            };
        }

        private sealed class GetJobDataQuery(TKey key) : InMemoryCommand<TKey, GetJobDataQuery.Data>
        {
            protected override Data Execute(InMemoryState<TKey> state)
            {
                if (!state.Jobs.TryGetValue(key, out var entry))
                {
                    return null;
                }

                return new Data
                {
                    InvocationData = entry.InvocationData,
                    State = entry.State?.Name,
                    CreatedAt = entry.CreatedAt,
                    Parameters = entry.GetParameters(),
                    StringComparer = state.Options.StringComparer
                };
            }

            internal sealed class Data
            {
                public InvocationData InvocationData { get; set; }
                public string State { get; set; }
                public MonotonicTime CreatedAt { get; set; }
                public KeyValuePair<string, string>[] Parameters { get; set; }
                public StringComparer StringComparer { get; set; }
            }
        }

        public override StateData GetStateData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!KeyProvider.TryParse(jobId, out var key))
            {
                return null;
            }

            var data = Dispatcher.QueryWriteAndWait(new GetStateDataQuery(key));
            if (data == null) return null;

            return new StateData
            {
                Name = data.Name,
                Reason = data.Reason,
                Data = data.StateData.ToDictionary(x => x.Key, x => x.Value, data.StringComparer)
            };
        }

        private sealed class GetStateDataQuery(TKey key) : InMemoryCommand<TKey, GetStateDataQuery.Data>
        {
            protected override Data Execute(InMemoryState<TKey> state)
            {
                if (!state.Jobs.TryGetValue(key, out var jobEntry) || jobEntry.State == null)
                {
                    return null;
                }

                return new Data
                {
                    Name = jobEntry.State.Name,
                    Reason = jobEntry.State.Reason,
                    StateData = jobEntry.State.Data,
                    StringComparer = state.Options.StringComparer
                };
            }
            
            internal sealed class Data
            {
                public string Name { get; set; }
                public string Reason { get; set; }
                public KeyValuePair<string, string>[] StateData { get; set; }
                public StringComparer StringComparer { get; set; }
            }
        }

        public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var now = Dispatcher.GetMonotonicTime();
            Dispatcher.QueryWriteAndWait(new AnnounceServerCommand(serverId, context, now));
        }

        private sealed class AnnounceServerCommand(string serverId, ServerContext context, MonotonicTime now) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
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

        public override void RemoveServer([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Dispatcher.QueryWriteAndWait(new RemoveServerCommand(serverId));
        }

        private sealed class RemoveServerCommand(string serverId) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                state.ServerRemove(serverId);
                return null;
            }
        }

        public override void Heartbeat([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var now = Dispatcher.GetMonotonicTime();
            var result = Dispatcher.QueryWriteAndWait(new ServerHeartbeatCommand(serverId, now));

            if (!result)
            {
                throw new BackgroundServerGoneException();
            }
        }

        private sealed class ServerHeartbeatCommand(string serverId, MonotonicTime now) : InMemoryValueCommand<TKey, bool>
        {
            protected override bool Execute(InMemoryState<TKey> state)
            {
                if (state.Servers.TryGetValue(serverId, out var server))
                {
                    server.HeartbeatAt = now;
                    return true;
                }

                return false;
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeout)
        {
            if (timeout.Duration() != timeout || timeout == TimeSpan.Zero)
            {
                throw new ArgumentException("The `timeout` value must be positive.", nameof(timeout));
            }

            var now = Dispatcher.GetMonotonicTime();
            return Dispatcher.QueryWriteAndWait(new RemoveInactiveServersCommand(timeout, now));
        }

        private sealed class RemoveInactiveServersCommand(TimeSpan timeout, MonotonicTime now) : InMemoryValueCommand<TKey, int>
        {
            protected override int Execute(InMemoryState<TKey> state)
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

        public override DateTime GetUtcDateTime()
        {
            return Dispatcher.GetMonotonicTime().ToUtcDateTime();
        }

        public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new GetAllItemsFromSetQuery(key));
        }

        private sealed class GetAllItemsFromSetQuery(string key) : InMemoryCommand<TKey, HashSet<string>>
        {
            protected override HashSet<string> Execute(InMemoryState<TKey> state)
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
            }
        }

        public override string GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be greater or equal to the `fromScore` value.", nameof(toScore));

            return Dispatcher.QueryReadAndWait(new GetFirstByLowestScoreFromSetSingleQuery(key, fromScore, toScore));
        }

        private sealed class GetFirstByLowestScoreFromSetSingleQuery(string key, double fromScore, double toScore)
            : InMemoryCommand<TKey, string>
        {
            protected override string Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    return set.GetFirstBetween(fromScore, toScore);
                }

                return null;
            }
        }

        public override List<string> GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore, int count)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be greater or equal to the `fromScore` value.", nameof(toScore));
            if (count <= 0) throw new ArgumentException("The value must be a positive number", nameof(count));

            return Dispatcher.QueryReadAndWait(new GetFirstByLowestScoreFromSetMultipleQuery(key, fromScore, toScore, count));
        }

        private sealed class GetFirstByLowestScoreFromSetMultipleQuery(string key, double fromScore, double toScore, int count) 
            : InMemoryCommand<TKey, List<string>>
        {
            protected override List<string> Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    return set.GetViewBetween(fromScore, toScore, count);
                }

                return new List<string>();
            }
        }

        public override bool GetSetContains([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            return Dispatcher.QueryReadAndWait(new GetSetContainsQuery(key, value));
        }

        private sealed class GetSetContainsQuery(string key, string value) : InMemoryValueCommand<TKey, bool>
        {
            protected override bool Execute(InMemoryState<TKey> state)
            {
                return state.Sets.TryGetValue(key, out var set) && set.Contains(value);
            }
        }

        public override long GetSetCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new GetSetCountQuery(key));
        }

        private sealed class GetSetCountQuery(string key) : InMemoryValueCommand<TKey, int>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                return state.Sets.TryGetValue(key, out var set) ? set.Count : 0;
            }
        }

        public override long GetSetCount([NotNull] IEnumerable<string> keys, int limit)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit), "Value must be greater or equal to 0.");

            return Dispatcher.QueryReadAndWait(new GetMultipleSetCountQuery(keys, limit));
        }

        private sealed class GetMultipleSetCountQuery(IEnumerable<string> keys, int limit) : InMemoryValueCommand<TKey, int>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                var count = 0;

                foreach (var key in keys)
                {
                    if (count >= limit) break;
                    count += state.Sets.TryGetValue(key, out var set) ? set.Count : 0;
                }

                return Math.Min(count, limit);
            }
        }

        public override long GetListCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new GetListCountQuery(key));
        }

        private sealed class GetListCountQuery(string key) : InMemoryValueCommand<TKey, int>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                return state.Lists.TryGetValue(key, out var list) ? list.Count : 0;
            }
        }

        public override long GetCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new GetCounterValueQuery(key));
        }

        private sealed class GetCounterValueQuery(string key) : InMemoryValueCommand<TKey, long>
        {
            protected override long Execute(InMemoryState<TKey> state)
            {
                return state.Counters.TryGetValue(key, out var counter) ? counter.Value : 0;
            }
        }

        public override long GetHashCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new GetHashFieldCountQuery(key));
        }

        private sealed class GetHashFieldCountQuery(string key) : InMemoryValueCommand<TKey, int>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                return state.Hashes.TryGetValue(key, out var hash) ? hash.Value.Count : 0;
            }
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var expireAt = Dispatcher.QueryReadAndWait(new GetHashTimeToLiveQuery(key));

            if (!expireAt.HasValue) return Timeout.InfiniteTimeSpan;

            var now = Dispatcher.GetMonotonicTime();
            var expireIn = expireAt.Value - now;
            return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
        }

        private sealed class GetHashTimeToLiveQuery(string key) : InMemoryValueCommand<TKey, MonotonicTime?>
        {
            protected override MonotonicTime? Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.ExpireAt.HasValue)
                {
                    return hash.ExpireAt;
                }

                return null;
            }
        }

        public override TimeSpan GetListTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var expireAt = Dispatcher.QueryReadAndWait(new GetListTimeToLiveQuery(key));

            if (!expireAt.HasValue) return Timeout.InfiniteTimeSpan;

            var now = Dispatcher.GetMonotonicTime();
            var expireIn = expireAt.Value - now;
            return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
        }

        private sealed class GetListTimeToLiveQuery(string key) : InMemoryValueCommand<TKey, MonotonicTime?>
        {
            protected override MonotonicTime? Execute(InMemoryState<TKey> state)
            {
                if (state.Lists.TryGetValue(key, out var list) && list.ExpireAt.HasValue)
                {
                    return list.ExpireAt;
                }

                return null;
            }
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var expireAt = Dispatcher.QueryReadAndWait(new GetSetTimeToLiveQuery(key));
            
            if (!expireAt.HasValue) return Timeout.InfiniteTimeSpan;

            var now = Dispatcher.GetMonotonicTime();
            var expireIn = expireAt.Value - now;
            return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
        }

        private sealed class GetSetTimeToLiveQuery(string key) : InMemoryValueCommand<TKey, MonotonicTime?>
        {
            protected override MonotonicTime? Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set) && set.ExpireAt.HasValue)
                {
                    return set.ExpireAt;
                }

                return null;
            }
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            Dispatcher.QueryWriteAndWait(new InMemoryTransaction<TKey>.SetRangeInHashCommand(key, keyValuePairs));
        }

        public override Dictionary<string, string> GetAllEntriesFromHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new GetAllEntriesFromHashQuery(key));
        }

        private sealed class GetAllEntriesFromHashQuery(string key) : IInMemoryCommand<TKey, Dictionary<string, string>>
        {
            public Dictionary<string, string> Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash))
                {
                    return hash.Value.ToDictionary(x => x.Key, x => x.Value, state.Options.StringComparer);
                }

                return null;
            }
        }

        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Dispatcher.QueryReadAndWait(new GetValueFromHashQuery(key, name));
        }

        private sealed class GetValueFromHashQuery(string key, string name) : IInMemoryCommand<TKey, string>
        {
            public string Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.Value.TryGetValue(name, out var result))
                {
                    return result;
                }

                return null;
            }
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new GetAllItemsFromListQuery(key));
        }

        private sealed class GetAllItemsFromListQuery(string key) : IInMemoryCommand<TKey, List<string>>
        {
            public List<string> Execute(InMemoryState<TKey> state)
            {
                if (state.Lists.TryGetValue(key, out var list))
                {
                    return new List<string>(list);
                }

                return new List<string>();
            }
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (startingFrom < 0) throw new ArgumentException("The value must be greater than or equal to zero.", nameof(startingFrom));
            if (endingAt < startingFrom) throw new ArgumentException($"The `{nameof(endingAt)}` value must be greater than the `{nameof(startingFrom)}` value.", nameof(endingAt));

            return Dispatcher.QueryReadAndWait(new GetRangeFromListQuery(key, startingFrom, endingAt));
        }

        private sealed class GetRangeFromListQuery(string key, int startingFrom, int endingAt) : IInMemoryCommand<TKey, List<string>>
        {
            public List<string> Execute(InMemoryState<TKey> state)
            {
                var result = new List<string>();

                if (state.Lists.TryGetValue(key, out var list))
                {
                    var count = endingAt - startingFrom + 1;
                    foreach (var item in list)
                    {
                        if (startingFrom-- > 0) continue;
                        if (count-- == 0) break;

                        result.Add(item);
                    }
                }

                return result;
            }
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (startingFrom < 0) throw new ArgumentException("The value must be greater than or equal to zero.", nameof(startingFrom));
            if (endingAt < startingFrom) throw new ArgumentException($"The `{nameof(endingAt)}` value must be greater or equal to the `{nameof(startingFrom)}` value.", nameof(endingAt));

            return Dispatcher.QueryReadAndWait(new GetRangeFromSetQuery(key, startingFrom, endingAt));
        }

        private sealed class GetRangeFromSetQuery(string key, int startingFrom, int endingAt) : IInMemoryCommand<TKey, List<string>>
        {
            public List<string> Execute(InMemoryState<TKey> state)
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
            }
        }

        private sealed class LockDisposable : IDisposable
        {
            private readonly InMemoryConnection<TKey> _reference;
            private readonly string _resource;
            private readonly LockEntry<JobStorageConnection> _entry;
            private bool _disposed;

            public LockDisposable(InMemoryConnection<TKey> reference, string resource, LockEntry<JobStorageConnection> entry)
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