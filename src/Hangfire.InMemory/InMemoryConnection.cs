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
using Hangfire.InMemory.State;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryConnection<TKey> : JobStorageConnection
        where TKey : IComparable<TKey>
    {
        private readonly List<LockDisposable> _acquiredLocks = new();

        public InMemoryConnection([NotNull] InMemoryStorageOptions options, [NotNull] DispatcherBase<TKey> dispatcher, [NotNull] IKeyProvider<TKey> keyProvider)
        {
            Options = options ?? throw new ArgumentNullException(nameof(options));
            Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
            KeyProvider = keyProvider ?? throw new ArgumentNullException(nameof(keyProvider));
        }

        public InMemoryStorageOptions Options { get; }
        public DispatcherBase<TKey> Dispatcher { get; }
        public IKeyProvider<TKey> KeyProvider { get; }

        public override void Dispose()
        {
            for (var i = _acquiredLocks.Count - 1; i >= 0; i--)
            {
                _acquiredLocks[i].Dispose();
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

            if (!Dispatcher.TryAcquireLockEntry(this, resource, timeout, out var entry))
            {
                throw new DistributedLockTimeoutException(resource);
            }

            return new LockDisposable(this, resource, entry!);
        }

        public override string CreateExpiredJob(
            [NotNull] Job job,
            [NotNull] IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var key = KeyProvider.GetUniqueKey();
            var data = InvocationData.SerializeJob(job);
            var now = Dispatcher.GetMonotonicTime();

            Dispatcher.QueryWriteAndWait(new Commands.JobCreate<TKey>(key, data, parameters.ToArray(), now, expireIn));
            return KeyProvider.ToString(key);
        }

        public override IFetchedJob FetchNextJob([NotNull] string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            var entries = Dispatcher.GetOrAddQueues(queues);

            foreach (var entry in entries)
            {
                if (entry.Value.Queue.TryDequeue(out var jobId))
                {
                    entry.Value.SignalOneWaitNode();
                    return new InMemoryFetchedJob<TKey>(this, entry.Key, KeyProvider.ToString(jobId));
                }
            }

            return FetchNextJobSlow(entries, cancellationToken);
        }

        private InMemoryFetchedJob<TKey> FetchNextJobSlow(KeyValuePair<string, QueueEntry<TKey>>[] entries, CancellationToken cancellationToken)
        {
            var readyEvents = new WaitHandle[entries.Length + 1];
            var waitAdded = new bool[entries.Length];

            try
            {
                for (var i = 0; i < entries.Length; i++)
                {
                    readyEvents[i] = new AutoResetEvent(false);
                }

                readyEvents[entries.Length] = cancellationToken.WaitHandle;

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
                            entries[i].Value.AddWaitNode(new QueueWaitNode((AutoResetEvent)readyEvents[i]));
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
                throw new OperationCanceledException(cancellationToken);
            }
            finally
            {
                for (var i = 0; i < entries.Length; i++)
                {
                    readyEvents[i]?.Dispose();
                }
            }
        }

        public override void SetJobParameter([NotNull] string id, [NotNull] string name, [CanBeNull] string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            if (!KeyProvider.TryParse(id, out var key))
            {
                return;
            }

            Dispatcher.QueryWriteAndWait(new Commands.JobSetParameter<TKey>(key, name, value));
        }

        public override string? GetJobParameter([NotNull] string id, [NotNull] string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            if (!KeyProvider.TryParse(id, out var key))
            {
                return null;
            }

            return Dispatcher.QueryReadAndWait(new Queries.JobGetParameter<TKey>(key, name), static (q, s) => q.Execute(s));
        }

        public override JobData? GetJobData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!KeyProvider.TryParse(jobId, out var key))
            {
                return null;
            }

            var data = Dispatcher.QueryReadAndWait(new Queries.JobGetData<TKey>(key), static (q, s) => q.Execute(s));
            if (data == null) return null;

            return new JobData
            {
                Job = data.Value.InvocationData.TryGetJob(out var loadException),
                LoadException = loadException,
#if !HANGFIRE_170
                InvocationData = data.Value.InvocationData,
                ParametersSnapshot = data.Value.Parameters.ToDictionary(static x => x.Key, static x => x.Value, data.Value.StringComparer),
#endif
                CreatedAt = data.Value.CreatedAt.ToUtcDateTime(),
                State = data.Value.State,
            };
        }

        public override StateData? GetStateData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!KeyProvider.TryParse(jobId, out var key))
            {
                return null;
            }

            var data = Dispatcher.QueryReadAndWait(new Queries.JobGetState<TKey>(key), static (q, s) => q.Execute(s));
            if (data == null) return null;

            return new StateData
            {
                Name = data.Value.Name,
                Reason = data.Value.Reason,
                Data = data.Value.StateData.ToDictionary(static x => x.Key, static x => x.Value, data.Value.StringComparer)
            };
        }

        public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var now = Dispatcher.GetMonotonicTime();
            Dispatcher.QueryWriteAndWait(new Commands.ServerAnnounce<TKey>(serverId, context, now), static (c, s) => c.Execute(s));
        }

        public override void RemoveServer([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Dispatcher.QueryWriteAndWait(new Commands.ServerDelete<TKey>(serverId), static (c, s) => c.Execute(s));
        }

        public override void Heartbeat([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var now = Dispatcher.GetMonotonicTime();
            var result = Dispatcher.QueryWriteAndWait(new Commands.ServerHeartbeat<TKey>(serverId, now), static (c, s) => c.Execute(s));

            if (!result)
            {
                throw new BackgroundServerGoneException();
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut || timeOut == TimeSpan.Zero)
            {
                throw new ArgumentException("The `timeout` value must be positive.", nameof(timeOut));
            }

            var now = Dispatcher.GetMonotonicTime();
            return Dispatcher.QueryWriteAndWait(new Commands.ServerDeleteInactive<TKey>(timeOut, now), static (c, s) => c.Execute(s));
        }

#if !HANGFIRE_170
        public override DateTime GetUtcDateTime()
        {
            return Dispatcher.GetMonotonicTime().ToUtcDateTime();
        }
#endif

        public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new Queries.SortedSetGetAll<TKey>(key), static (q, s) => q.Execute(s));
        }

        public override string? GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be greater or equal to the `fromScore` value.", nameof(toScore));

            return Dispatcher.QueryReadAndWait(
                new Queries.SortedSetFirstByLowestScore<TKey>(key, fromScore, toScore),
                static (q, s) => q.Execute(s));
        }

        public override List<string> GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore, int count)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be greater or equal to the `fromScore` value.", nameof(toScore));
            if (count <= 0) throw new ArgumentException("The value must be a positive number", nameof(count));

            return Dispatcher.QueryReadAndWait(
                new Queries.SortedSetFirstByLowestScoreMultiple<TKey>(key, fromScore, toScore, count),
                static (q, s) => q.Execute(s));
        }

#if !HANGFIRE_170
        public override bool GetSetContains([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            return Dispatcher.QueryReadAndWait(new Queries.SortedSetContains<TKey>(key, value), static (q, s) => q.Execute(s));
        }
#endif

        public override long GetSetCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new Queries.SortedSetCount<TKey>(key), static (q, s) => q.Execute(s));
        }

#if !HANGFIRE_170
        public override long GetSetCount([NotNull] IEnumerable<string> keys, int limit)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit), "Value must be greater or equal to 0.");

            return Dispatcher.QueryReadAndWait(
                new Queries.SortedSetCountMultiple<TKey>(keys, limit),
                static (q, s) => q.Execute(s));
        }
#endif

        public override long GetListCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new Queries.ListCount<TKey>(key), static (q, s) => q.Execute(s));
        }

        public override long GetCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new Queries.CounterGet<TKey>(key), static (q, s) => q.Execute(s));
        }

        public override long GetHashCount([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new Queries.HashFieldCount<TKey>(key), static (q, s) => q.Execute(s));
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var expireAt = Dispatcher.QueryReadAndWait(new Queries.HashTimeToLive<TKey>(key), static (q, s) => q.Execute(s));

            if (!expireAt.HasValue) return Timeout.InfiniteTimeSpan;

            var now = Dispatcher.GetMonotonicTime();
            var expireIn = expireAt.Value - now;
            return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
        }

        public override TimeSpan GetListTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var expireAt = Dispatcher.QueryReadAndWait(new Queries.ListTimeToLive<TKey>(key), static (q, s) => q.Execute(s));

            if (!expireAt.HasValue) return Timeout.InfiniteTimeSpan;

            var now = Dispatcher.GetMonotonicTime();
            var expireIn = expireAt.Value - now;
            return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var expireAt = Dispatcher.QueryReadAndWait(new Queries.SortedSetTimeToLive<TKey>(key), static (q, s) => q.Execute(s));
            
            if (!expireAt.HasValue) return Timeout.InfiniteTimeSpan;

            var now = Dispatcher.GetMonotonicTime();
            var expireIn = expireAt.Value - now;
            return expireIn >= TimeSpan.Zero ? expireIn : TimeSpan.Zero;
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            Dispatcher.QueryWriteAndWait(new Commands.HashSetRange<TKey>(key, keyValuePairs));
        }

        public override Dictionary<string, string>? GetAllEntriesFromHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new Queries.HashGetAll<TKey>(key), static (q, s) => q.Execute(s));
        }

        public override string? GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Dispatcher.QueryReadAndWait(new Queries.HashGet<TKey>(key, name), static (q, s) => q.Execute(s));
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Dispatcher.QueryReadAndWait(new Queries.ListGetAll<TKey>(key), static (q, s) => q.Execute(s));
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (startingFrom < 0) throw new ArgumentException("The value must be greater than or equal to zero.", nameof(startingFrom));
            if (endingAt < startingFrom) throw new ArgumentException($"The `{nameof(endingAt)}` value must be greater than the `{nameof(startingFrom)}` value.", nameof(endingAt));

            return Dispatcher.QueryReadAndWait(new Queries.ListRange<TKey>(key, startingFrom, endingAt), static (q, s) => q.Execute(s));
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (startingFrom < 0) throw new ArgumentException("The value must be greater than or equal to zero.", nameof(startingFrom));
            if (endingAt < startingFrom) throw new ArgumentException($"The `{nameof(endingAt)}` value must be greater or equal to the `{nameof(startingFrom)}` value.", nameof(endingAt));

            return Dispatcher.QueryReadAndWait(new Queries.SortedSetRange<TKey>(key, startingFrom, endingAt), static (q, s) => q.Execute(s));
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