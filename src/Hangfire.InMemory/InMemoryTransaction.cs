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
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.InMemory.Entities;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryTransaction<TKey> : JobStorageTransaction
        where TKey : IComparable<TKey>
    {
        private readonly LinkedList<IInMemoryCommand<TKey>> _commands = new LinkedList<IInMemoryCommand<TKey>>();
        private readonly HashSet<QueueEntry> _enqueued = new HashSet<QueueEntry>();
        private readonly InMemoryConnection<TKey> _connection;
        private readonly List<IDisposable> _acquiredLocks = new List<IDisposable>();

        public InMemoryTransaction([NotNull] InMemoryConnection<TKey> connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public override void Dispose()
        {
            foreach (var acquiredLock in _acquiredLocks)
            {
                acquiredLock.Dispose();
            }

            base.Dispose();
        }

        public override void Commit()
        {
            _connection.Dispatcher.QueryWriteAndWait(state =>
            {
                try
                {
                    foreach (var command in _commands)
                    {
                        command.Execute(state);
                    }
                }
                finally
                {
                    foreach (var acquiredLock in _acquiredLocks)
                    {
                        acquiredLock.Dispose();
                    }
                }

                foreach (var queue in _enqueued)
                {
                    queue.SignalOneWaitNode();
                }
            });
        }

        public override void AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            var disposableLock = _connection.AcquireDistributedLock(resource, timeout);
            _acquiredLocks.Add(disposableLock);
        }

        public override string CreateJob([NotNull] Job job, [NotNull] IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var entry = new JobEntry<TKey>(
                _connection.KeyProvider.GetUniqueKey(),
                InvocationData.SerializeJob(job),
                parameters,
                _connection.Dispatcher.GetMonotonicTime());

            AddCommand(new CreateJobCommand(entry, expireIn));
            return _connection.KeyProvider.ToString(entry.Key);
        }

        private sealed class CreateJobCommand(JobEntry<TKey> entry, TimeSpan? expireIn) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                state.JobCreate(entry, expireIn, ignoreMaxExpirationTime: true);
                return null;
            }
        }

        public override void SetJobParameter(
            [NotNull] string id,
            [NotNull] string name,
            [CanBeNull] string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            if (!_connection.KeyProvider.TryParse(id, out var key))
            {
                return;
            }

            AddCommand(new SetJobParameterCommand(key, name, value));
        }

        private sealed class SetJobParameterCommand(TKey key, string name, string value) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var jobEntry))
                {
                    jobEntry.SetParameter(name, value, state.Options.StringComparer);
                }
                return null;
            }
        }

        public override void ExpireJob([NotNull] string jobId, TimeSpan expireIn)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new ExpireJobCommand(key, expireIn, now));
        }

        private sealed class ExpireJobCommand(TKey key, TimeSpan expireIn, MonotonicTime now) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var job))
                {
                    state.JobExpire(job, now, expireIn);
                }

                return null;
            }
        }

        public override void PersistJob([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            AddCommand(new PersistJobCommand(key));
        }

        private sealed class PersistJobCommand(TKey key) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var job))
                {
                    state.JobExpire(job, now: null, expireIn: null);
                }

                return null;
            }
        }

        public override void SetJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));
            if (state.Name == null) throw new ArgumentException("Name property must not return null.", nameof(state));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var entry = new StateEntry(
                state.Name,
                state.Reason,
                state.SerializeData(),
                _connection.Dispatcher.GetMonotonicTime());

            AddCommand(new SetJobStateCommand(key, entry));
        }

        private sealed class SetJobStateCommand(TKey key, StateEntry entry) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var job))
                {
                    job.AddHistoryEntry(entry, state.Options.MaxStateHistoryLength);
                    state.JobSetState(job, entry);
                }

                return null;
            }
        }

        public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var entry = new StateEntry(
                state.Name,
                state.Reason,
                state.SerializeData(),
                _connection.Dispatcher.GetMonotonicTime());

            AddCommand(new AddJobStateCommand(key, entry));
        }

        private sealed class AddJobStateCommand(TKey key, StateEntry entry) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Jobs.TryGetValue(key, out var job))
                {
                    job.AddHistoryEntry(entry, state.Options.MaxStateHistoryLength);
                }

                return null;
            }
        }

        public override void AddToQueue([NotNull] string queue, [NotNull] string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            AddCommand(new AddToQueueCommand(queue, jobId, _enqueued));
        }

        private sealed class AddToQueueCommand(string queue, string jobId, HashSet<QueueEntry> enqueued)
            : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                var entry = state.QueueGetOrCreate(queue);
                entry.Queue.Enqueue(jobId);

                enqueued.Add(entry);
                return null;
            }
        }

        public override void RemoveFromQueue([NotNull] IFetchedJob fetchedJob)
        {
            // Nothing to do here
        }

        public override void IncrementCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            AddCommand(new CounterIncrementCommand(key, value: 1, expireIn: null, now: null));
        }

        public override void IncrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new CounterIncrementCommand(key, value: 1, expireIn, now));
        }

        public override void DecrementCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            AddCommand(new CounterIncrementCommand(key, value: -1, expireIn: null, now: null));
        }

        public override void DecrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new CounterIncrementCommand(key, value: -1, expireIn, now));
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value)
        {
            AddToSet(key, value, score: 0.0D);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value, double score)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new AddToSetCommand(key, value, score));
        }

        private sealed class AddToSetCommand(string key, string value, double score) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                state.SetGetOrAdd(key).Add(value, score);
                return null;
            }
        }

        public override void RemoveFromSet([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new RemoveFromSetCommand(key, value));
        }

        private sealed class RemoveFromSetCommand(string key, string value) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    set.Remove(value);
                    if (set.Count == 0) state.SetDelete(set);
                }

                return null;
            }
        }

        public override void InsertToList([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new InsertToListCommand(key, value));
        }

        private sealed class InsertToListCommand(string key, string value) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                state.ListGetOrAdd(key).Add(value);
                return null;
            }
        }

        public override void RemoveFromList([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new RemoveFromListCommand(key, value));
        }

        private sealed class RemoveFromListCommand(string key, string value) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                var list = state.ListGetOrAdd(key);
                if (list.RemoveAll(value, state.Options.StringComparer) == 0)
                {
                    state.ListDelete(list);
                }

                return null;
            }
        }

        public override void TrimList([NotNull] string key, int keepStartingFrom, int keepEndingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new TrimListCommand(key, keepStartingFrom, keepEndingAt));
        }

        private sealed class TrimListCommand(string key, int keepStartingFrom, int keepEndingAt)
            : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
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

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            AddCommand(new SetRangeInHashCommand(key, keyValuePairs));
        }

        private sealed class SetRangeInHashCommand(string key, IEnumerable<KeyValuePair<string, string>> items)
            : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
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

        public override void RemoveHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new RemoveHashCommand(key));
        }

        private sealed class RemoveHashCommand(string key) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashDelete(hash);
                return null;
            }
        }

        public override void AddRangeToSet([NotNull] string key, [NotNull] IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            foreach (var item in items)
            {
                if (item == null) throw new ArgumentException("The list of items must not contain any `null` entries.", nameof(items));
            }

            if (items.Count == 0) return;

            AddCommand(new AddRangeToSetCommand(key, items));
        }

        private sealed class AddRangeToSetCommand(string key, IEnumerable<string> items) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                var set = state.SetGetOrAdd(key);

                foreach (var value in items)
                {
                    set.Add(value, 0.0D);
                }

                return null;
            }
        }

        public override void RemoveSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new RemoveSetCommand(key));
        }

        private sealed class RemoveSetCommand(string key) : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetDelete(set);
                return null;
            }
        }

        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new ExpireHashCommand(key, expireIn, now));
        }

        private sealed class ExpireHashCommand(string key, TimeSpan? expireIn, MonotonicTime? now)
            : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, now, expireIn);
                return null;
            }
        }

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new ExpireListCommand(key, expireIn, now));
        }

        private sealed class ExpireListCommand(string key, TimeSpan? expireIn, MonotonicTime? now)
            : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, now, expireIn);
                return null;
            }
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new ExpireSetCommand(key, expireIn, now));
        }

        private sealed class ExpireSetCommand(string key, TimeSpan? expireIn, MonotonicTime? now)
            : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, now, expireIn);
                return null;
            }
        }

        public override void PersistHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new ExpireHashCommand(key, expireIn: null, now: null));
        }

        public override void PersistList([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new ExpireListCommand(key, expireIn: null, now: null));
        }

        public override void PersistSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new ExpireSetCommand(key, expireIn: null, now: null));
        }

        private void AddCommand(IInMemoryCommand<TKey> action)
        {
            _commands.AddLast(action);
        }

        private sealed class CounterIncrementCommand(string key, long value, TimeSpan? expireIn, MonotonicTime? now)
            : IInMemoryCommand<TKey>
        {
            public object Execute(InMemoryState<TKey> state)
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
    }
}