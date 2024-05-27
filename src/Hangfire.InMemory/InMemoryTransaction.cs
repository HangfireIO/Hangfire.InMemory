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
        private readonly LinkedList<Action<InMemoryState<TKey>>> _actions = new LinkedList<Action<InMemoryState<TKey>>>();
        private readonly LinkedList<Action<InMemoryState<TKey>>> _queueActions = new LinkedList<Action<InMemoryState<TKey>>>();
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
                    foreach (var action in _actions)
                    {
                        action(state);
                    }

                    // We reorder queue actions and run them after all the other commands, because
                    // our GetJobData method may be reordered too.
                    foreach (var action in _queueActions)
                    {
                        action(state);
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

            AddAction(state =>
            {
                state.JobCreate(entry, expireIn, ignoreMaxExpirationTime: true);
            });

            return _connection.KeyProvider.ToString(entry.Key);
        }

        public override void SetJobParameter(
            [NotNull] string id,
            [NotNull] string name,
            [CanBeNull] string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            if (!_connection.KeyProvider.TryParse(id, out var jobKey))
            {
                return;
            }

            AddAction(state =>
            {
                if (state.Jobs.TryGetValue(jobKey, out var jobEntry))
                {
                    jobEntry.SetParameter(name, value, state.Options.StringComparer);
                }
            });
        }

        public override void ExpireJob([NotNull] string jobId, TimeSpan expireIn)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_connection.KeyProvider.TryParse(jobId, out var jobKey))
            {
                return;
            }

            var now = _connection.Dispatcher.GetMonotonicTime();

            AddAction(state =>
            {
                if (state.Jobs.TryGetValue(jobKey, out var job))
                {
                    state.JobExpire(job, now, expireIn);
                }
            });
        }

        public override void PersistJob([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_connection.KeyProvider.TryParse(jobId, out var jobKey))
            {
                return;
            }

            AddAction(state =>
            {
                if (state.Jobs.TryGetValue(jobKey, out var job))
                {
                    state.JobExpire(job, now: null, expireIn: null);
                }
            });
        }

        public override void SetJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));
            if (state.Name == null) throw new ArgumentException("Name property must not return null.", nameof(state));

            if (!_connection.KeyProvider.TryParse(jobId, out var jobKey))
            {
                return;
            }

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var stateEntry = new StateEntry(
                state.Name,
                state.Reason,
                state.SerializeData(),
                _connection.Dispatcher.GetMonotonicTime());

            AddAction(memory =>
            {
                if (memory.Jobs.TryGetValue(jobKey, out var job))
                {
                    job.AddHistoryEntry(stateEntry, memory.Options.MaxStateHistoryLength);
                    memory.JobSetState(job, stateEntry);
                }
            });
        }

        public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            if (!_connection.KeyProvider.TryParse(jobId, out var jobKey))
            {
                return;
            }

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var stateEntry = new StateEntry(
                state.Name,
                state.Reason,
                state.SerializeData(),
                _connection.Dispatcher.GetMonotonicTime());

            AddAction(memory =>
            {
                if (memory.Jobs.TryGetValue(jobKey, out var job))
                {
                    job.AddHistoryEntry(stateEntry, memory.Options.MaxStateHistoryLength);
                }
            });
        }

        public override void AddToQueue([NotNull] string queue, [NotNull] string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            AddAction(state =>
            {
                var entry = state.QueueGetOrCreate(queue);
                entry.Queue.Enqueue(jobId);

                _enqueued.Add(entry);
            });
        }

        public override void RemoveFromQueue([NotNull] IFetchedJob fetchedJob)
        {
            // Nothing to do here
        }

        public override void IncrementCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            AddAction(state => CounterIncrement(state, key, value: 1, now: null, expireIn: null));
        }

        public override void IncrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddAction(state => CounterIncrement(state, key, value: 1, now, expireIn));
        }

        public override void DecrementCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            AddAction(state => CounterIncrement(state, key, value: -1, now: null, expireIn: null));
        }

        public override void DecrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddAction(state => CounterIncrement(state, key, value: -1, now, expireIn));
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value)
        {
            AddToSet(key, value, score: 0.0D);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value, double score)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddAction(state => state.SetGetOrAdd(key).Add(value, score));
        }

        public override void RemoveFromSet([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddAction(state =>
            {
                if (!state.Sets.TryGetValue(key, out var set)) return;

                set.Remove(value);

                if (set.Count == 0) state.SetDelete(set);
            });
        }

        public override void InsertToList([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddAction(state => state.ListGetOrAdd(key).Add(value));
        }

        public override void RemoveFromList([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddAction(state =>
            {
                var list = state.ListGetOrAdd(key);
                if (list.RemoveAll(value, state.Options.StringComparer) == 0)
                {
                    state.ListDelete(list);
                }
            });
        }

        public override void TrimList([NotNull] string key, int keepStartingFrom, int keepEndingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddAction(state =>
            {
                if (!state.Lists.TryGetValue(key, out var list)) return;

                if (list.Trim(keepStartingFrom, keepEndingAt) == 0)
                {
                    state.ListDelete(list);
                }
            });
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            AddAction(state =>
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
            });
        }

        public override void RemoveHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddAction(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashDelete(hash);
            });
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

            AddAction(state =>
            {
                var set = state.SetGetOrAdd(key);

                foreach (var value in items)
                {
                    set.Add(value, 0.0D);
                }
            });
        }

        public override void RemoveSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddAction(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetDelete(set);
            });
        }

        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();

            AddAction(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, now, expireIn);
            });
        }

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();

            AddAction(state =>
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, now, expireIn);
            });
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();

            AddAction(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, now, expireIn);
            });
        }

        public override void PersistHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddAction(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, now: null, expireIn: null);
            });
        }

        public override void PersistList([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddAction(state =>
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, now: null, expireIn: null);
            });
        }

        public override void PersistSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddAction(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, now: null, expireIn: null);
            });
        }

        private void AddAction(Action<InMemoryState<TKey>> action)
        {
            _actions.AddLast(action);
        }

        private static void CounterIncrement(InMemoryState<TKey> state, string key, int value, MonotonicTime? now, TimeSpan? expireIn)
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
        }
    }
}