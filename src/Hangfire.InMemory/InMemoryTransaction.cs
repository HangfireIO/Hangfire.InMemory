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
    internal sealed class InMemoryTransaction : JobStorageTransaction
    {
        private readonly List<Action<InMemoryState>> _actions = new List<Action<InMemoryState>>();
        private readonly List<Action<InMemoryState>> _queueActions = new List<Action<InMemoryState>>();
        private readonly HashSet<QueueEntry> _enqueued = new HashSet<QueueEntry>();
        private readonly InMemoryConnection _connection;
        private readonly List<IDisposable> _acquiredLocks = new List<IDisposable>();

        public InMemoryTransaction([NotNull] InMemoryConnection connection)
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

        public override void AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            var disposableLock = _connection.AcquireDistributedLock(resource, timeout);
            _acquiredLocks.Add(disposableLock);
        }

        public override string CreateJob([NotNull] Job job, [NotNull] IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var key = Guid.NewGuid().ToString(); // TODO: Change with Long type

            _actions.Add(state =>
            {
                var now = state.TimeResolver();

                var entry = new JobEntry(
                    key,
                    job,
                    parameters,
                    now,
                    state.Options.DisableJobSerialization,
                    state.Options.StringComparer);

                // TODO: We need somehow to ensure that this entry isn't removed before initialization
                state.JobCreate(entry, expireIn);
            });

            return key;
        }

        public override void SetJobParameter(
            [NotNull] string id,
            [NotNull] string name,
            [CanBeNull] string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            _actions.Add(state =>
            {
                if (state.Jobs.TryGetValue(id, out var jobEntry))
                {
                    jobEntry.Parameters[name] = value;
                }
            });
        }

        public override void ExpireJob([NotNull] string jobId, TimeSpan expireIn)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            _actions.Add(state =>
            {
                if (state.Jobs.TryGetValue(jobId, out var job))
                {
                    state.JobExpire(job, expireIn);
                }
            });
        }

        public override void PersistJob([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            _actions.Add(state =>
            {
                if (state.Jobs.TryGetValue(jobId, out var job))
                {
                    state.JobExpire(job, null);
                }
            });
        }

        public override void SetJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));
            if (state.Name == null) throw new ArgumentException("Name property must not return null.", nameof(state));

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var name = state.Name;
            var reason = state.Reason;
            var data = state.SerializeData();

            _actions.Add(memory =>
            {
                if (memory.Jobs.TryGetValue(jobId, out var job))
                {
                    var stateEntry = new StateEntry(
                        name,
                        reason,
                        data,
                        memory.TimeResolver(),
                        memory.Options.StringComparer);

                    job.AddHistoryEntry(stateEntry, memory.Options.MaxStateHistoryLength);
                    memory.JobSetState(job, stateEntry);
                }
            });
        }

        public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var name = state.Name;
            var reason = state.Reason;
            var data = state.SerializeData();

            _actions.Add(memory =>
            {
                if (memory.Jobs.TryGetValue(jobId, out var job))
                {
                    var stateEntry = new StateEntry(
                        name,
                        reason,
                        data,
                        memory.TimeResolver(),
                        memory.Options.StringComparer);

                    job.AddHistoryEntry(stateEntry, memory.Options.MaxStateHistoryLength);
                }
            });
        }

        public override void AddToQueue([NotNull] string queue, [NotNull] string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            _queueActions.Add(state =>
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
            _actions.Add(state => CounterIncrement(state, key, 1, null));
        }

        public override void IncrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            _actions.Add(state => CounterIncrement(state, key, 1, expireIn));
        }

        public override void DecrementCounter([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            _actions.Add(state => CounterIncrement(state, key, -1, null));
        }

        public override void DecrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            _actions.Add(state => CounterIncrement(state, key, -1, expireIn));
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value)
        {
            AddToSet(key, value, 0.0D);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value, double score)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            _actions.Add(state => state.SetGetOrAdd(key).Add(value, score));
        }

        public override void RemoveFromSet([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            _actions.Add(state =>
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

            _actions.Add(state => state.ListGetOrAdd(key).Add(value));
        }

        public override void RemoveFromList([NotNull] string key, [NotNull] string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            _actions.Add(state =>
            {
                var list = state.ListGetOrAdd(key);
                list.RemoveAll(value);

                if (list.Count == 0) state.ListDelete(list);
            });
        }

        public override void TrimList([NotNull] string key, int keepStartingFrom, int keepEndingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _actions.Add(state =>
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

            _actions.Add(state =>
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

            _actions.Add(state =>
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

            _actions.Add(state =>
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

            _actions.Add(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetDelete(set);
            });
        }

        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _actions.Add(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, expireIn);
            });
        }

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _actions.Add(state =>
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, expireIn);
            });
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _actions.Add(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, expireIn);
            });
        }

        public override void PersistHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _actions.Add(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, null);
            });
        }

        public override void PersistList([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _actions.Add(state =>
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, null);
            });
        }

        public override void PersistSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _actions.Add(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, null);
            });
        }

        public override void Commit()
        {
            _connection.Dispatcher.QueryAndWait(state =>
            {
                try
                {
                    foreach (var action in _actions)
                    {
                        action(state);
                    }

                    // We reorder queue actions and run them after all the other commands, because
                    // our GetJobData method is being reordered too
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
            });

            // TODO: QueryAndWait can throw TimeoutException, in this case queue will be unsignaled
            foreach (var queue in _enqueued)
            {
                queue.SignalOneWaitNode();
            }
        }

        private static void CounterIncrement(InMemoryState state, string key, int value, TimeSpan? expireIn)
        {
            var counter = state.CounterGetOrAdd(key);
            counter.Value += value;

            if (counter.Value != 0)
            {
                if (expireIn.HasValue)
                {
                    state.CounterExpire(counter, expireIn);
                }
            }
            else
            {
                state.CounterDelete(counter);
            }
        }
    }
}