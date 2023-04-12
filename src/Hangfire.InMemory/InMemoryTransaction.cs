using System;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Common;
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
        }

        public override void AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            var disposableLock = _connection.AcquireDistributedLock(resource, timeout);
            _acquiredLocks.Add(disposableLock);
        }

        public override string CreateJob([NotNull] Job job, [NotNull] IDictionary<string, string> parameters, [CanBeNull] TimeSpan? expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var key = Guid.NewGuid().ToString(); // TODO: Change with Long type

            _actions.Add(state =>
            {
                var now = state.TimeResolver();
                
                // TODO: Precondition: jobId does not exist
                var backgroundJob = BackgroundJobEntry.Create(
                    key,
                    job,
                    parameters,
                    now,
                    expireIn.HasValue ? (DateTime?)now.Add(expireIn.Value) : null,
                    _connection.Options.DisableJobSerialization);

                // TODO: We need somehow to ensure that this entry isn't removed before initialization
                state.JobCreate(backgroundJob);
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

            // TODO: Add test to ensure state.Name doesn't throw inside dispatcher
            // TODO: Add test to ensure state.SerializeData doesn't throw inside dispatcher

            var stateEntry = new StateEntry
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = state.SerializeData()
            };

            _actions.Add(memory =>
            {
                if (memory.Jobs.TryGetValue(jobId, out var job))
                {
                    stateEntry.CreatedAt = memory.TimeResolver();

                    // TODO: Limit this somehow
                    job.History.Add(stateEntry);
                    memory.JobSetState(job, stateEntry);
                }
            });
        }

        public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            // TODO: Add non-null state.Name check
            // TODO: Add test to ensure state.Name doesn't throw inside dispatcher
            // TODO: Add test to ensure state.SerializeData doesn't throw inside dispatcher

            var stateEntry = new StateEntry
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = state.SerializeData()
            };

            _actions.Add(memory =>
            {
                if (memory.Jobs.TryGetValue(jobId, out var job))
                {
                    stateEntry.CreatedAt = memory.TimeResolver();
                    // TODO: Limit this somehow
                    job.History.Add(stateEntry);
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

                var result = new List<string>(); // TODO: Create only when really necessary

                for (var index = list.Count - 1; index >= 0; index--)
                {
                    if (index >= keepStartingFrom && index <= keepEndingAt)
                    {
                        result.Add(list[index]);
                    }
                }

                if (result.Count > 0)
                {
                    // TODO: Replace with better version
                    list.Update(result);
                }
                else
                {
                    state.ListDelete(list);
                }
            });
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            // TODO Return early when keyValuePairs empty, can remove comparison and deletion when empty

            _actions.Add(state =>
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

        public override void ExpireHash([NotNull]string key, TimeSpan expireIn)
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

            foreach (var queue in _enqueued)
            {
                _connection.Dispatcher.SignalOneQueueWaitNode(queue);
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