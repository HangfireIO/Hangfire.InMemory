using System;
using System.Collections.Generic;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Memory
{
    internal sealed class MemoryTransaction : JobStorageTransaction
    {
        private readonly List<Action<MemoryState>> _actions = new List<Action<MemoryState>>();
        private readonly HashSet<string> _enqueued = new HashSet<string>();
        private readonly IMemoryDispatcher _dispatcher;

        public MemoryTransaction(IMemoryDispatcher dispatcher)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            _actions.Add(state =>
            {
                if (state.Jobs.TryGetValue(jobId, out var job)) state.JobExpire(job, expireIn);
            });
        }

        public override void PersistJob(string jobId)
        {
            _actions.Add(state =>
            {
                if (state.Jobs.TryGetValue(jobId, out var job)) state.JobExpire(job, null);
            });
        }

        public override void SetJobState(string jobId, IState state)
        {
            var stateEntry = new StateEntry
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = state.SerializeData(),
                CreatedAt = DateTime.UtcNow // TODO Replace with time factory
            };

            // TODO: Precondition: jobId exists
            _actions.Add(memory =>
            {
                var backgroundJob = memory.JobGetOrThrow(jobId);
                backgroundJob.History.Add(stateEntry);

                memory.JobSetState(backgroundJob, stateEntry);
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            var stateEntry = new StateEntry
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = state.SerializeData(),
                CreatedAt = DateTime.UtcNow // TODO Replace with time factory
            };

            _actions.Add(memory =>
            {
                // TODO: Precondition: jobId exists
                var backgroundJob = memory.JobGetOrThrow(jobId);
                backgroundJob.History.Add(stateEntry);
            });
        }

        public override void AddToQueue(string queue, string jobId)
        {
            _actions.Add(state => state.QueueGetOrCreate(queue).Enqueue(jobId));
            _enqueued.Add(queue);
        }

        public override void IncrementCounter(string key)
        {
            _actions.Add(state => CounterIncrement(state, key, 1, null));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            _actions.Add(state => CounterIncrement(state, key, 1, expireIn));
        }

        public override void DecrementCounter(string key)
        {
            _actions.Add(state => CounterIncrement(state, key, -1, null));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            _actions.Add(state => CounterIncrement(state, key, -1, expireIn));
        }

        public override void AddToSet(string key, string value)
        {
            _actions.Add(state =>
            {
                var set = state.SetGetOrAdd(key);
                // TODO: What about null values?
                set.Add(value, 0.0D);
            });
        }

        public override void AddToSet(string key, string value, double score)
        {
            _actions.Add(state =>
            {
                var set = state.SetGetOrAdd(key);
                // TODO: What about null values?
                set.Add(value, score);
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            _actions.Add(state =>
            {
                if (!state.Sets.TryGetValue(key, out var set)) return;

                set.Remove(value);

                if (set.Count == 0) state.SetDelete(set);
            });
        }

        public override void InsertToList(string key, string value)
        {
            _actions.Add(state =>
            {
                // TODO: Possible that value is null?
                var list = state.ListGetOrAdd(key);
                list.Add(value);
            });
        }

        public override void RemoveFromList(string key, string value)
        {
            _actions.Add(state =>
            {
                // TODO: Possible that value is null?
                var list = state.ListGetOrAdd(key);
                // TODO: Does this remove all occurrences?
                list.Remove(value);

                if (list.Count == 0) state.ListDelete(list);
            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            _actions.Add(state =>
            {
                if (!state.Lists.TryGetValue(key, out var list)) return;

                var result = new List<string>(); // TODO: Create only when really necessary

                for (var index = 0; index < list.Count; index++)
                {
                    if (index >= keepStartingFrom && index <= keepEndingAt)
                    {
                        // TODO: Ensure resulting list is not inverted
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

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
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

        public override void RemoveHash(string key)
        {
            _actions.Add(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashDelete(hash);
            });
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            _actions.Add(state =>
            {
                // TODO: Don't do anything when items is empty
                var set = state.SetGetOrAdd(key);

                // TODO: Original implementation in SQL Server expects all the items in this case don't exist.
                foreach (var value in items)
                {
                    set.Add(value, 0.0D);
                }
            });
        }

        public override void RemoveSet(string key)
        {
            _actions.Add(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetDelete(set);
            });
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            _actions.Add(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, expireIn);
            });
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            _actions.Add(state =>
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, expireIn);
            });
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            _actions.Add(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, expireIn);
            });
        }

        public override void PersistHash(string key)
        {
            _actions.Add(state =>
            {
                if (state.Hashes.TryGetValue(key, out var hash)) state.HashExpire(hash, null);
            });
        }

        public override void PersistList(string key)
        {
            _actions.Add(state =>
            {
                if (state.Lists.TryGetValue(key, out var list)) state.ListExpire(list, null);
            });
        }

        public override void PersistSet(string key)
        {
            _actions.Add(state =>
            {
                if (state.Sets.TryGetValue(key, out var set)) state.SetExpire(set, null);
            });
        }

        public override void Commit()
        {
            _dispatcher.QueryAndWait(state =>
            {
                // TODO: Check all the preconditions (for example if locks are still held)
                foreach (var action in _actions)
                {
                    action(state);
                }
            });

            foreach (var queue in _enqueued)
            {
                _dispatcher.SignalOneQueueWaitNode(queue);
            }
        }

        private static void CounterIncrement(MemoryState state, string key, int value, TimeSpan? expireIn)
        {
            var counter = state.CounterGetOrAdd(key);
            counter.Value += value;

            if (counter.Value != 0)
            {
                state.CounterExpire(counter, expireIn);
            }
            else
            {
                state.CounterDelete(counter);
            }
        }
    }
}