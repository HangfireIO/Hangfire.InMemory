using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Memory
{
    internal sealed class MemoryTransaction : JobStorageTransaction
    {
        private readonly List<Action<MemoryState>> _actions = new List<Action<MemoryState>>();
        private readonly IMemoryDispatcher _dispatcher;

        public MemoryTransaction(IMemoryDispatcher dispatcher)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            _actions.Add(state => EntityExpire(state._jobs, state._jobIndex, jobId, expireIn));
        }

        public override void PersistJob(string jobId)
        {
            _actions.Add(state => EntityExpire(state._jobs, state._jobIndex, jobId, null));
        }

        public override void SetJobState(string jobId, IState state)
        {
            var serializedData = state.SerializeData();
            var now = DateTime.UtcNow; // TODO Replace with time factory

            _actions.Add(memory =>
            {
                // TODO: Precondition: jobId exists
                var backgroundJob = JobGetOrThrow(memory, jobId);
                var stateEntry = new StateEntry
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    Data = serializedData,
                    CreatedAt = now
                };

                if (backgroundJob.State != null && memory._jobStateIndex.TryGetValue(backgroundJob.State.Name, out var indexEntry))
                {
                    indexEntry.Remove(backgroundJob);
                    if (indexEntry.Count == 0) memory._jobStateIndex.Remove(backgroundJob.State.Name);
                }

                backgroundJob.State = stateEntry;
                backgroundJob.History.Add(stateEntry);

                if (!memory._jobStateIndex.TryGetValue(stateEntry.Name, out indexEntry))
                {
                    memory._jobStateIndex.Add(stateEntry.Name, indexEntry = new SortedSet<BackgroundJobEntry>(new BackgroundJobStateCreatedAtComparer()));
                }

                indexEntry.Add(backgroundJob);
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            _actions.Add(memory =>
            {
                // TODO: Precondition: jobId exists
                var backgroundJob = JobGetOrThrow(memory, jobId);
                var stateEntry = new StateEntry
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    Data = state.SerializeData(),
                    CreatedAt = DateTime.UtcNow // TODO Replace with time factory
                };

                backgroundJob.History.Add(stateEntry);
            });
        }

        public override void AddToQueue(string queue, string jobId)
        {
            _actions.Add(state =>
            {
                if (!state._queues.TryGetValue(queue, out var queueObj))
                {
                    state._queues.Add(queue, queueObj = new BlockingCollection<string>(new ConcurrentQueue<string>()));
                }

                queueObj.Add(jobId);
            });
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
            _actions.Add(state => SetAdd(state, key, value, 0.0D));
        }

        public override void AddToSet(string key, string value, double score)
        {
            _actions.Add(state => SetAdd(state, key, value, score));
        }

        public override void RemoveFromSet(string key, string value)
        {
            _actions.Add(state => SetRemove(state, key, value));
        }

        public override void InsertToList(string key, string value)
        {
            _actions.Add(state => ListInsert(state, key, value));
        }

        public override void RemoveFromList(string key, string value)
        {
            _actions.Add(state => ListRemove(state, key, value));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            _actions.Add(state => ListTrim(state, key, keepStartingFrom, keepEndingAt));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            _actions.Add(state => HashSet(state, key, keyValuePairs));
        }

        public override void RemoveHash(string key)
        {
            _actions.Add(state => HashDelete(state, key));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            _actions.Add(state => SetAdd(state, key, items));
        }

        public override void RemoveSet(string key)
        {
            _actions.Add(state => SetDelete(state, key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            _actions.Add(state => HashExpire(state, key, expireIn));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            _actions.Add(state => ListExpire(state, key, expireIn));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            _actions.Add(state => SetExpire(state, key, expireIn));
        }

        public override void PersistHash(string key)
        {
            _actions.Add(state => HashExpire(state, key, null));
        }

        public override void PersistList(string key)
        {
            _actions.Add(state => ListExpire(state, key, null));
        }

        public override void PersistSet(string key)
        {
            _actions.Add(state => SetExpire(state, key, null));
        }

        public override void Commit()
        {
            _dispatcher.QueryNoWait(state =>
            {
                // TODO: Check all the preconditions (for example if locks are still held)
                foreach (var action in _actions)
                {
                    action(state);
                }
            });
        }

        private static BackgroundJobEntry JobGetOrThrow(MemoryState state, string jobId)
        {
            if (!state._jobs.TryGetValue(jobId, out var backgroundJob))
            {
                throw new InvalidOperationException($"Background job with '{jobId}' identifier does not exist.");
            }

            return backgroundJob;
        }

        private static void CounterIncrement(MemoryState state, string key, int value, TimeSpan? expireIn)
        {
            if (!state._counters.TryGetValue(key, out var counter))
            {
                state._counters.Add(key, counter = new CounterEntry(key));
            }

            counter.Value += value;

            if (counter.Value != 0)
            {
                EntityExpire(state._counters, state._counterIndex, key, expireIn);
            }
            else
            {
                state._counters.Remove(key);
                state._counterIndex.Remove(counter); // TODO: Can avoid this by checking expireAt property first
            }
        }

        private static void EntityExpire<T>(
            IDictionary<string, T> collection,
            ISet<T> index,
            string key,
            TimeSpan? expireIn)
            where T : IExpirableEntry
        {
            if (!collection.TryGetValue(key, out var entity))
            {
                return;
            }

            if (entity.ExpireAt.HasValue)
            {
                index.Remove(entity);
            }

            if (expireIn.HasValue)
            {
                // TODO: Replace DateTime.UtcNow everywhere with some time factory
                entity.ExpireAt = DateTime.UtcNow.Add(expireIn.Value);
                index.Add(entity);
            }
            else
            {
                entity.ExpireAt = null;
            }
        }

        private static void SetAdd(MemoryState state, string key, string value, double score)
        {
            if (!state._sets.TryGetValue(key, out var set))
            {
                state._sets.Add(key, set = new SetEntry(key));
            }

            if (!set.Hash.TryGetValue(value, out var entry))
            {
                entry = new SortedSetEntry<string>(value) { Score = score };
                set.Value.Add(entry);
                set.Hash.Add(value, entry);
            }
            else
            {
                // Element already exists, just need to add a score value – re-create it.
                set.Value.Remove(entry);
                entry.Score = score;
                set.Value.Add(entry);
            }
        }

        private static void SetAdd(MemoryState state, string key, IList<string> items)
        {
            if (!state._sets.TryGetValue(key, out var set))
            {
                state._sets.Add(key, set = new SetEntry(key));
            }

            // TODO: Original implementation in SQL Server expects all the items in this case don't exist.
            foreach (var value in items)
            {
                if (!set.Hash.TryGetValue(value, out var entry))
                {
                    entry = new SortedSetEntry<string>(value) { Score = 0.0D };
                    set.Value.Add(entry);
                    set.Hash.Add(value, entry);
                }
                else
                {
                    // Element already exists, just need to add a score value – re-create it.
                    set.Value.Remove(entry);
                    entry.Score = 0.0D;
                    set.Value.Add(entry);
                }
            }
        }

        private static void SetRemove(MemoryState state, string key, string value)
        {
            if (!state._sets.TryGetValue(key, out var set))
            {
                return;
            }

            if (set.Hash.TryGetValue(value, out var entry))
            {
                set.Value.Remove(entry);
                set.Hash.Remove(value);
            }

            if (set.Value.Count == 0)
            {
                state._sets.Remove(key);
                if (set.ExpireAt.HasValue)
                {
                    state._setIndex.Remove(set);
                }
            }
        }

        private static void SetDelete(MemoryState state, string key)
        {
            if (state._sets.TryGetValue(key, out var set))
            {
                state._sets.Remove(key);
                if (set.ExpireAt.HasValue)
                {
                    state._setIndex.Remove(set);
                }
            }
        }

        private static void SetExpire(MemoryState state, string key, TimeSpan? expireIn)
        {
            EntityExpire(state._sets, state._setIndex, key, expireIn);
        }

        private static void ListInsert(MemoryState state, string key, string value)
        {
            // TODO: Possible that value is null?

            if (!state._lists.TryGetValue(key, out var list))
            {
                state._lists.Add(key, list = new ListEntry(key));
            }

            list.Value.Add(value);
        }

        private static void ListRemove(MemoryState state, string key, string value)
        {
            // TODO: Possible that value is null?

            if (!state._lists.TryGetValue(key, out var list))
            {
                state._lists.Add(key, list = new ListEntry(key));
            }

            // TODO: Does this remove all occurrences?
            list.Value.Remove(value);

            if (list.Value.Count == 0)
            {
                state._lists.Remove(key);
                if (list.ExpireAt.HasValue)
                {
                    state._listIndex.Remove(list);
                }
            }
        }

        private static void ListTrim(MemoryState state, string key, int keepFrom, int keepTo)
        {
            if (!state._lists.TryGetValue(key, out var list))
            {
                return;
            }

            var resultingList = new List<string>(); // TODO: Create only when really necessary

            // Our list is inverted, so we are iterating starting from the end.
            var index = list.Value.Count - 1;
            var counter = 0;

            while (index >= 0)
            {
                // TODO: Can optimize this by avoiding iterations on unneeded elements.
                if (counter >= keepFrom && counter <= keepTo)
                {
                    resultingList.Add(list.Value[index]);
                }

                index--;
                counter++;
            }

            if (resultingList.Count > 0)
            {
                list.Value = resultingList;
            }
            else
            {
                state._lists.Remove(key);
                if (list.ExpireAt.HasValue)
                {
                    state._listIndex.Remove(list);
                }
            }
        }

        private static void ListExpire(MemoryState state, string key, TimeSpan? expireIn)
        {
            EntityExpire(state._lists, state._listIndex, key, expireIn);
        }

        private static void HashSet(MemoryState state, string key, IEnumerable<KeyValuePair<string, string>> values)
        {
            // TODO: Avoid creating a hash when values are empty
            if (!state._hashes.TryGetValue(key, out var hash))
            {
                // TODO: What case sensitivity to use?
                state._hashes.Add(key, hash = new HashEntry(key));
            }

            foreach (var valuePair in values)
            {
                hash.Value[valuePair.Key] = valuePair.Value;
            }

            if (hash.Value.Count == 0)
            {
                state._hashes.Remove(key);
                if (hash.ExpireAt.HasValue)
                {
                    state._hashIndex.Remove(hash);
                }
            }
        }

        private static void HashDelete(MemoryState state, string key)
        {
            if (state._hashes.TryGetValue(key, out var hash))
            {
                state._hashes.Remove(key);
                if (hash.ExpireAt.HasValue)
                {
                    state._hashIndex.Remove(hash);
                }
            }
        }
        
        private static void HashExpire(MemoryState state, string key, TimeSpan? expireIn)
        {
            EntityExpire(state._hashes, state._hashIndex, key, expireIn);
        }
    }
}