using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Annotations;

namespace Hangfire.InMemory
{
    internal class InMemoryDispatcherBase
    {
        private static readonly InMemoryQueueWaitNode Tombstone = new InMemoryQueueWaitNode(null);
        private readonly InMemoryState _state;

        public InMemoryDispatcherBase(InMemoryState state)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
        }

        public IReadOnlyDictionary<string, QueueEntry> GetOrAddQueues([NotNull] IReadOnlyCollection<string> queueNames)
        {
            if (queueNames == null) throw new ArgumentNullException(nameof(queueNames));

            var entries = new Dictionary<string, QueueEntry>(queueNames.Count);
            foreach (var queueName in queueNames)
            {
                if (!entries.ContainsKey(queueName))
                {
                    entries.Add(queueName, _state.QueueGetOrCreate(queueName));
                }
            }

            return entries;
        }

        public bool TryGetJobData([NotNull] string jobId, out BackgroundJobEntry entry)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            return _state.Jobs.TryGetValue(jobId, out entry);
        }

        public string GetJobParameter([NotNull] string jobId, [NotNull] string name)
        {
            // TODO: Consider removing guard statements below and from other method and delegate checks to Connection and Transaction instead
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (name == null) throw new ArgumentNullException(nameof(name));

            if (_state.Jobs.TryGetValue(jobId, out var jobEntry) && jobEntry.Parameters.TryGetValue(name, out var result))
            {
                return result;
            }

            return null;
        }

        public bool TryAcquireLockEntry(InMemoryConnection connection, string resource, out LockEntry entry)
        {
            var acquired = false;

            lock (_state._locks)
            {
                if (!_state._locks.TryGetValue(resource, out entry))
                {
                    _state._locks.Add(resource, entry = new LockEntry { Owner = connection, ReferenceCount = 1, Level = 1 });
                    acquired = true;
                }
                else if (entry.Owner == connection)
                {
                    entry.Level++;
                    acquired = true;
                }
                else
                {
                    // TODO: Ensure ReferenceCount is updated only under _state._locks
                    entry.ReferenceCount++;
                }
            }

            return acquired;
        }

        public void CancelLockEntry(string resource, LockEntry entry)
        {
            lock (_state._locks)
            {
                if (!_state._locks.TryGetValue(resource, out var current2) || !ReferenceEquals(current2, entry))
                {
                    throw new InvalidOperationException("Precondition failed when decrementing a lock");
                }

                entry.ReferenceCount--;

                if (entry.ReferenceCount == 0)
                {
                    _state._locks.Remove(resource);
                }
            }
        }

        public void ReleaseLockEntry(InMemoryConnection connection, string resource, LockEntry entry)
        {
            // TODO: Ensure lock ordering to avoid deadlocks
            lock (_state._locks)
            {
                if (!_state._locks.TryGetValue(resource, out var current)) throw new InvalidOperationException("Does not contain a lock");
                if (!ReferenceEquals(current, entry)) throw new InvalidOperationException("Does not contain a correct lock entry");

                lock (entry)
                {
                    if (!ReferenceEquals(entry.Owner, connection)) throw new InvalidOperationException("Wrong entry owner");
                    if (entry.Level <= 0) throw new InvalidOperationException("Wrong level");

                    entry.Level--;

                    if (entry.Level == 0)
                    {
                        entry.Owner = null;
                        entry.ReferenceCount--;

                        if (entry.ReferenceCount == 0)
                        {
                            _state._locks.Remove(resource);
                        }
                        else
                        {
                            Monitor.Pulse(entry);
                        }
                    }
                }
            }
        }

        public void AddQueueWaitNode(QueueEntry entry, InMemoryQueueWaitNode node)
        {
            var headNext = node.Next = null;
            var spinWait = new SpinWait();

            while (true)
            {
                var newNext = Interlocked.CompareExchange(ref entry.WaitHead.Next, node, headNext);
                if (newNext == headNext) break;

                headNext = node.Next = newNext;
                spinWait.SpinOnce();
            }
        }

        public void SignalOneQueueWaitNode(QueueEntry entry)
        {
            if (Volatile.Read(ref entry.WaitHead.Next) == null) return;
            SignalOneQueueWaitNodeSlow(entry);
        }

        private static void SignalOneQueueWaitNodeSlow(QueueEntry entry)
        {
            while (true)
            {
                var node = Interlocked.Exchange(ref entry.WaitHead.Next, null);
                if (node == null) return;

                var tailNode = Interlocked.Exchange(ref node.Next, Tombstone);
                if (tailNode != null)
                {
                    var waitHead = entry.WaitHead;
                    do
                    {
                        waitHead = Interlocked.CompareExchange(ref waitHead.Next, tailNode, null);
                        if (ReferenceEquals(waitHead, Tombstone))
                        {
                            waitHead = entry.WaitHead;
                        }
                    } while (waitHead != null);
                }

                try
                {
                    node.Value.Set();
                    return;
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        protected virtual object QueryAndWait(Func<InMemoryState, object> query)
        {
            return query(_state);
        }

        public T QueryAndWait<T>(Func<InMemoryState, T> query)
        {
            object Callback(InMemoryState state) => query(state);
            return (T)QueryAndWait(Callback);
        }

        public void QueryAndWait(Action<InMemoryState> query)
        {
            QueryAndWait(state =>
            {
                query(state);
                return true;
            });
        }

        protected void ExpireEntries()
        {
            var now = _state.TimeResolver();

            // TODO: Think how to expire under memory pressure and limit the collection to avoid OOM exceptions
            ExpireIndex(now, _state._counterIndex, entry => _state.CounterDelete(entry));
            ExpireIndex(now, _state._hashIndex, entry => _state.HashDelete(entry));
            ExpireIndex(now, _state._listIndex, entry => _state.ListDelete(entry));
            ExpireIndex(now, _state._setIndex, entry => _state.SetDelete(entry));
            ExpireJobIndex(now, _state);
        }

        protected static void ExpireIndex<T>(DateTime now, SortedSet<T> index, Action<T> action)
            where T : IExpirableEntry
        {
            T entry;

            while (index.Count > 0 && (entry = index.Min).ExpireAt.HasValue && now >= entry.ExpireAt)
            {
                action(entry);
            }
        }

        protected void ExpireJobIndex(DateTime now, InMemoryState state)
        {
            BackgroundJobEntry entry;

            // TODO: Replace with actual expiration rules
            while (state._jobIndex.Count > 0 && (entry = state._jobIndex.Min).ExpireAt.HasValue && now >= entry.ExpireAt)
            {
                state.JobDelete(entry);
            }
        }
    }
}