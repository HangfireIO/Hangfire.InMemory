using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.InMemory.Entities;
using Hangfire.Storage;

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

        public KeyValuePair<string, QueueEntry>[] GetOrAddQueues(IReadOnlyCollection<string> queueNames)
        {
            var entries = new KeyValuePair<string, QueueEntry>[queueNames.Count];
            var index = 0;

            foreach (var queueName in queueNames)
            {
                entries[index++] = new KeyValuePair<string, QueueEntry>(
                    queueName,
                    _state.QueueGetOrCreate(queueName));
            }

            return entries;
        }

        public bool TryGetJobData(string jobId, out BackgroundJobEntry entry)
        {
            return _state.Jobs.TryGetValue(jobId, out entry);
        }

        public string GetJobParameter(string jobId, string name)
        {
            if (_state.Jobs.TryGetValue(jobId, out var jobEntry) && jobEntry.Parameters.TryGetValue(name, out var result))
            {
                return result;
            }

            return null;
        }

        public bool TryAcquireLockEntry(JobStorageConnection owner, string resource, out LockEntry<JobStorageConnection> entry)
        {
            var acquired = false;

            lock (_state.Locks)
            {
                if (!_state.Locks.TryGetValue(resource, out entry))
                {
                    _state.Locks.Add(resource, entry = new LockEntry<JobStorageConnection>(owner));
                    acquired = true;
                }
                else
                {
                    entry.TryAcquire(owner, ref acquired);
                }
            }

            return acquired;
        }

        public void CancelLockEntry(string resource, LockEntry<JobStorageConnection> entry)
        {
            lock (_state.Locks)
            {
                if (!_state.Locks.TryGetValue(resource, out var current) || !ReferenceEquals(current, entry))
                {
                    throw new InvalidOperationException("Precondition failed when decrementing a lock");
                }

                entry.Cancel();

                if (entry.Finalized)
                {
                    _state.Locks.Remove(resource);
                }
            }
        }

        public void ReleaseLockEntry(JobStorageConnection owner, string resource, LockEntry<JobStorageConnection> entry)
        {
            lock (_state.Locks)
            {
                if (!_state.Locks.TryGetValue(resource, out var current)) throw new InvalidOperationException("Does not contain a lock");
                if (!ReferenceEquals(current, entry)) throw new InvalidOperationException("Does not contain a correct lock entry");
                
                entry.Release(owner);

                if (entry.Finalized)
                {
                    _state.Locks.Remove(resource);
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

        private static void ExpireIndex<T>(DateTime now, SortedSet<T> index, Action<T> action)
            where T : IExpirableEntry
        {
            T entry;

            while (index.Count > 0 && (entry = index.Min).ExpireAt.HasValue && now >= entry.ExpireAt)
            {
                action(entry);
            }
        }

        private static void ExpireJobIndex(DateTime now, InMemoryState state)
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