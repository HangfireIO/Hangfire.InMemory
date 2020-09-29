using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Hangfire.Annotations;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryDispatcher : IInMemoryDispatcher
    {
        private static readonly InMemoryQueueWaitNode Tombstone = new InMemoryQueueWaitNode(null);

        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1);
        private readonly ConcurrentQueue<InMemoryDispatcherCallback> _queries = new ConcurrentQueue<InMemoryDispatcherCallback>();
        private readonly InMemoryState _state;
        private readonly Thread _thread;

        private PaddedInt64 _outstandingRequests;

        public InMemoryDispatcher(InMemoryState state)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));

            _thread = new Thread(DoWork)
            {
                IsBackground = true,
                Name = "Hangfire:InMemoryDispatcher"
            };
            _thread.Start();
        }

        public IReadOnlyDictionary<string, QueueEntry> GetOrAddQueues([NotNull] IReadOnlyCollection<string> queueNames)
        {
            if (queueNames == null) throw new ArgumentNullException(nameof(queueNames));

            var entries = new Dictionary<string, QueueEntry>(queueNames.Count);
            foreach (var queueName in queueNames)
            {
                entries.Add(queueName, _state.QueueGetOrCreate(queueName));
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
                    _state._locks.Add(resource, entry = new LockEntry {Owner = connection, ReferenceCount = 1, Level = 1});
                    acquired = true;
                }
                else if (entry.Owner == connection)
                {
                    entry.Level++;
                    acquired = true;
                }

                // TODO: Ensure ReferenceCount is updated only under _state._locks
                entry.ReferenceCount++;
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

            node.Value.Release();
        }

        public T QueryAndWait<T>(Func<InMemoryState, T> query)
        {
            using (var callback = new InMemoryDispatcherCallback(state => query(state)))
            {
                _queries.Enqueue(callback);

                if (Volatile.Read(ref _outstandingRequests.Value) == 0)
                {
                    if (Interlocked.Exchange(ref _outstandingRequests.Value, 1) == 0)
                    {
                        _semaphore.Release();
                    }
                }

                // TODO: Add timeout here – dispatcher thread can fail, and we shouldn't block user code in this case
                callback.Ready.Wait();
                return (T) callback.Result;
            }
        }

        public void QueryAndWait(Action<InMemoryState> query)
        {
            QueryAndWait(state =>
            {
                query(state);
                return true;
            });
        }

        private void DoWork()
        {
            while (true)
            {
                if (_semaphore.Wait(TimeSpan.FromSeconds(1)))
                {
                    Interlocked.Exchange(ref _outstandingRequests.Value, 0);

                    while (_queries.TryDequeue(out var next))
                    {
                        next.Result = next.Callback(_state);
                        next.Ready.Set();
                    }
                }
                else
                {
                    var now = DateTime.UtcNow; // TODO: Use time factory instead

                    // TODO: Think how to expire under memory pressure and limit the collection to avoid OOM exceptions
                    ExpireIndex(now, _state._counterIndex, entry => _state.CounterDelete(entry));
                    ExpireIndex(now, _state._hashIndex, entry => _state.HashDelete(entry));
                    ExpireIndex(now, _state._listIndex, entry => _state.ListDelete(entry));
                    ExpireIndex(now, _state._setIndex, entry => _state.SetDelete(entry));
                    ExpireJobIndex(now, _state);
                }
            }
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

        [StructLayout(LayoutKind.Explicit, Size = 2 * CACHE_LINE_SIZE)]
        internal struct PaddedInt64
        {
            internal const int CACHE_LINE_SIZE = 128;

            [FieldOffset(CACHE_LINE_SIZE)]
            internal long Value;
        }
    }
}