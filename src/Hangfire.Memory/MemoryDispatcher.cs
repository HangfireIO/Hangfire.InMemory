using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.Memory
{
    internal sealed class MemoryDispatcher : IMemoryDispatcher
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1);
        private readonly ConcurrentQueue<MemoryDispatcherCallback> _queries = new ConcurrentQueue<MemoryDispatcherCallback>();
        private readonly MemoryState _state;
        private readonly Thread _thread;

        private PaddedInt64 _outstandingRequests;

        public MemoryDispatcher(MemoryState state)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));

            _thread = new Thread(DoWork)
            {
                IsBackground = true,
                Name = "Hangfire:InMemoryDispatcher"
            };
            _thread.Start();
        }

        public IReadOnlyDictionary<string, BlockingCollection<string>> TryGetQueues([NotNull] IReadOnlyCollection<string> queueNames)
        {
            if (queueNames == null) throw new ArgumentNullException(nameof(queueNames));

            var entries = new Dictionary<string, BlockingCollection<string>>(queueNames.Count);
            foreach (var queueName in queueNames)
            {
                if (_state.Queues.TryGetValue(queueName, out var queue))
                {
                    entries.Add(queueName, queue);
                }
            }

            return entries;
        }

        public JobData GetJobData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_state.Jobs.TryGetValue(jobId, out var jobEntry))
            {
                return null;
            }

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = jobEntry.InvocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                LoadException = loadException,
                CreatedAt = jobEntry.CreatedAt,
                State = jobEntry.State?.Name
            };
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

        public bool TryAcquireLockEntry(MemoryConnection connection, string resource, out LockEntry entry)
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

        public void ReleaseLockEntry(MemoryConnection connection, string resource, LockEntry entry)
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

        public T QueryAndWait<T>(Func<MemoryState, T> query)
        {
            using (var callback = new MemoryDispatcherCallback(state => query(state)))
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

        public void QueryAndWait(Action<MemoryState> query)
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

        private static void ExpireJobIndex(DateTime now, MemoryState state)
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