using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Hangfire.Memory
{
    internal sealed class MemoryDispatcher : IMemoryDispatcher
    {
        private sealed class MemoryCallback : IDisposable
        {
            public object Result;
            public ManualResetEventSlim Ready = new ManualResetEventSlim(false);
            public Action<MemoryCallback, MemoryState> Callback;

            public void Dispose()
            {
                Ready?.Dispose();
            }
        }

        private readonly MemoryState _state = new MemoryState();
        private readonly BlockingCollection<MemoryCallback> _queries = new BlockingCollection<MemoryCallback>();
        private readonly Thread _thread;

        public MemoryDispatcher()
        {
            _thread = new Thread(DoWork)
            {
                IsBackground = true,
                Name = "Hangfire:InMemoryDispatcher"
            };
            _thread.Start();
        }

        public T QueryAndWait<T>(Func<MemoryState, T> query)
        {
            using (var callback = new MemoryCallback {Callback = (obj, state) =>
                {
                    obj.Result = query(state);
                    obj.Ready.Set();
                }
            })
            {
                _queries.Add(callback);

                // TODO: Add timeout here – dispatcher thread can fail, and we shouldn't block user code in this case
                callback.Ready.Wait();
                return (T) callback.Result;
            }
        }

        public void QueryNoWait(Action<MemoryState> query)
        {
            using (var callback = new MemoryCallback
            {
                Callback = (obj, state) =>
                {
                    query(state);
                }
            })
            {
                _queries.Add(callback);
            }
        }

        private void DoWork()
        {
            var spinWait = new SpinWait();
            while (true)
            {
                if (_queries.TryTake(out var next, TimeSpan.FromSeconds(1)))
                {
                    next.Callback(next, _state);
                }
                else
                {
                    //ExpireIndex(_state._counterIndex, _state._counters);
                    //ExpireIndex(_state._hashIndex, _state._hashes);
                    //ExpireIndex(_state._listIndex, _state._lists);
                    //ExpireIndex(_state._setIndex, _state._sets);
                    //ExpireJobIndex(_state);
                }
            }
        }

        private static void ExpireIndex<T>(SortedSet<T> index, IDictionary<string, T> collection)
            where T : IExpirableEntry
        {
            // TODO: Replace with actual expiration rules
            while (index.Count > 0 && index.Min.ExpireAt.HasValue)
            {
                var entry = index.Min;

                index.Remove(entry);
                collection.Remove(entry.Key);
            }
        }

        private static void ExpireJobIndex(MemoryState state)
        {
            // TODO: Replace with actual expiration rules
            while (state._jobIndex.Count > 0 && state._jobIndex.Min.ExpireAt.HasValue)
            {
                var entry = state._jobIndex.Min;

                state._jobIndex.Remove(entry);
                state._jobs.Remove(entry.Key);

                if (entry.State?.Name != null && state._jobStateIndex.TryGetValue(entry.State.Name, out var stateIndex))
                {
                    stateIndex.Remove(entry);
                    if (stateIndex.Count == 0) state._jobStateIndex.Remove(entry.State.Name);
                }
            }
        }
    }
}