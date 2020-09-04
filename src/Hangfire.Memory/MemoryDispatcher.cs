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

        private readonly BlockingCollection<MemoryCallback> _queries = new BlockingCollection<MemoryCallback>();
        private readonly MemoryState _state;
        private readonly Thread _thread;

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
    }
}