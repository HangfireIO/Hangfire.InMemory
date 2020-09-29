using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Threading;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryDispatcher : InMemoryDispatcherBase
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1);
        private readonly ConcurrentQueue<InMemoryDispatcherCallback> _queries = new ConcurrentQueue<InMemoryDispatcherCallback>();
        private readonly Thread _thread;

        private PaddedInt64 _outstandingRequests;

        public InMemoryDispatcher(InMemoryState state) : base(state)
        {
            _thread = new Thread(DoWork)
            {
                IsBackground = true,
                Name = "Hangfire:InMemoryDispatcher"
            };
            _thread.Start();
        }

        protected override object QueryAndWait(Func<InMemoryState, object> query)
        {
            using (var callback = new InMemoryDispatcherCallback(query))
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
                return callback.Result;
            }
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
                        next.Result = base.QueryAndWait(next.Callback);
                        next.Ready.Set();
                    }
                }
                else
                {
                    var now = DateTime.UtcNow; // TODO: Use time factory instead
                    ExpireEntries(now);
                }
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