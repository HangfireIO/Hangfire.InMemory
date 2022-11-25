using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryDispatcher : InMemoryDispatcherBase
    {
        private static readonly TimeSpan DefaultQueryTimeout = TimeSpan.FromSeconds(15);
        private static readonly uint DefaultExpirationIntervalMs = 1000U;

        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1);
        private readonly ConcurrentQueue<InMemoryDispatcherCallback> _queries = new ConcurrentQueue<InMemoryDispatcherCallback>();
        private readonly Thread _thread;
        private readonly ILog _logger = LogProvider.GetLogger(typeof(InMemoryStorage));

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

                if (!callback.Ready.Wait(DefaultQueryTimeout))
                {
                    throw new TimeoutException();
                }

                return callback.Result;
            }
        }

        private void DoWork()
        {
            try
            {
                while (true)
                {
                    if (_semaphore.Wait(TimeSpan.FromMilliseconds(DefaultExpirationIntervalMs)))
                    {
                        Interlocked.Exchange(ref _outstandingRequests.Value, 0);

                        var startTime = Environment.TickCount;

                        while (_queries.TryDequeue(out var next))
                        {
                            next.Result = base.QueryAndWait(next.Callback);

                            try
                            {
                                next.Ready.Set();
                            }
                            catch (ObjectDisposedException)
                            {
                            }

                            if (Environment.TickCount - startTime >= DefaultExpirationIntervalMs)
                            {
                                ExpireEntries();
                                startTime = Environment.TickCount;
                            }
                        }
                    }
                    else
                    {
                        ExpireEntries();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.FatalException("Query dispatcher stopped due to an exception, no queries will be processed. Please report this problem to Hangfire.InMemory developers.", ex);
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