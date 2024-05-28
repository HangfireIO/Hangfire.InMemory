// This file is part of Hangfire.InMemory. Copyright © 2020 Hangfire OÜ.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Concurrent;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryDispatcher<TKey> : InMemoryDispatcherBase<TKey>, IDisposable
        where TKey : IComparable<TKey>
    {
        private const uint DefaultExpirationIntervalMs = 1000U;
        private static readonly TimeSpan DefaultQueryTimeout = TimeSpan.FromSeconds(15);

        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1);
        private readonly ConcurrentBag<InMemoryDispatcherCallback<TKey>> _readQueries = new ConcurrentBag<InMemoryDispatcherCallback<TKey>>();
        private readonly ConcurrentBag<InMemoryDispatcherCallback<TKey>> _queries = new ConcurrentBag<InMemoryDispatcherCallback<TKey>>();
        private readonly Thread _thread;
        private readonly ILog _logger = LogProvider.GetLogger(typeof(InMemoryStorage));
        private volatile bool _disposed;

        private PaddedInt64 _outstandingRequests;

        public InMemoryDispatcher(Func<MonotonicTime> timeResolver, InMemoryState<TKey> state) : base(timeResolver, state)
        {
            _thread = new Thread(DoWork)
            {
                IsBackground = true,
                Name = "Hangfire:InMemoryDispatcher"
            };
            _thread.Start();
        }

        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;
            _semaphore.Dispose();
            _thread.Join();
        }

        public override object QueryWriteAndWait(IInMemoryCommand<TKey, object> query)
        {
            if (_disposed) ThrowObjectDisposedException();

            using (var callback = new InMemoryDispatcherCallback<TKey>(query, rethrowExceptions: true))
            {
                _queries.Add(callback);

                if (Volatile.Read(ref _outstandingRequests.Value) == 0)
                {
                    if (Interlocked.Exchange(ref _outstandingRequests.Value, 1) == 0)
                    {
                        _semaphore.Release();
                    }
                }

                if (!callback.Wait(DefaultQueryTimeout, CancellationToken.None))
                {
                    throw new TimeoutException();
                }

                if (callback.IsFaulted)
                {
                    throw new InvalidOperationException("Dispatcher stopped due to an unhandled exception, storage state is corrupted.", (Exception)callback.Result);
                }

                return callback.Result;
            }
        }

        public override object QueryReadAndWait(IInMemoryCommand<TKey, object> query)
        {
            if (_disposed) ThrowObjectDisposedException();

            using (var callback = new InMemoryDispatcherCallback<TKey>(query, rethrowExceptions: false))
            {
                _readQueries.Add(callback);

                if (Volatile.Read(ref _outstandingRequests.Value) == 0)
                {
                    if (Interlocked.Exchange(ref _outstandingRequests.Value, 1) == 0)
                    {
                        _semaphore.Release();
                    }
                }

                if (!callback.Wait(DefaultQueryTimeout, CancellationToken.None))
                {
                    throw new TimeoutException();
                }

                if (callback.IsFaulted)
                {
                    throw new InvalidOperationException("An exception occurred while executing a read query. Please see inner exception for details.", (Exception)callback.Result);
                }

                return callback.Result;
            }
        }

        private void DoWork()
        {
            try
            {
                while (!_disposed)
                {
                    if (_semaphore.Wait(TimeSpan.FromMilliseconds(DefaultExpirationIntervalMs)))
                    {
                        Interlocked.Exchange(ref _outstandingRequests.Value, 0);

                        var startTime = Environment.TickCount;

                        while (_readQueries.TryTake(out var next) || _queries.TryTake(out next))
                        {
                            next.Execute(State);

                            if (Environment.TickCount - startTime >= DefaultExpirationIntervalMs)
                            {
                                EvictExpiredEntries();
                                startTime = Environment.TickCount;
                            }
                        }
                    }
                    else
                    {
                        EvictExpiredEntries();
                    }
                }
            }
            catch (ObjectDisposedException ex) when (_disposed)
            {
                _logger.DebugException("Query dispatched stopped, because it was disposed.", ex);
            }
            catch (Exception ex) when (ExceptionHelper.IsCatchableExceptionType(ex))
            {
                _logger.FatalException("Query dispatcher stopped due to an exception, no queries will be processed. Please report this problem to Hangfire.InMemory developers.", ex);
            }
        }

        private static void ThrowObjectDisposedException()
        {
            throw new ObjectDisposedException(typeof(InMemoryDispatcher<TKey>).FullName);
        }
    }
}