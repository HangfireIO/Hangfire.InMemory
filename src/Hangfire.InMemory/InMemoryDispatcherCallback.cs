using System;
using System.Threading;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryDispatcherCallback : IDisposable
    {
        private volatile object _result;

        public InMemoryDispatcherCallback(Func<InMemoryState, object> callback)
        {
            Callback = callback ?? throw new ArgumentNullException(nameof(callback));
        }

        public Func<InMemoryState, object> Callback { get; }
        public ManualResetEventSlim Ready { get; } = new ManualResetEventSlim(false);

        public object Result
        {
            get => _result;
            set => _result = value;
        }

        public void Dispose()
        {
            Ready?.Dispose();
        }
    }
}