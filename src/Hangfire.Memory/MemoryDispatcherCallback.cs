using System;
using System.Threading;

namespace Hangfire.Memory
{
    internal sealed class MemoryDispatcherCallback : IDisposable
    {
        private volatile object _result;

        public MemoryDispatcherCallback(Func<MemoryState, object> callback)
        {
            Callback = callback ?? throw new ArgumentNullException(nameof(callback));
        }

        public Func<MemoryState, object> Callback { get; }
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