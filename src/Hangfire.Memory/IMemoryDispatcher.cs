using System;

namespace Hangfire.Memory
{
    internal interface IMemoryDispatcher
    {
        T QueryAndWait<T>(Func<MemoryState, T> query);
        void QueryNoWait(Action<MemoryState> query);
    }
}