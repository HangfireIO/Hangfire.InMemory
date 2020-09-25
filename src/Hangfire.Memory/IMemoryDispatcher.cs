using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Hangfire.Annotations;

namespace Hangfire.Memory
{
    internal interface IMemoryDispatcher
    {
        IReadOnlyDictionary<string, BlockingCollection<string>> TryGetQueues([NotNull] IReadOnlyCollection<string> queueNames);

        T QueryAndWait<T>(Func<MemoryState, T> query);
        void QueryNoWait(Action<MemoryState> query);
    }
}