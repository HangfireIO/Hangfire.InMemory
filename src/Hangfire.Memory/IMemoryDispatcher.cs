using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.Memory
{
    internal interface IMemoryDispatcher
    {
        IReadOnlyDictionary<string, BlockingCollection<string>> TryGetQueues([NotNull] IReadOnlyCollection<string> queueNames);
        JobData GetJobData([NotNull] string jobId);
        string GetJobParameter([NotNull] string jobId, [NotNull] string name);

        T QueryAndWait<T>(Func<MemoryState, T> query);
        void QueryNoWait(Action<MemoryState> query);
    }
}