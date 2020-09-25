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

        bool TryAcquireLockEntry(MemoryConnection connection, string resource, out LockEntry entry);
        void CancelLockEntry(string resource, LockEntry entry);
        void ReleaseLockEntry(MemoryConnection connection, string resource, LockEntry entry);

        T QueryAndWait<T>(Func<MemoryState, T> query);
        void QueryAndWait(Action<MemoryState> query);
    }
}