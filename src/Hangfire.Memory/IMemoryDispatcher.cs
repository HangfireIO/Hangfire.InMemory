using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Hangfire.Annotations;

namespace Hangfire.Memory
{
    internal interface IMemoryDispatcher
    {
        IReadOnlyDictionary<string, QueueEntry> GetOrAddQueues([NotNull] IReadOnlyCollection<string> queueNames);
        bool TryGetJobData([NotNull] string jobId, out BackgroundJobEntry entry);
        string GetJobParameter([NotNull] string jobId, [NotNull] string name);

        bool TryAcquireLockEntry(MemoryConnection connection, string resource, out LockEntry entry);
        void CancelLockEntry(string resource, LockEntry entry);
        void ReleaseLockEntry(MemoryConnection connection, string resource, LockEntry entry);

        void AddQueueWaitNode(QueueEntry entry, MemoryQueueWaitNode node);
        void SignalOneQueueWaitNode(QueueEntry entry);

        T QueryAndWait<T>(Func<MemoryState, T> query);
        void QueryAndWait(Action<MemoryState> query);
    }
}