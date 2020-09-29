using System;
using System.Collections.Generic;
using Hangfire.Annotations;

namespace Hangfire.InMemory
{
    internal interface IInMemoryDispatcher
    {
        IReadOnlyDictionary<string, QueueEntry> GetOrAddQueues([NotNull] IReadOnlyCollection<string> queueNames);
        bool TryGetJobData([NotNull] string jobId, out BackgroundJobEntry entry);
        string GetJobParameter([NotNull] string jobId, [NotNull] string name);

        bool TryAcquireLockEntry(InMemoryConnection connection, string resource, out LockEntry entry);
        void CancelLockEntry(string resource, LockEntry entry);
        void ReleaseLockEntry(InMemoryConnection connection, string resource, LockEntry entry);

        void AddQueueWaitNode(QueueEntry entry, InMemoryQueueWaitNode node);
        void SignalOneQueueWaitNode(QueueEntry entry);

        T QueryAndWait<T>(Func<InMemoryState, T> query);
        void QueryAndWait(Action<InMemoryState> query);
    }
}