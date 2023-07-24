using System.Collections.Concurrent;

namespace Hangfire.InMemory.Entities
{
    internal sealed class QueueEntry
    {
        public ConcurrentQueue<string> Queue { get; } = new ConcurrentQueue<string>();
        public InMemoryQueueWaitNode WaitHead { get; } = new InMemoryQueueWaitNode(null);
    }
}