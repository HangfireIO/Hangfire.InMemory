using System.Collections.Concurrent;

namespace Hangfire.InMemory.Entities
{
    internal sealed class QueueEntry
    {
        public readonly ConcurrentQueue<string> Queue = new ConcurrentQueue<string>();
        public readonly InMemoryQueueWaitNode WaitHead = new InMemoryQueueWaitNode(null);
    }
}