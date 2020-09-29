using System.Threading;

namespace Hangfire.InMemory
{
    internal sealed class MemoryQueueWaitNode
    {
        public MemoryQueueWaitNode(SemaphoreSlim value)
        {
            Value = value;
        }

        public readonly SemaphoreSlim Value;
        public MemoryQueueWaitNode Next;
    }
}