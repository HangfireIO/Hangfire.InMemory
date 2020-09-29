using System.Threading;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryQueueWaitNode
    {
        public InMemoryQueueWaitNode(SemaphoreSlim value)
        {
            Value = value;
        }

        public readonly SemaphoreSlim Value;
        public InMemoryQueueWaitNode Next;
    }
}