using System.Threading;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryQueueWaitNode
    {
        public InMemoryQueueWaitNode(AutoResetEvent value)
        {
            Value = value;
        }

        public readonly AutoResetEvent Value;
        public InMemoryQueueWaitNode Next;
    }
}