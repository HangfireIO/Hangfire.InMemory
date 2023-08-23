using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Hangfire.InMemory.Entities
{
    internal sealed class QueueEntry
    {
        private static readonly InMemoryQueueWaitNode Tombstone = new InMemoryQueueWaitNode(null);

        public ConcurrentQueue<string> Queue { get; } = new ConcurrentQueue<string>();
        public InMemoryQueueWaitNode WaitHead { get; } = new InMemoryQueueWaitNode(null);
        
        public void AddWaitNode(InMemoryQueueWaitNode node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            var headNext = node.Next = null;
            var spinWait = new SpinWait();

            while (true)
            {
                var newNext = Interlocked.CompareExchange(ref WaitHead.Next, node, headNext);
                if (newNext == headNext) break;

                headNext = node.Next = newNext;
                spinWait.SpinOnce();
            }
        }

        public void SignalOneWaitNode()
        {
            if (Volatile.Read(ref WaitHead.Next) == null) return;
            SignalOneWaitNodeSlow();
        }

        private void SignalOneWaitNodeSlow()
        {
            while (true)
            {
                var node = Interlocked.Exchange(ref WaitHead.Next, null);
                if (node == null) return;

                var tailNode = Interlocked.Exchange(ref node.Next, Tombstone);
                if (tailNode != null)
                {
                    var waitHead = WaitHead;
                    do
                    {
                        waitHead = Interlocked.CompareExchange(ref waitHead.Next, tailNode, null);
                        if (ReferenceEquals(waitHead, Tombstone))
                        {
                            waitHead = WaitHead;
                        }
                    } while (waitHead != null);
                }

                try
                {
                    node.Value.Set();
                    return;
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }
}