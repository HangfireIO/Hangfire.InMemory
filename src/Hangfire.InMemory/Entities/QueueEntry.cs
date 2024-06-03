// This file is part of Hangfire.InMemory. Copyright © 2020 Hangfire OÜ.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Hangfire.InMemory.Entities
{
    internal sealed class QueueEntry<TKey>
        where TKey : IComparable<TKey>
    {
        private static readonly QueueWaitNode Tombstone = new QueueWaitNode(null);

        public ConcurrentQueue<TKey> Queue { get; } = new ConcurrentQueue<TKey>();
        public QueueWaitNode WaitHead { get; } = new QueueWaitNode(null);
        
        public void AddWaitNode(QueueWaitNode node)
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
                    if (node.Value == null)
                    {
                        throw new InvalidOperationException("Trying to signal on a Tombstone object.");
                    }

                    node.Value.Set();
                    return;
                }
                catch (ObjectDisposedException)
                {
                    // Benign race condition, nothing to signal in this case.
                }
            }
        }
    }
}