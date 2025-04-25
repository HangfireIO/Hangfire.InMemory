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
    internal sealed class QueueEntry<TKey> where TKey : IComparable<TKey>
    {
        private static readonly WaitNode Tombstone = new WaitNode(null);

        private readonly WaitNode _waitHead = new WaitNode(null);
        public ConcurrentQueue<TKey> Queue { get; } = new ConcurrentQueue<TKey>();

        internal bool NoWaiters => _waitHead.Next == null;

        public void AddWaitEvent(AutoResetEvent waitEvent)
        {
            if (waitEvent == null) throw new ArgumentNullException(nameof(waitEvent));

            var node = new WaitNode(waitEvent);
            var headNext = node.Next;
            var spinWait = new SpinWait();

            while (true)
            {
                var newNext = Interlocked.CompareExchange(ref _waitHead.Next, node, headNext);
                if (newNext == headNext) break;

                headNext = node.Next = newNext;
                spinWait.SpinOnce();
            }
        }

        public void SignalOneWaitEvent()
        {
            if (Volatile.Read(ref _waitHead.Next) == null) return;
            SignalOneWaitEventSlow();
        }

        private void SignalOneWaitEventSlow()
        {
            while (true)
            {
                var node = Interlocked.Exchange(ref _waitHead.Next, null);
                if (node == null) return;

                var tailNode = Interlocked.Exchange(ref node.Next, Tombstone);
                if (tailNode != null)
                {
                    var waitHead = _waitHead;
                    do
                    {
                        waitHead = Interlocked.CompareExchange(ref waitHead.Next, tailNode, null);
                        if (ReferenceEquals(waitHead, Tombstone))
                        {
                            waitHead = _waitHead;
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

        private sealed class WaitNode(AutoResetEvent? value)
        {
            public readonly AutoResetEvent? Value = value;
            public WaitNode? Next;
        }
    }
}