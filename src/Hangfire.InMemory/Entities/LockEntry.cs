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
using System.Threading;

namespace Hangfire.InMemory.Entities
{
    internal sealed class LockEntry<T> where T : class
    {
        private readonly object _syncRoot = new object();
        private T? _owner;
        private int _referenceCount;
        private int _level;
        private bool _finalized;

        public bool TryAcquire(T owner, TimeSpan timeout, out bool retry, out bool cleanUp)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));

            retry = false;
            cleanUp = false;

            lock (_syncRoot)
            {
                if (_finalized)
                {
                    // Our entry was finalized by someone else, so we should retry
                    // with a completely new entry.
                    retry = true;
                    return false;
                }

                var timeoutMs = (int)timeout.TotalMilliseconds;
                var started = Environment.TickCount;
                
                if (ReferenceEquals(_owner, owner))
                {
                    // Entry is currently owned by the same owner, so our lock has been
                    // already acquired.
                    _level++;
                    return true;
                }

                // Whether it's already owned or not, we should increase
                // the number of references to avoid finalizing it too early and
                // allow waiting for it.
                _referenceCount++;

                while (_owner != null)
                {
                    var remaining = timeoutMs - unchecked(Environment.TickCount - started);
                    if (remaining < 0)
                    {
                        _referenceCount--;

                        // Finalize if there are no other references and request to clean up
                        // in this case. No retry is needed, just give up.
                        cleanUp = _finalized = _referenceCount == 0;
                        return false;
                    }

                    Monitor.Wait(_syncRoot, remaining);
                }

                // New ownership has been successfully acquired.
                _owner = owner;
                _level = 1;
                return true;
            }
        }

        public void Release(T owner, out bool finalized)
        {
            if (owner == null) throw new ArgumentNullException(nameof(owner));

            lock (_syncRoot)
            {
                if (_finalized) ThrowFinalizedException();
                if (!ReferenceEquals(_owner, owner)) throw new InvalidOperationException("Wrong entry owner");
                if (_level <= 0) throw new InvalidOperationException("Wrong level");
                if (_referenceCount <= 0) throw new InvalidOperationException("Wrong reference count");

                _level--;

                if (_level == 0)
                {
                    _owner = null;
                    _referenceCount--;

                    if (_referenceCount == 0)
                    {
                        _finalized = true;
                    }
                    else
                    {
                        Monitor.Pulse(_syncRoot);
                    }
                }

                finalized = _finalized;
            }
        }

        private static void ThrowFinalizedException()
        {
            throw new InvalidOperationException("Lock entry is already finalized.");
        }
    }
}