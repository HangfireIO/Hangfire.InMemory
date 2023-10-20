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
        private T _owner;
        private int _referenceCount;
        private int _level;
        private bool _finalized;

        public LockEntry(T owner)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            _referenceCount = 1;
            _level = 1;
            _finalized = false;
        }

        public bool Finalized => _finalized;

        public void TryAcquire(T owner, ref bool acquired)
        {
            lock (_syncRoot)
            {
                if (_finalized) ThrowFinalizedException();

                if (ReferenceEquals(_owner, owner))
                {
                    _level++;
                    acquired = true;
                }
                else
                {
                    _referenceCount++;
                }
            }
        }

        public bool WaitUntilAcquired(T owner, TimeSpan timeout)
        {
            lock (_syncRoot)
            {
                if (_finalized) ThrowFinalizedException();

                var timeoutMs = (int)timeout.TotalMilliseconds;
                var started = Environment.TickCount;

                while (_owner != null)
                {
                    var remaining = timeoutMs - unchecked(Environment.TickCount - started);
                    if (remaining < 0)
                    {
                        return false;
                    }

                    Monitor.Wait(_syncRoot, remaining);
                }

                _owner = owner;
                _level = 1;
            }

            return true;
        }

        public void Cancel()
        {
            lock (_syncRoot)
            {
                if (_finalized) ThrowFinalizedException();

                _referenceCount--;

                if (_referenceCount == 0)
                {
                    _finalized = true;
                }
            }
        }

        public void Release(T owner)
        {
            lock (_syncRoot)
            {
                if (_finalized) ThrowFinalizedException();
                if (!ReferenceEquals(_owner, owner)) throw new InvalidOperationException("Wrong entry owner");
                if (_level <= 0) throw new InvalidOperationException("Wrong level");

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
            }
        }

        private static void ThrowFinalizedException()
        {
            throw new InvalidOperationException("Lock entry is already finalized.");
        }
    }
}