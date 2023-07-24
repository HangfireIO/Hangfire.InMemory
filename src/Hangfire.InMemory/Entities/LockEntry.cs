using System;
using System.Threading;

namespace Hangfire.InMemory.Entities
{
    internal sealed class LockEntry
    {
        private object _owner;
        private int _referenceCount;
        private int _level;
        private bool _finalized;

        public LockEntry(object owner)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            _referenceCount = 1;
            _level = 1;
            _finalized = false;
        }

        public bool Finalized => _finalized;

        public void TryAcquire(object owner, ref bool acquired)
        {
            lock (this)
            {
                if (_finalized) ThrowFinalizedException();

                if (ReferenceEquals(_owner, owner))
                {
                    _level++;
                    acquired = true;
                }
                else
                {
                    // TODO: Ensure ReferenceCount is updated only under _state._locks
                    _referenceCount++;
                }
            }
        }

        public bool WaitUntilAcquired(object owner, TimeSpan timeout)
        {
            lock (this)
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

                    Monitor.Wait(this, remaining);
                }

                _owner = owner;
                _level = 1;
            }

            return true;
        }

        public void Cancel()
        {
            lock (this)
            {
                if (_finalized) ThrowFinalizedException();

                _referenceCount--;

                if (_referenceCount == 0)
                {
                    _finalized = true;
                }
            }
        }

        public void Release(object owner)
        {
            lock (this)
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
                        Monitor.Pulse(this);
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