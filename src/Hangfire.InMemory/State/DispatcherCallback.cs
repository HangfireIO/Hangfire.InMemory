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

namespace Hangfire.InMemory.State
{
    internal interface IDispatcherCallback<TKey>
        where TKey : IComparable<TKey>
    {
        void Execute(MemoryState<TKey> state);
    }

    internal sealed class DispatcherCallback<TKey, TCommand, TResult> : IDispatcherCallback<TKey>, IDisposable
        where TKey : IComparable<TKey>
    {
        private readonly ManualResetEventSlim _ready = new ManualResetEventSlim(false);
        private readonly TCommand _command;
        private readonly Func<TCommand, MemoryState<TKey>, TResult> _func;
        private readonly bool _rethrowExceptions;

        private bool _isFaulted;
        private TResult? _result;
        private Exception? _exception;

        public DispatcherCallback(TCommand command, Func<TCommand, MemoryState<TKey>, TResult> func, bool rethrowExceptions)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _func = func ?? throw new ArgumentNullException(nameof(func));
            _rethrowExceptions = rethrowExceptions;
        }

        public bool IsFaulted { get { lock (_ready) { return _isFaulted; } } }
        public TResult? Result { get { lock (_ready) { return _result; } } }
        public Exception? Exception { get { lock (_ready) { return _exception; } } }

        public void Execute(MemoryState<TKey> state)
        {
            try
            {
                var result = _func(_command, state);

                lock (_ready)
                {
                    _isFaulted = false;
                    _result = result;
                }

                TrySetReady();
            }
            catch (Exception ex) when (ExceptionHelper.IsCatchableExceptionType(ex))
            {
                lock (_ready)
                {
                    _isFaulted = true;
                    _exception = ex;
                }

                TrySetReady();

                if (_rethrowExceptions)
                {
                    throw;
                }
            }
        }

        public bool Wait(TimeSpan timeout, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            return _ready.Wait(timeout, token);
        }

        public void Dispose()
        {
            _ready.Dispose();
        }

        private void TrySetReady()
        {
            try
            {
                _ready.Set();
            }
            catch (ObjectDisposedException)
            {
                // Benign race condition, nothing to signal in this case.
            }
        }
    }
}