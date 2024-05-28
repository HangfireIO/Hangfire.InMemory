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
    internal sealed class DispatcherCallback<TKey> : IDisposable
        where TKey : IComparable<TKey>
    {
        private readonly ManualResetEventSlim _ready = new ManualResetEventSlim(false);
        private readonly ICommand<TKey, object> _command;
        private readonly bool _rethrowExceptions;
        private volatile object _result;

        public DispatcherCallback(ICommand<TKey, object> command, bool rethrowExceptions)
        {
            _rethrowExceptions = rethrowExceptions;
            _command = command ?? throw new ArgumentNullException(nameof(command));
        }

        public bool IsFaulted { get; private set; }
        public object Result => _result;

        public void Execute(MemoryState<TKey> state)
        {
            try
            {
                _result = _command.Execute(state);
                IsFaulted = false;
                TrySetReady();
            }
            catch (Exception ex) when (ExceptionHelper.IsCatchableExceptionType(ex))
            {
                _result = ex;
                IsFaulted = true;
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
            }
        }
    }
}