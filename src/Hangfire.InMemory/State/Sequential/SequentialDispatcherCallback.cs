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

namespace Hangfire.InMemory.State.Sequential
{
    internal sealed class SequentialDispatcherCallback<TKey, TCommand, TResult> : ISequentialDispatcherCallback<TKey>, IDisposable
        where TKey : IComparable<TKey>
    {
        private readonly ManualResetEventSlim _ready = new ManualResetEventSlim(false);
        private readonly TCommand _command;
        private readonly Func<TCommand, IMemoryState<TKey>, TResult> _func;

        private TResult? _result;
        private Exception? _exception;

        public SequentialDispatcherCallback(TCommand command, Func<TCommand, IMemoryState<TKey>, TResult> func)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public void Execute(IMemoryState<TKey> state)
        {
            try
            {
                var result = _func(_command, state);

                _result = result;
                _exception = null;
                TrySetReady();
            }
            catch (Exception ex) when (ExceptionHelper.IsCatchableExceptionType(ex))
            {
                _result = default;
                _exception = ex;
                TrySetReady();

                throw;
            }
        }

        public bool Wait(out TResult? result, out Exception? exception, TimeSpan timeout, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (_ready.Wait(timeout, token))
            {
                result = _result;
                exception = _exception;
                return true;
            }

            result = default;
            exception = null;
            return false;
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