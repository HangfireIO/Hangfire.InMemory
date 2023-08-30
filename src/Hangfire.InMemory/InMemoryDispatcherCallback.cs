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

namespace Hangfire.InMemory
{
    internal sealed class InMemoryDispatcherCallback : IDisposable
    {
        private volatile object _result;

        public InMemoryDispatcherCallback(Func<InMemoryState, object> callback)
        {
            Callback = callback ?? throw new ArgumentNullException(nameof(callback));
        }

        public Func<InMemoryState, object> Callback { get; }
        public ManualResetEventSlim Ready { get; } = new ManualResetEventSlim(false);

        public object Result
        {
            get => _result;
            set => _result = value;
        }

        public void Dispose()
        {
            Ready?.Dispose();
        }
    }
}