// This file is part of Hangfire.InMemory. Copyright © 2025 Hangfire OÜ.
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

using System.Globalization;
using System.Threading;
using Hangfire.InMemory.State;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class LongStorageProvider : IStorageProvider, IKeyProvider<ulong>
    {
        private PaddedInt64 _nextId;

        private readonly Dispatcher<ulong, InMemoryConnection<ulong>> _dispatcher;
        private readonly InMemoryStorageOptions _options;

        public LongStorageProvider(Dispatcher<ulong, InMemoryConnection<ulong>> dispatcher, InMemoryStorageOptions options)
        {
            _dispatcher = dispatcher;
            _options = options;
        }

        public void Dispose()
        {
            _dispatcher.Dispose();
        }

        public JobStorageMonitor GetMonitoringApi()
        {
            return new InMemoryMonitoringApi<ulong>(_dispatcher, this);
        }

        public JobStorageConnection GetConnection()
        {
            return new InMemoryConnection<ulong>(_options, _dispatcher, this);
        }

        ulong IKeyProvider<ulong>.GetUniqueKey()
        {
            return (ulong)Interlocked.Increment(ref _nextId.Value);
        }

        bool IKeyProvider<ulong>.TryParse(string input, out ulong key)
        {
            return ulong.TryParse(input, NumberStyles.Integer, CultureInfo.InvariantCulture, out key);
        }

        string IKeyProvider<ulong>.ToString(ulong key)
        {
            return key.ToString(CultureInfo.InvariantCulture);
        }
    }
}