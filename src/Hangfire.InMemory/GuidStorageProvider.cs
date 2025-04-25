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

using System;
using Hangfire.InMemory.State;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class GuidStorageProvider : IStorageProvider, IKeyProvider<Guid>
    {
        private readonly Dispatcher<Guid, InMemoryConnection<Guid>> _dispatcher;
        private readonly InMemoryStorageOptions _options;

        public GuidStorageProvider(Dispatcher<Guid, InMemoryConnection<Guid>> dispatcher, InMemoryStorageOptions options)
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
            return new InMemoryMonitoringApi<Guid>(_dispatcher, this);
        }

        public JobStorageConnection GetConnection()
        {
            return new InMemoryConnection<Guid>(_options, _dispatcher, this);
        }

        Guid IKeyProvider<Guid>.GetUniqueKey()
        {
            return Guid.NewGuid();
        }

        bool IKeyProvider<Guid>.TryParse(string input, out Guid key)
        {
            return Guid.TryParse(input, out key);
        }

        string IKeyProvider<Guid>.ToString(Guid key)
        {
            return key.ToString("D");
        }
    }
}