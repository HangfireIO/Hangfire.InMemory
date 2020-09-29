using System;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    public sealed class InMemoryStorage : JobStorage
    {
        private readonly InMemoryDispatcherBase _dispatcher = new InMemoryDispatcher(new InMemoryState(() => DateTime.UtcNow));

        public override IMonitoringApi GetMonitoringApi()
        {
            return new InMemoryMonitoringApi(_dispatcher);
        }

        public override IStorageConnection GetConnection()
        {
            return new InMemoryConnection(_dispatcher);
        }
    }
}
