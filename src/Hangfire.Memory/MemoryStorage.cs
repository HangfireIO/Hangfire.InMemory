using Hangfire.Storage;

namespace Hangfire.Memory
{
    public sealed class MemoryStorage : JobStorage
    {
        private readonly IMemoryDispatcher _dispatcher = new MemoryDispatcher();

        public override IMonitoringApi GetMonitoringApi()
        {
            return new MemoryMonitoringApi(_dispatcher);
        }

        public override IStorageConnection GetConnection()
        {
            return new MemoryConnection(_dispatcher);
        }
    }
}
