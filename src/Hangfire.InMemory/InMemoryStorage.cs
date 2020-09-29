using Hangfire.Storage;

namespace Hangfire.InMemory
{
    public sealed class InMemoryStorage : JobStorage
    {
        private readonly IInMemoryDispatcher _dispatcher = new InMemoryDispatcher(new InMemoryState());

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
