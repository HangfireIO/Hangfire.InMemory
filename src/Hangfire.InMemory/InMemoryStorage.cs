using System;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    public sealed class InMemoryStorage : JobStorage
    {
        private readonly InMemoryDispatcherBase _dispatcher = new InMemoryDispatcher(new InMemoryState(() => DateTime.UtcNow));

        public InMemoryStorage()
            : this(new InMemoryStorageOptions())
        {
        }

        public InMemoryStorage([NotNull] InMemoryStorageOptions options)
        {
            Options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public InMemoryStorageOptions Options { get; }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new InMemoryMonitoringApi(_dispatcher);
        }

        public override IStorageConnection GetConnection()
        {
            return new InMemoryConnection(_dispatcher, Options);
        }

        public override string ToString()
        {
            return "In-Memory Storage";
        }
    }
}
