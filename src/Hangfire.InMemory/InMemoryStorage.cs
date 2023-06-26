using System;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    public sealed class InMemoryStorage : JobStorage
    {
        private readonly InMemoryDispatcherBase _dispatcher;

        // These options don't relate to the defined storage comparison options
        private readonly Dictionary<string, bool> _features = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
        {
            { "Storage.ExtendedApi", true },
            { "Job.Queue", true },
            { "Connection.GetUtcDateTime", true },
            { "Connection.BatchedGetFirstByLowestScoreFromSet", true },
            { "Connection.GetSetContains", true },
            { "Connection.GetSetCount.Limited", true },
            { "BatchedGetFirstByLowestScoreFromSet", true },
            { "Transaction.AcquireDistributedLock", true },
            { "Transaction.CreateJob", true },
            { "Transaction.SetJobParameter", true },
            { "TransactionalAcknowledge:InMemoryFetchedJob", true },
            { "Monitoring.DeletedStateGraphs", true },
            { "Monitoring.AwaitingJobs", true }
        };

        public InMemoryStorage()
            : this(new InMemoryStorageOptions())
        {
        }

        public InMemoryStorage([NotNull] InMemoryStorageOptions options)
        {
            Options = options ?? throw new ArgumentNullException(nameof(options));

            _dispatcher = new InMemoryDispatcher(new InMemoryState(() => DateTime.UtcNow, Options.StringComparer));
        }

        public InMemoryStorageOptions Options { get; }
        public override bool LinearizableReads => true;

        public override bool HasFeature(string featureId)
        {
            if (featureId == null) throw new ArgumentNullException(nameof(featureId));

            return _features.TryGetValue(featureId, out var isSupported) 
                ? isSupported
                : base.HasFeature(featureId);
        }

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
