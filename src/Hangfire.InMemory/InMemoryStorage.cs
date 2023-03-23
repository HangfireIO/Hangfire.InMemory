using System;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    public sealed class InMemoryStorage : JobStorage
    {
        private readonly InMemoryDispatcherBase _dispatcher = new InMemoryDispatcher(new InMemoryState(() => DateTime.UtcNow));

        private readonly Dictionary<string, bool> _features = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
        {
            { "Storage.ExtendedApi", true },
            { "Job.Queue", true },
            { "Connection.GetUtcDateTime", true },
            { "Connection.BatchedGetFirstByLowestScoreFromSet", true },
            { "Connection.GetSetContains", true },
            { "Connection.GetSetCount.Limited", true },
            { "BatchedGetFirstByLowestScoreFromSet", true },
            { "Transaction.AcquireDistributedLock", false },
            { "Transaction.CreateJob", true },
            { "Transaction.SetJobParameter", false },
            { "TransactionalAcknowledge:InMemoryFetchedJob", false },
            { "Monitoring.DeletedStateGraphs", false },
            { "Monitoring.AwaitingJobs", false }
        };

        public InMemoryStorage()
            : this(new InMemoryStorageOptions())
        {
        }

        public InMemoryStorage([NotNull] InMemoryStorageOptions options)
        {
            Options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public InMemoryStorageOptions Options { get; }

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
