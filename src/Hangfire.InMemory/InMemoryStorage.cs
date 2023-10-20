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

            _dispatcher = new InMemoryDispatcher(new InMemoryState(MonotonicTime.GetCurrent, Options));
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
            return new InMemoryConnection(_dispatcher);
        }

        public override string ToString()
        {
            return "In-Memory Storage";
        }
    }
}
