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
#if !HANGFIRE_170
using System.Collections.Generic;
#endif
using Hangfire.Annotations;
using Hangfire.InMemory.State;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    /// <summary>
    /// A class that represents an in-memory job storage that stores all data
    /// related to background processing in a process' memory.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("ReSharper", "RedundantNullnessAttributeWithNullableReferenceTypes", Justification = "Should be used for public classes")]
    public sealed class InMemoryStorage : JobStorage, IDisposable
    {
        private readonly IStorageProvider _provider;

#if !HANGFIRE_170
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
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryStorage"/> class with default options.
        /// </summary>
        public InMemoryStorage()
            : this(new InMemoryStorageOptions())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryStorage"/> class with specified options.
        /// </summary>
        /// <param name="options">The options for the in-memory storage. Cannot be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when the <paramref name="options"/> argument is null.</exception>
        public InMemoryStorage([NotNull] InMemoryStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            _provider = CreateStorageProvider(options);
            Options = options;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _provider.Dispose();
        }

        /// <summary>
        /// Gets the options for the in-memory storage. 
        /// </summary>
        public InMemoryStorageOptions Options { get; }

        /// <summary>
        /// Override of <see cref="LinearizableReads"/> property. Always returns true for <see cref="InMemoryStorage"/>.
        /// </summary>
        public override bool LinearizableReads => true;

#if !HANGFIRE_170
        /// <inheritdoc />
        public override bool HasFeature(string featureId)
        {
            if (featureId == null) throw new ArgumentNullException(nameof(featureId));

            return _features.TryGetValue(featureId, out var isSupported) 
                ? isSupported
                : base.HasFeature(featureId);
        }
#endif

        /// <inheritdoc />
        public override IMonitoringApi GetMonitoringApi()
        {
            return _provider.GetMonitoringApi();
        }

        /// <inheritdoc />
        public override IStorageConnection GetConnection()
        {
            return _provider.GetConnection();
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "In-Memory Storage";
        }

        private static IStorageProvider CreateStorageProvider(InMemoryStorageOptions options)
        {
            return options.IdType switch
            {
                InMemoryStorageIdType.Guid => new GuidStorageProvider(
                    new Dispatcher<Guid, InMemoryConnection<Guid>>("Hangfire:InMemoryDispatcher",
                        MonotonicTime.GetCurrent, new MemoryState<Guid>(options.StringComparer, null))
                    {
                        CommandTimeout = options.CommandTimeout
                    },
                    options),
                InMemoryStorageIdType.Long => new LongStorageProvider(
                    new Dispatcher<ulong, InMemoryConnection<ulong>>("Hangfire:InMemoryDispatcher",
                        MonotonicTime.GetCurrent, new MemoryState<ulong>(options.StringComparer, null))
                    {
                        CommandTimeout = options.CommandTimeout
                    },
                    options),
                _ => throw new NotSupportedException(
                    $"The given '{nameof(InMemoryStorageOptions)}.{nameof(InMemoryStorageOptions.IdType)}' value is not supported: {options.IdType:G}")
            };
        }
    }
}
