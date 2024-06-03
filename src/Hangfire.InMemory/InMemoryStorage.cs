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
using System.Globalization;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.InMemory.State;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    /// <summary>
    /// A class that represents an in-memory job storage that stores all data
    /// related to background processing in a process' memory.
    /// </summary>
    public sealed class InMemoryStorage : JobStorage, IKeyProvider<Guid>, IKeyProvider<ulong>, IDisposable
    {
        private readonly Dispatcher<Guid>? _guidDispatcher;
        private readonly Dispatcher<ulong>? _longDispatcher;

        private PaddedInt64 _nextId;

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
            Options = options ?? throw new ArgumentNullException(nameof(options));

            switch (options.IdType)
            {
                case InMemoryStorageIdType.Guid:
                    _guidDispatcher = new Dispatcher<Guid>(
                        "Hangfire:InMemoryDispatcher",
                        Options.CommandTimeout,
                        MonotonicTime.GetCurrent,
                        new MemoryState<Guid>(Options.StringComparer, null));
                    break;
                case InMemoryStorageIdType.Integer:
                    _longDispatcher = new Dispatcher<ulong>(
                        "Hangfire:InMemoryDispatcher",
                        Options.CommandTimeout,
                        MonotonicTime.GetCurrent,
                        new MemoryState<ulong>(Options.StringComparer, null));
                    break;
                default:
                    throw new NotSupportedException(
                        $"The given 'Options.IdType' value is not supported: {options.IdType:G}");
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _guidDispatcher?.Dispose();
            _longDispatcher?.Dispose();
        }

        /// <summary>
        /// Gets the options for the in-memory storage. 
        /// </summary>
        public InMemoryStorageOptions Options { get; }

        /// <summary>
        /// Override of <see cref="LinearizableReads"/> property. Always returns true for <see cref="InMemoryStorage"/>.
        /// </summary>
        public override bool LinearizableReads => true;

        /// <inheritdoc />
        public override bool HasFeature(string featureId)
        {
            if (featureId == null) throw new ArgumentNullException(nameof(featureId));

            return _features.TryGetValue(featureId, out var isSupported) 
                ? isSupported
                : base.HasFeature(featureId);
        }

        /// <inheritdoc />
        public override IMonitoringApi GetMonitoringApi()
        {
            if (_guidDispatcher != null)
            {
                return new InMemoryMonitoringApi<Guid>(_guidDispatcher, this);
            }

            if (_longDispatcher != null)
            {
                return new InMemoryMonitoringApi<ulong>(_longDispatcher, this);
            }

            throw new InvalidOperationException("Can not determine the dispatcher.");
        }

        /// <inheritdoc />
        public override IStorageConnection GetConnection()
        {
            if (_guidDispatcher != null)
            {
                return new InMemoryConnection<Guid>(Options, _guidDispatcher, this);
            }

            if (_longDispatcher != null)
            {
                return new InMemoryConnection<ulong>(Options, _longDispatcher, this);
            }

            throw new InvalidOperationException("Can not determine the dispatcher.");
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "In-Memory Storage";
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
