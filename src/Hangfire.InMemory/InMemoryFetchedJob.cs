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
using Hangfire.Annotations;
using Hangfire.InMemory.Entities;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal class InMemoryFetchedJob<TKey> : IFetchedJob
        where TKey : IComparable<TKey>
    {
        private readonly InMemoryConnection<TKey> _connection;

        public InMemoryFetchedJob(
            [NotNull] InMemoryConnection<TKey> connection,
            [NotNull] string queueName,
            [NotNull] string jobId)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));

            QueueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
        }

        public string QueueName { get; }
        public string JobId { get; }

        public void Requeue()
        {
            if (!_connection.KeyProvider.TryParse(JobId, out var key))
            {
                return;
            }

            var entry = _connection.Dispatcher.QueryWriteAndWait(
                new InMemoryCommands.QueueEnqueue<TKey>(QueueName, key, null));

            entry.SignalOneWaitNode();
        }

        void IDisposable.Dispose()
        {
        }

        void IFetchedJob.RemoveFromQueue()
        {
        }
    }
}
