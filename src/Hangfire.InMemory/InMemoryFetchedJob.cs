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
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal class InMemoryFetchedJob<TKey> : IFetchedJob
        where TKey : IComparable<TKey>
    {
        private readonly InMemoryDispatcherBase<TKey> _dispatcher;

        public InMemoryFetchedJob(
            [NotNull] InMemoryDispatcherBase<TKey> dispatcher,
            [NotNull] string queueName,
            [NotNull] string jobId)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));

            QueueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
        }

        public string QueueName { get; }
        public string JobId { get; }

        public void Requeue()
        {
            var entry = _dispatcher.QueryWriteAndWait(state =>
            {
                var value = state.QueueGetOrCreate(QueueName);
                value.Queue.Enqueue(JobId);
                return value;
            });

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
