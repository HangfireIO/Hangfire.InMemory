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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.InMemory.Entities
{
    internal sealed class BackgroundJobEntry : IExpirableEntry
    {
        private readonly LinkedList<StateEntry> _history = new LinkedList<StateEntry>();

        public BackgroundJobEntry(
            string key,
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            DateTime? expireAt,
            bool disableSerialization,
            StringComparer comparer)
        {
            Key = key;
            InvocationData = disableSerialization == false ? InvocationData.SerializeJob(job) : null;
            Job = disableSerialization ? new Job(job.Type, job.Method, job.Args.ToArray(), job.Queue) : null;
            Parameters = new ConcurrentDictionary<string, string>(parameters, comparer);
            CreatedAt = createdAt;
            ExpireAt = expireAt;
        }

        public string Key { get; }
        public InvocationData InvocationData { get; internal set; }
        public Job Job { get; }

        public ConcurrentDictionary<string, string> Parameters { get; }

        public StateEntry State { get; set; }
        public IEnumerable<StateEntry> History => _history;
        public DateTime CreatedAt { get; }
        public DateTime? ExpireAt { get; set; }

        public Job TryGetJob(out JobLoadException exception)
        {
            exception = null;

            if (Job != null)
            {
                return new Job(Job.Type, Job.Method, Job.Args.ToArray(), Job.Queue);
            }

            try
            {
                return InvocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                exception = ex;
                return null;
            }
        }

        public void AddHistoryEntry(StateEntry entry, int maxLength)
        {
            if (entry == null) throw new ArgumentNullException(nameof(entry));
            if (maxLength <= 0) throw new ArgumentOutOfRangeException(nameof(maxLength));

            _history.AddLast(entry);
            if (_history.Count > maxLength)
            {
                _history.RemoveFirst();
            }
        }
    }
}