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
    internal sealed class JobEntry : IExpirableEntry
    {
        private const int StateCountForRegularJob = 4; // (Scheduled) -> Enqueued -> Processing -> Succeeded
        private readonly List<StateEntry> _history = new(StateCountForRegularJob);
        private readonly Job _job;

        public JobEntry(
            string key,
            Job job,
            IDictionary<string, string> parameters,
            MonotonicTime createdAt,
            bool disableSerialization,
            StringComparer comparer)
        {
            Key = key;
            InvocationData = disableSerialization == false ? InvocationData.SerializeJob(job) : null;
            _job = disableSerialization ? new Job(job.Type, job.Method, job.Args.ToArray(), job.Queue) : null;
            Parameters = new ConcurrentDictionary<string, string>(concurrencyLevel: 1, parameters, comparer);
            CreatedAt = createdAt;
#if NET451
            Comparer = comparer;
#endif
        }

        public string Key { get; }
        public InvocationData InvocationData { get; internal set; }

        public ConcurrentDictionary<string, string> Parameters { get; }

        public StateEntry State { get; set; }
        public IEnumerable<StateEntry> History => _history;
        public MonotonicTime CreatedAt { get; }
        public MonotonicTime? ExpireAt { get; set; }

#if NET451
        public StringComparer Comparer { get; }
#endif

        public Job TryGetJob(out JobLoadException exception)
        {
            exception = null;

            if (_job != null)
            {
                return new Job(_job.Type, _job.Method, _job.Args.ToArray(), _job.Queue);
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

            if (_history.Count < maxLength)
            {
                _history.Add(entry);
            }
            else
            {
                for (var i = 0; i < _history.Count - 1; i++)
                {
                    _history[i] = _history[i + 1];
                }

                _history[_history.Count - 1] = entry;
            }
        }
    }
}