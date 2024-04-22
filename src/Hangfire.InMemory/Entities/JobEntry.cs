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
        private readonly List<KeyValuePair<string, string>> _parameters = new List<KeyValuePair<string, string>>();

        public JobEntry(
            string key,
            InvocationData data,
            IDictionary<string, string> parameters,
            MonotonicTime createdAt)
        {
            Key = key;
            InvocationData = data;
            CreatedAt = createdAt;

            foreach (var parameter in parameters)
            {
                _parameters.Add(parameter);
            }
        }

        public string Key { get; }
        public InvocationData InvocationData { get; internal set; }

        public StateEntry State { get; set; }
        public IEnumerable<StateEntry> History => _history;
        public MonotonicTime CreatedAt { get; }
        public MonotonicTime? ExpireAt { get; set; }

        public string GetParameter(string name, StringComparer comparer)
        {
            foreach (var parameter in _parameters)
            {
                if (comparer.Compare(parameter.Key, name) == 0)
                {
                    return parameter.Value;
                }
            }

            return null;
        }

        public void SetParameter(string name, string value, StringComparer comparer)
        {
            var parameter = new KeyValuePair<string, string>(name, value);
            
            for (var i = 0; i < _parameters.Count; i++)
            {
                if (comparer.Compare(_parameters[i].Key, name) == 0)
                {
                    _parameters[i] = parameter;
                    return;
                }
            }

            _parameters.Add(parameter);
        }

        public IReadOnlyDictionary<string, string> GetParametersSnapshot(StringComparer comparer)
        {
            var result = new Dictionary<string, string>(capacity: _parameters.Count, comparer);

            foreach (var parameter in _parameters)
            {
                result.Add(parameter.Key, parameter.Value);
            }

            return result;
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