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
using Hangfire.InMemory.State;
using Hangfire.Storage;

namespace Hangfire.InMemory.Entities
{
    internal sealed class JobEntry<T> : IExpirableEntry<T>
    {
        private StateRecord[] _history = [];
        private KeyValuePair<string, string>[] _parameters;

        public JobEntry(
            T key,
            InvocationData data,
            KeyValuePair<string, string>[] parameters,
            MonotonicTime createdAt)
        {
            Key = key;
            InvocationData = data;
            CreatedAt = createdAt;

            _parameters = parameters;
        }

        public T Key { get; }
        public InvocationData InvocationData { get; internal set; }

        public StateRecord? State { get; set; }
        public IEnumerable<StateRecord> History => _history;
        public MonotonicTime CreatedAt { get; }
        public MonotonicTime? ExpireAt { get; set; }

        public string? GetParameter(string name, StringComparer comparer)
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
            
            for (var i = 0; i < _parameters.Length; i++)
            {
                if (comparer.Compare(_parameters[i].Key, name) == 0)
                {
                    _parameters[i] = parameter;
                    return;
                }
            }

            Array.Resize(ref _parameters, _parameters.Length + 1);
            _parameters[_parameters.Length - 1] = parameter;
        }

        public KeyValuePair<string, string>[] GetParameters()
        {
            return _parameters;
        }

        public void AddHistoryEntry(StateRecord record, int maxLength)
        {
            if (record == null) throw new ArgumentNullException(nameof(record));
            if (maxLength <= 0) throw new ArgumentOutOfRangeException(nameof(maxLength));

            if (_history.Length < maxLength)
            {
                Array.Resize(ref _history, _history.Length + 1);
            }
            else
            {
                Array.Copy(_history, 1, _history, 0, _history.Length - 1);
            }

            _history[_history.Length - 1] = record;
        }
    }
}