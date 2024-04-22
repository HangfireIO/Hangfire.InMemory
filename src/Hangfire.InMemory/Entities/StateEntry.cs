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

namespace Hangfire.InMemory.Entities
{
    internal sealed class StateEntry
    {
        private readonly KeyValuePair<string, string>[] _data;

        public StateEntry(string name, string reason, IDictionary<string, string> data, MonotonicTime createdAt, StringComparer comparer)
        {
            Name = name;
            Reason = reason;
            CreatedAt = createdAt;

            if (data != null)
            {
                _data = new KeyValuePair<string, string>[data.Count];

                var index = 0;

                foreach (var item in data)
                {
                    _data[index++] = item;
                }
            }
        }

        public string Name { get; }
        public string Reason { get; }
        public MonotonicTime CreatedAt { get; }

        public IDictionary<string, string> GetData(StringComparer comparer)
        {
            if (_data == null) return null;

            var result = new Dictionary<string, string>(capacity: _data.Length, comparer);

            foreach (var item in _data)
            {
                result.Add(item.Key, item.Value);
            }

            return result;
        }
    }
}