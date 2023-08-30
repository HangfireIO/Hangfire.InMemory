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
    internal sealed class ListEntry : IExpirableEntry
    {
        private readonly StringComparer _stringComparer;
        private List<string> _value = new List<string>();

        public ListEntry(string id, StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
            Key = id;
        }

        public string Key { get; }
        public DateTime? ExpireAt { get; set; }

        public int Count => _value.Count;

        public string this[int index] => _value[Count - index - 1];

        public void Add(string value)
        {
            _value.Add(value);
        }

        public void RemoveAll(string value)
        {
            _value.RemoveAll(other => _stringComparer.Equals(value, other));
        }

        public int Trim(int keepStartingFrom, int keepEndingAt)
        {
            var from = Math.Max(0, keepStartingFrom);
            var to = Math.Min(_value.Count - 1, keepEndingAt);

            var result = new List<string>();

            for (var index = to; index >= from; index--)
            {
                result.Add(this[index]);
            }

            _value = result;
            return _value.Count;
        }
    }
}