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
        private readonly Dictionary<string, string> _data;

        public StateEntry(string name, string reason, IDictionary<string, string> data, DateTime createdAt, StringComparer comparer)
        {
            _data = data != null ? new Dictionary<string, string>(data, comparer) : null;
            Name = name;
            Reason = reason;
            CreatedAt = createdAt;
        }

        public string Name { get; }
        public string Reason { get; }
        public DateTime CreatedAt { get; }

        public IDictionary<string, string> GetData()
        {
            return _data != null 
                ? new Dictionary<string, string>(_data, _data.Comparer)
                : null;
        }
    }
}