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

namespace Hangfire.InMemory.Entities
{
    internal sealed class HashEntry : IExpirableEntry<string>
    {
        public HashEntry(string id, StringComparer comparer)
        {
            Key = id;
            Value = new SortedDictionary<string, string>(comparer);
        }

        public string Key { get; }
        public IDictionary<string, string> Value { get; }
        public MonotonicTime? ExpireAt { get; set; }
    }
}