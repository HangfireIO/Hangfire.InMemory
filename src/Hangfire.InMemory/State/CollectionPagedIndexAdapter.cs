// This file is part of Hangfire.InMemory. Copyright © 2025 Hangfire OÜ.
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.InMemory.State
{
    internal sealed class CollectionPagedIndexAdapter<T>(ICollection<T> collection) : IPagedIndex<T>
    {
        public IReadOnlyCollection<T> GetPage(int from, int count, bool reverse)
        {
            if (from == 0 && count == Int32.MaxValue && !reverse)
            {
                return new CollectionReadOnlyCollectionAdapter(collection);
            }

            var enumerable = reverse ? collection.Reverse() : collection;
            return enumerable.Skip(from).Take(count).ToList().AsReadOnly();
        }

        public long GetCount()
        {
            return collection.Count;
        }

        private sealed class CollectionReadOnlyCollectionAdapter(ICollection<T> collection) : IReadOnlyCollection<T>
        {
            public IEnumerator<T> GetEnumerator()
            {
                return collection.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public int Count => collection.Count;
        }
    }
}