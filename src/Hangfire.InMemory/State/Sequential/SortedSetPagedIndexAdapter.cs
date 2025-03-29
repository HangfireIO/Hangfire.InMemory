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

using System.Collections.Generic;

namespace Hangfire.InMemory.State.Sequential
{
    internal sealed class SortedSetPagedIndexAdapter<T> : IPagedIndex<T>
    {
        public SortedSet<T> SortedSet { get; } = new SortedSet<T>();

        public IReadOnlyCollection<T> GetPage(int from, int count, bool reverse)
        {
            var result = new List<T>();
            var index = 0;
            var collection = reverse ? SortedSet.Reverse() : SortedSet;

            foreach (var entry in collection)
            {
                if (index < from) { index++; continue; }
                if (index >= from + count) break;

                result.Add(entry);
                index++;
            }

            return result;
        }

        long IPagedIndex<T>.GetCount()
        {
            return SortedSet.Count;
        }
    }
}