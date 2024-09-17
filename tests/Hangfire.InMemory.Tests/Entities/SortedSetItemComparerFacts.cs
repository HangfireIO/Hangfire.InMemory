// This file is part of Hangfire.InMemory. Copyright © 2024 Hangfire OÜ.
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
using System.Linq;
using Hangfire.InMemory.Entities;
using Xunit;

namespace Hangfire.InMemory.Tests.Entities
{
    public class SortedSetItemComparerFacts
    {
        [Fact]
        public void Compare_ReturnsZero_WhenSortingDefaultItems()
        {
            var comparer = CreateComparer();
            var result = comparer.Compare(default, default);

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_DoesNotTakeValueIntoAccount_WhenItIsNullForX()
        {
            var comparer = CreateComparer();

            var result = comparer.Compare(new SortedSetItem(null!, 0.5D), new SortedSetItem("y", 0.5D));

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_DoesNotTakeValueIntoAccount_WhenItIsNullForY()
        {
            var comparer = CreateComparer();

            var result = comparer.Compare(new SortedSetItem("x", 0.5D), new SortedSetItem(null!, 0.5D));

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_LeadsToSorting_InTheAscendingOrder()
        {
            var array = new []
            {
                /* [0]: #4 */ new SortedSetItem("1", 1.0D),
                /* [1]: #5 */ new SortedSetItem(null!, 1.0D),
                /* [2]: #2 */ new SortedSetItem("2", 0.5D),
                /* [3]: #3 */ new SortedSetItem(null!, 0.5D),
                /* [4]: #6 */ new SortedSetItem("2", 1.0D),
                /* [5]: #1 */ new SortedSetItem("1", 0.5D)
            };

            var comparer = CreateComparer();
            var result = array.OrderBy(static x => x, comparer).ToArray();

            Assert.Equal([array[5], array[2], array[3], array[0], array[1], array[4]], result);
        }

        private static SortedSetItemComparer CreateComparer()
        {
            return new SortedSetItemComparer(StringComparer.Ordinal);
        }
    }
}