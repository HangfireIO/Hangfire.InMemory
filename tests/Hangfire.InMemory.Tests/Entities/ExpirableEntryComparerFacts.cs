// This file is part of Hangfire.InMemory. Copyright © 2023 Hangfire OÜ.
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
using Hangfire.InMemory.State;
using Xunit;

namespace Hangfire.InMemory.Tests.Entities
{
    public class ExpirableEntryComparerFacts
    {
        private readonly StringComparer _stringComparer = StringComparer.Ordinal;

        [Fact]
        public void Ctor_DoesNotThrowAnException_WhenComparerIsNull()
        {
            var comparer = new ExpirableEntryComparer<string>(null);
            Assert.NotNull(comparer);
        }

        [Fact]
        public void Compare_ThrowsAnException_WhenXIsNull()
        {
            var comparer = CreateComparer();
            var exception = Assert.Throws<ArgumentNullException>(
                () => comparer.Compare(null, new ExpirableEntryStub<string>(null, null)));

            Assert.Equal("x", exception.ParamName);
        }

        [Fact]
        public void Compare_ThrowsAnException_WhenYIsNull()
        {
            var comparer = CreateComparer();
            var exception = Assert.Throws<ArgumentNullException>(
                () => comparer.Compare(new ExpirableEntryStub<string>(null, null), null));

            Assert.Equal("y", exception.ParamName);
        }

        [Fact]
        public void Compare_ReturnsZero_ForSameEntries_WithNullExpireAt()
        {
            var comparer = CreateComparer();
            var entry = new ExpirableEntryStub<string>(null, null);

            var result = comparer.Compare(entry, entry);

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_ReturnsZero_ForSameEntries_WithNonNullExpireAt()
        {
            var comparer = CreateComparer();
            var entry = new ExpirableEntryStub<string>("key", MonotonicTime.GetCurrent());

            var result = comparer.Compare(entry, entry);

            Assert.Equal(0, result);
        }
        
        [Fact]
        public void Compare_ReturnsZero_ForEntries_WithTheSameKey_AndSameNonNullExpireAt()
        {
            var comparer = CreateComparer();
            var now = MonotonicTime.GetCurrent();

            var result = comparer.Compare(
                new ExpirableEntryStub<string>("key", now),
                new ExpirableEntryStub<string>("key", now));

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_ReturnsZero_ForEntries_WithNullKey_AndSameNonNullExpireAt()
        {
            var comparer = CreateComparer();
            var now = MonotonicTime.GetCurrent();

            var result = comparer.Compare(
                new ExpirableEntryStub<string>(null, now),
                new ExpirableEntryStub<string>(null, now));

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_ReturnsZero_ForEntries_WithTheSameNonNullKey_AndNullExpireAt()
        {
            var comparer = CreateComparer();

            var result = comparer.Compare(
                new ExpirableEntryStub<string>("key", null),
                new ExpirableEntryStub<string>("key", null));

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_ReturnsZero_ForEntries_WithNullKey_AndNullExpireAt()
        {
            var comparer = CreateComparer();

            var result = comparer.Compare(
                new ExpirableEntryStub<string>(null, null),
                new ExpirableEntryStub<string>(null, null));

            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_ReturnsPlusOne_WhenXIsNull_AndYIsNot()
        {
            var comparer = CreateComparer();
            var now = MonotonicTime.GetCurrent();
            var x = new ExpirableEntryStub<string>("key-1", null);

            Assert.Equal(-1, _stringComparer.Compare("key-1", "key-2")); // Just to check
            Assert.Equal(+1, comparer.Compare(x, new ExpirableEntryStub<string>("key-2", now)));
            Assert.Equal(+1, comparer.Compare(x, new ExpirableEntryStub<string>("key-1", now)));
            Assert.Equal(+1, comparer.Compare(x, new ExpirableEntryStub<string>(null, now)));
        }

        [Fact]
        public void Compare_ReturnsPlusOne_WhenXExpireAt_IsGreaterThan_YExpireAt()
        {
            var comparer = CreateComparer();
            var now = MonotonicTime.GetCurrent();
            var x = new ExpirableEntryStub<string>("key", now);
            
            Assert.Equal(+1, comparer.Compare(x, new ExpirableEntryStub<string>("key", now.Add(TimeSpan.FromSeconds(-1)))));
        }

        [Fact]
        public void Compare_ReturnsMinusOne_WhenYIsNull_AndXIsNot()
        {
            var comparer = CreateComparer();
            var now = MonotonicTime.GetCurrent();
            var y = new ExpirableEntryStub<string>("key-1", null);

            Assert.Equal(+1, _stringComparer.Compare("key-2", "key-1")); // Just to check
            Assert.Equal(-1, comparer.Compare(new ExpirableEntryStub<string>("key-2", now), y));
            Assert.Equal(-1, comparer.Compare(new ExpirableEntryStub<string>("key-1", now), y));
            Assert.Equal(-1, comparer.Compare(new ExpirableEntryStub<string>(null, now), y));
        }

        [Fact]
        public void Compare_ReturnsMinusOne_WhenXExpireAt_IsLessThan_YExpireAt()
        {
            var comparer = CreateComparer();
            var now = MonotonicTime.GetCurrent();
            var x = new ExpirableEntryStub<string>("key", now);
            
            Assert.Equal(-1, comparer.Compare(x, new ExpirableEntryStub<string>("key", now.Add(TimeSpan.FromSeconds(1)))));
        }

        [Fact]
        public void Compare_FallsBackToComparer_WhenItPassed_WhenExpireAtAreEqual_OrNull()
        {
            var comparer = CreateComparer();
            var now = MonotonicTime.GetCurrent();
            
            Assert.Equal(-1, comparer.Compare(
                new ExpirableEntryStub<string>("key-1", now),
                new ExpirableEntryStub<string>("key-2", now)));

            Assert.Equal(+1, comparer.Compare(
                new ExpirableEntryStub<string>("key-2", now),
                new ExpirableEntryStub<string>("key-1", now)));

            Assert.Equal(-1, comparer.Compare(
                new ExpirableEntryStub<string>("key-1", null),
                new ExpirableEntryStub<string>("key-2", null)));

            Assert.Equal(+1, comparer.Compare(
                new ExpirableEntryStub<string>("key-2", null),
                new ExpirableEntryStub<string>("key-1", null)));
        }

        [Fact]
        public void Compare_FallsBackToEntities_WhenItPassed_WhenExpireAtAreEqual_OrNull()
        {
            var comparer = new ExpirableEntryComparer<long>(null);
            var now = MonotonicTime.GetCurrent();
            
            Assert.Equal(-1, comparer.Compare(
                new ExpirableEntryStub<long>(1, now),
                new ExpirableEntryStub<long>(2, now)));

            Assert.Equal(+1, comparer.Compare(
                new ExpirableEntryStub<long>(2, now),
                new ExpirableEntryStub<long>(1, now)));

            Assert.Equal(-1, comparer.Compare(
                new ExpirableEntryStub<long>(1, null),
                new ExpirableEntryStub<long>(2, null)));

            Assert.Equal(+1, comparer.Compare(
                new ExpirableEntryStub<long>(2, null),
                new ExpirableEntryStub<long>(1, null)));
        }

        [Fact]
        public void Compare_LeadsToSorting_InTheAscendingOrder_OfExpireAtValues()
        {
            var now = MonotonicTime.GetCurrent();
            var array = new []
            {
                new ExpirableEntryStub<string>("key", now),
                new ExpirableEntryStub<string>("key", now.Add(TimeSpan.FromHours(-1))),
                new ExpirableEntryStub<string>("key", now.Add(TimeSpan.FromDays(1)))
            };

            var comparer = CreateComparer();
            var result = array.OrderBy(x => x, comparer).ToArray();

            Assert.Equal(new [] { array[1], array[0], array[2] }, result);
        }

        [Fact]
        public void Compare_PlacesNullsLast_WhenSortingByExpireAtValues()
        {
            var now = MonotonicTime.GetCurrent();
            var array = new[]
            {
                new ExpirableEntryStub<string>("key", null),
                new ExpirableEntryStub<string>("key", now),
                new ExpirableEntryStub<string>("key", now.Add(TimeSpan.FromSeconds(1)))
            };

            var comparer = CreateComparer();
            var result = array.OrderBy(x => x, comparer).ToArray();

            Assert.Equal(new [] { array[1], array[2], array[0] }, result);
        }

        private ExpirableEntryComparer<string> CreateComparer()
        {
            return new ExpirableEntryComparer<string>(_stringComparer);
        }

        private sealed class ExpirableEntryStub<T> : IExpirableEntry<T>
        {
            public ExpirableEntryStub(T key, MonotonicTime? expireAt)
            {
                Key = key;
                ExpireAt = expireAt;
            }

            public T Key { get; }
            public MonotonicTime? ExpireAt { get; set; }
        }
    }
}