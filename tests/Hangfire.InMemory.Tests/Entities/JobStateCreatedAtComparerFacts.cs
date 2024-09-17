using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.InMemory.Entities;
using Hangfire.InMemory.State;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.InMemory.Tests.Entities
{
    public class JobStateCreatedAtComparerFacts
    {
        private readonly StringComparer _stringComparer = StringComparer.Ordinal;
        private readonly InvocationData _data = InvocationData.SerializeJob(Job.FromExpression(() => Console.WriteLine()));
        private readonly KeyValuePair<string, string>[] _parameters = [];

        [Fact]
        public void Ctor_DoesNotThrowAnException_WhenComparerIsNull()
        {
            var comparer = new JobStateCreatedAtComparer<string>(null);
            Assert.NotNull(comparer);
        }

        [Fact]
        public void Compare_ReturnsZero_WhenBothEntries_AreNull()
        {
            var comparer = CreateComparer();

            var result = comparer.Compare(null, null);
            
            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_ReturnsMinusOne_WhenXIsNull_AndYIsNotNull()
        {
            var comparer = CreateComparer();
            var entry = CreateEntry(null, MonotonicTime.GetCurrent(), null, null);

            var result = comparer.Compare(null, entry);

            Assert.Equal(-1, result);
        }

        [Fact]
        public void Compare_ReturnsPlusOne_WhenXIsNotNull_AndYIsNull()
        {
            var comparer = CreateComparer();
            var entry = CreateEntry(null, MonotonicTime.GetCurrent(), null, null);

            var result = comparer.Compare(entry, null);

            Assert.Equal(+1, result);
        }

        [Fact]
        public void Compare_ReturnsZero_WhenStateOfBothEntries_IsNull()
        {
            var comparer = CreateComparer();
            var x = CreateEntry(null, MonotonicTime.GetCurrent(), null, null);
            var y = CreateEntry(null, MonotonicTime.GetCurrent(), null, null);

            var result = comparer.Compare(x, y);
            
            Assert.Equal(0, result);
        }

        [Fact]
        public void Compare_ReturnsMinusOne_WhenStateOfXIsNull_AndStateOfYIsNotNull()
        {
            var comparer = CreateComparer();
            var x = CreateEntry(null, MonotonicTime.GetCurrent(), null, null);
            var y = CreateEntry(null, MonotonicTime.GetCurrent(), "State", MonotonicTime.GetCurrent());

            var result = comparer.Compare(x, y);

            Assert.Equal(-1, result);
        }

        [Fact]
        public void Compare_ReturnsPlusOne_WhenStateOfXIsNotNull_AndStateOfYIsNull()
        {
            var comparer = CreateComparer();
            var x = CreateEntry(null, MonotonicTime.GetCurrent(), "State", MonotonicTime.GetCurrent());
            var y = CreateEntry(null, MonotonicTime.GetCurrent(), null, null);

            var result = comparer.Compare(x, y);

            Assert.Equal(+1, result);
        }

        [Fact]
        public void Compare_LeadsToSorting_InTheAscendingOrder()
        {
            var now = MonotonicTime.GetCurrent();
            var array = new []
            {
                /* [0]: #6 */ CreateEntry("key", now.Add(TimeSpan.FromDays(1)), "State", now),
                /* [1]: #4 */ CreateEntry("key", now, "Another", now),
                /* [2]: #5 */ CreateEntry("key", now, "State", now),
                /* [3]: #1 */ null,
                /* [4]: #2 */ CreateEntry("key", now, null, null),
                /* [5]: #7 */ CreateEntry("key", now, "State", now.Add(TimeSpan.FromDays(1))),
                /* [6]: #3 */ CreateEntry("another", now, "State", now)
            };

            var comparer = CreateComparer();
            var result = array.OrderBy(static x => x, comparer).ToArray();

            Assert.Equal([array[3], array[4], array[6], array[1], array[2], array[0], array[5]], result);
        }

        private JobEntry<string> CreateEntry(string key, MonotonicTime createdAt, string state, MonotonicTime? stateCreatedAt)
        {
            var entry = new JobEntry<string>(key, _data, _parameters, createdAt);

            if (state != null)
            {
                if (stateCreatedAt == null) throw new ArgumentNullException(nameof(stateCreatedAt));
                entry.State = new StateRecord(state, null, [], stateCreatedAt.Value);
            }

            return entry;
        }

        private JobStateCreatedAtComparer<string> CreateComparer()
        {
            return new JobStateCreatedAtComparer<string>(_stringComparer);
        }
    }
}