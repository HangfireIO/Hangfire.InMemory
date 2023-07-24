using System;
using System.Collections.Generic;

namespace Hangfire.InMemory.Entities
{
    internal sealed class SortedSetItemComparer : IComparer<SortedSetItem>
    {
        private readonly StringComparer _stringComparer;

        public SortedSetItemComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(SortedSetItem x, SortedSetItem y)
        {
            var scoreComparison = x.Score.CompareTo(y.Score);
            if (scoreComparison != 0 ||
                ReferenceEquals(null, y.Value) ||
                ReferenceEquals(null, x.Value))
            {
                return scoreComparison;
            }

            return _stringComparer.Compare(x.Value, y.Value);
        }
    }
}