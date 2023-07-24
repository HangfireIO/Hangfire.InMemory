using System;
using System.Collections.Generic;

namespace Hangfire.InMemory.Entities
{
    internal sealed class SortedSetEntryComparer : IComparer<SortedSetEntry>
    {
        private readonly StringComparer _stringComparer;

        public SortedSetEntryComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(SortedSetEntry x, SortedSetEntry y)
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