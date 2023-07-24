using System;
using System.Collections.Generic;

namespace Hangfire.InMemory.Entities
{
    internal sealed class BackgroundJobStateCreatedAtComparer : IComparer<BackgroundJobEntry>
    {
        private readonly StringComparer _stringComparer;

        public BackgroundJobStateCreatedAtComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(BackgroundJobEntry x, BackgroundJobEntry y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (y?.State == null) return 1;
            if (x?.State == null) return -1;

            var stateCreatedAtComparison = x.State.CreatedAt.CompareTo(y.State.CreatedAt);
            if (stateCreatedAtComparison != 0) return stateCreatedAtComparison;

            var createdAtComparison = x.CreatedAt.CompareTo(y.CreatedAt);
            if (createdAtComparison != 0) return createdAtComparison;

            return _stringComparer.Compare(x.Key, y.Key);
        }
    }
}