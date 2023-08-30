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
    internal sealed class JobStateCreatedAtComparer : IComparer<JobEntry>
    {
        private readonly StringComparer _stringComparer;

        public JobStateCreatedAtComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(JobEntry x, JobEntry y)
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