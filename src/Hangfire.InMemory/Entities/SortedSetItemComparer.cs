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
    internal sealed class SortedSetItemComparer : IComparer<SortedSetItem>
    {
        private readonly StringComparer _stringComparer;

        public SortedSetItemComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(SortedSetItem x, SortedSetItem y)
        {
            // TODO: Add unit tests for all the comparers in project
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