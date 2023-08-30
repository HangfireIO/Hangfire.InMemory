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
    internal sealed class ExpirableEntryComparer : IComparer<IExpirableEntry>
    {
        private readonly StringComparer _stringComparer;

        public ExpirableEntryComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(IExpirableEntry x, IExpirableEntry y)
        {
            if (ReferenceEquals(x, y)) return 0;

            // TODO: Nulls last, our indexes shouldn't contain nulls anyway
            // TODO: Check this
            if (x == null) return -1;
            if (y == null) return +1;

            if (!x.ExpireAt.HasValue)
            {
                throw new InvalidOperationException("Left side does not contain ExpireAt value");
            }

            if (!y.ExpireAt.HasValue)
            {
                throw new InvalidOperationException("Right side does not contain ExpireAt value");
            }

            var expirationCompare = x.ExpireAt.Value.CompareTo(y.ExpireAt.Value);
            if (expirationCompare != 0) return expirationCompare;

            return _stringComparer.Compare(x.Key, y.Key);
        }
    }
}
