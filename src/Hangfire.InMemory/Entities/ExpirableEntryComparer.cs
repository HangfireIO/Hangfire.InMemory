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
    internal sealed class ExpirableEntryComparer<T> : IComparer<IExpirableEntry<T>>
        where T : IComparable<T>
    {
        private readonly IComparer<T>? _comparer;

        public ExpirableEntryComparer(IComparer<T>? comparer)
        {
            _comparer = comparer;
        }

        public int Compare(IExpirableEntry<T> x, IExpirableEntry<T> y)
        {
            if (x == null) throw new ArgumentNullException(nameof(x));
            if (y == null) throw new ArgumentNullException(nameof(y));

            if (ReferenceEquals(x, y)) return 0;

            // Place nulls last just in case, because they will prevent expiration
            // manager from correctly running and stopping earlier, since it works
            // from first value until is higher than the current time.
            if (x.ExpireAt.HasValue && y.ExpireAt.HasValue)
            {
                var expirationCompare = x.ExpireAt.Value.CompareTo(y.ExpireAt.Value);
                if (expirationCompare != 0) return expirationCompare;
            }
            else if (!x.ExpireAt.HasValue && y.ExpireAt.HasValue)
            {
                return +1;
            }
            else if (!y.ExpireAt.HasValue && x.ExpireAt.HasValue)
            {
                return -1;
            }

            return _comparer?.Compare(x.Key, y.Key) ?? x.Key.CompareTo(y.Key);
        }
    }
}
