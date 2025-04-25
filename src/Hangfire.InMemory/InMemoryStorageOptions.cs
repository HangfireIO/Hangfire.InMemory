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
using System.Threading;

namespace Hangfire.InMemory
{
    /// <summary>
    /// Provides configuration options for in-memory storage in Hangfire.
    /// </summary>
    public sealed class InMemoryStorageOptions
    {
        private int _maxStateHistoryLength = 10;

        /// <summary>
        /// Gets or sets the underlying key type for background jobs that can be useful
        /// to simulate different persistent storages.
        /// </summary>
        public InMemoryStorageIdType IdType { get; set; } = InMemoryStorageIdType.Long;

        /// <summary>
        /// Gets or sets the maximum expiration time for all the entries. When set, this
        /// value overrides any expiration time set in the other places of Hangfire. The
        /// main rationale for this is to control the amount of consumed RAM, since we are
        /// more limited in this case, especially when compared to disk-based storages.
        /// </summary>
        public TimeSpan? MaxExpirationTime { get; set; } = TimeSpan.FromHours(3);

        /// <summary>
        /// Gets or sets the maximum length of state history for each background job. Older
        /// records are trimmed to avoid uncontrollable growth when some background job is
        /// constantly moved from one state to another without being completed.
        /// </summary>
        public int MaxStateHistoryLength
        {
            get => _maxStateHistoryLength;
            set
            {
                if (value <= 0) throw new ArgumentOutOfRangeException(nameof(value), "Value is out of range. Must be greater than zero.");
                _maxStateHistoryLength = value;
            }
        }

        /// <summary>
        /// Gets or sets comparison rules for keys and indexes inside the storage. You can use
        /// this option to match semantics of different storages, for example, use the
        /// <see cref="StringComparer.Ordinal"/> value to match Redis' case-sensitive rules,
        /// or use the <see cref="StringComparer.OrdinalIgnoreCase"/> option to match SQL Server's
        /// default case-insensitive rules.
        /// </summary>
        public StringComparer StringComparer { get; set; } = StringComparer.Ordinal;

        /// <summary>
        /// Gets or sets the maximum time to wait for a command completion.
        /// </summary>
        public TimeSpan CommandTimeout { get; set; } = System.Diagnostics.Debugger.IsAttached ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(15);
    }
}
