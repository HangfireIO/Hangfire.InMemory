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

namespace Hangfire.InMemory
{
    /// <summary>
    /// Provides configuration options for in-memory storage in Hangfire.
    /// </summary>
    public class InMemoryStorageOptions
    {
        private int _maxStateHistoryLength = 10;

        // TODO: Rename it to EnableSerialization and disable by default? Remember that Args can be substituted at runtime
        /// <summary>
        /// Gets or sets a value indicating whether job serialization should be disabled.
        /// </summary>
        public bool DisableJobSerialization { get; set; }

        /// <summary>
        /// Gets or sets the maximum expiration time for all the entries. When set, this
        /// value overrides any expiration time set in the other places of Hangfire. The
        /// main rationale for this is to control the amount of consumed RAM, since we are
        /// more limited in this case, especially when comparing to disk-based storages.
        /// </summary>
        public TimeSpan? MaxExpirationTime { get; set; } = TimeSpan.FromHours(2);

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
    }
}
