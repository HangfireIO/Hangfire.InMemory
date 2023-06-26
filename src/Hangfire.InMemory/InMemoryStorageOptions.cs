using System;

namespace Hangfire.InMemory
{
    public class InMemoryStorageOptions
    {
        public bool DisableJobSerialization { get; set; }
        // TODO: Rename it to EnableSerialization and disable by default? Remember that Args can be substituted at runtime
        // TODO: int MaxStateHistoryLength to prevent infinite growth of state entries (10 or 100 by default)
        // TODO: bool ExpireKeysOnGen2Collection (defaults to true). Implement key expiration regardless of actual expiration value on Gen2 collections and make this default.
        // https://github.com/dotnet/runtime/blob/36439c510b779103a4a8066359d0d63dc003eed3/src/libraries/System.Private.CoreLib/src/System/Gen2GcCallback.cs

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