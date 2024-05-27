using System.Diagnostics.CodeAnalysis;

namespace Hangfire.InMemory
{
    /// <summary>
    /// Represents the type using for storing background job identifiers.
    /// </summary>
    [SuppressMessage("Naming", "CA1720:Identifier contains type name", Justification = "This is intentionally, by design.")]
    public enum InMemoryStorageIdType
    {
        /// <summary>
        /// Background job identifiers will be integer-based as in Hangfire.SqlServer storage.
        /// </summary>
        UInt64,

        /// <summary>
        /// Background job identifiers will be Guid-based like in Hangfire.Pro.Redis storage.
        /// </summary>
        Guid,
    }
}