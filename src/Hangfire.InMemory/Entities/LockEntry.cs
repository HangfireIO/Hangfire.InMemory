namespace Hangfire.InMemory.Entities
{
    internal sealed class LockEntry
    {
        public InMemoryConnection Owner { get; set; }
        public int ReferenceCount { get; set; }
        public int Level { get; set; }
    }
}