using System;

namespace Hangfire.InMemory.Tests
{
    internal class TestInMemoryDispatcher<TKey> : InMemoryDispatcherBase<TKey>
        where TKey : IComparable<TKey>
    {
        public TestInMemoryDispatcher(Func<MonotonicTime> timeResolver, InMemoryState<TKey> state) : base(timeResolver, state)
        {
        }

        public new void EvictExpiredEntries()
        {
            base.EvictExpiredEntries();
        }
    }
}