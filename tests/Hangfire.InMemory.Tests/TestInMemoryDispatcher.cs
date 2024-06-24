using System;
using Hangfire.InMemory.State;

namespace Hangfire.InMemory.Tests
{
    internal sealed class TestInMemoryDispatcher<TKey> : DispatcherBase<TKey>
        where TKey : IComparable<TKey>
    {
        public TestInMemoryDispatcher(Func<MonotonicTime> timeResolver, MemoryState<TKey> state) : base(timeResolver, state)
        {
        }

        public new void EvictExpiredEntries()
        {
            base.EvictExpiredEntries();
        }
    }
}