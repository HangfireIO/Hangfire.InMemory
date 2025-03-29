using System;
using Hangfire.InMemory.State;
using Hangfire.InMemory.State.Sequential;

namespace Hangfire.InMemory.Tests
{
    internal sealed class TestInMemoryDispatcher<TKey> : DispatcherBase<TKey, InMemoryConnection<TKey>>
        where TKey : IComparable<TKey>
    {
        public TestInMemoryDispatcher(Func<MonotonicTime> timeResolver, SequentialMemoryState<TKey> state) : base(timeResolver, state)
        {
        }

        public new void EvictExpiredEntries()
        {
            base.EvictExpiredEntries();
        }
    }
}