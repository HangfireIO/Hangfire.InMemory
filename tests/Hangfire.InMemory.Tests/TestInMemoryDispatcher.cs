using System;

namespace Hangfire.InMemory.Tests
{
    internal class TestInMemoryDispatcher : InMemoryDispatcherBase
    {
        public TestInMemoryDispatcher(Func<MonotonicTime> timeResolver, InMemoryState state) : base(timeResolver, state)
        {
        }

        public new void EvictExpiredEntries()
        {
            base.EvictExpiredEntries();
        }
    }
}