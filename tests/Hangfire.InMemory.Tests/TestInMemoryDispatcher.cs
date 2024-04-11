namespace Hangfire.InMemory.Tests
{
    internal class TestInMemoryDispatcher : InMemoryDispatcherBase
    {
        public TestInMemoryDispatcher(InMemoryState state) : base(state)
        {
        }

        public new void EvictEntries()
        {
            base.EvictEntries();
        }
    }
}