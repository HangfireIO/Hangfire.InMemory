namespace Hangfire.InMemory.Tests
{
    internal class TestInMemoryDispatcher(InMemoryState state) : InMemoryDispatcherBase(state)
    {
        public new void EvictEntries()
        {
            base.EvictEntries();
        }
    }
}