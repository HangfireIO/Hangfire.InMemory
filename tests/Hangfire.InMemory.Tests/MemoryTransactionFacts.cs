using System;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class MemoryTransactionFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenDispatcherIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MemoryTransaction(null));
            Assert.Equal("dispatcher", exception.ParamName);
        }
    }
}
