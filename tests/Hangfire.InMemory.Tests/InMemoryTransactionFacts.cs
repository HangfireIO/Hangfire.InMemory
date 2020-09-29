using System;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryTransactionFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenDispatcherIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new InMemoryTransaction(null));
            Assert.Equal("dispatcher", exception.ParamName);
        }
    }
}
