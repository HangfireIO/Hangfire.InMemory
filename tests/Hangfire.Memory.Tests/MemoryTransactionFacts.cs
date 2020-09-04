using System;
using Xunit;

namespace Hangfire.Memory.Tests
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
