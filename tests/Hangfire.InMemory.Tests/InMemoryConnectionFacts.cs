using System;
using System.Linq;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryConnectionFacts
    {
        private readonly DateTime _now;
        private readonly InMemoryState _state;

        public InMemoryConnectionFacts()
        {
            _now = new DateTime(2020, 09, 29, 08, 05, 30, DateTimeKind.Utc);
            _state = new InMemoryState(() => _now);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDispatcherIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryConnection(null));

            Assert.Equal("dispatcher", exception.ParamName);
        }

        [Fact]
        public void CreateWriteTransaction_ReturnsAWorkingInMemoryTransactionInstance()
        {
            using (var connection = CreateConnection())
            {
                var transaction = connection.CreateWriteTransaction();
                transaction.AddToSet("key", "value");

                transaction.Commit();

                Assert.Equal("value", _state.Sets["key"].Single().Value);
            }
        }

        private InMemoryConnection CreateConnection()
        {
            return new InMemoryConnection(new InMemoryDispatcherBase(_state));
        }
    }
}
