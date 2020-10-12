using System;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryMonitoringApiFacts
    {
        private readonly InMemoryState _state;
        private readonly DateTime _now;

        public InMemoryMonitoringApiFacts()
        {
            _now = new DateTime(2020, 09, 29, 08, 05, 30, DateTimeKind.Utc);
            _state = new InMemoryState(() => _now);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDispatcherIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryMonitoringApi(null));
            
            Assert.Equal("dispatcher", exception.ParamName);
        }

        [Fact]
        public void Queues_ReturnsEmptyCollection_WhenThereAreNoQueues()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.Queues();

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        private InMemoryMonitoringApi CreateMonitoringApi()
        {
            return new InMemoryMonitoringApi(new InMemoryDispatcherBase(_state));
        }
    }
}
