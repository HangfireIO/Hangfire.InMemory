using System;
using Moq;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryTransactionFacts
    {
        private readonly Mock<InMemoryDispatcherBase> _dispatcher;
        private readonly InMemoryState _state;
        private readonly DateTime _now;

        public InMemoryTransactionFacts()
        {
            _dispatcher = new Mock<InMemoryDispatcherBase>();
            _now = new DateTime(2020, 09, 29, 08, 05, 30, DateTimeKind.Utc);
            _state = new InMemoryState(() => _now);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDispatcherIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new InMemoryTransaction(null));
            Assert.Equal("dispatcher", exception.ParamName);
        }

        [Fact]
        public void ExpireJob_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.ExpireJob(null, TimeSpan.Zero)));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void ExpireJob_DoesNotThrow_WhenJobDoesNotExist()
        {
            Commit(x => x.ExpireJob("some-job", TimeSpan.Zero));
        }

        [Fact]
        public void ExpireJob_SetsExpirationTime_ForAJob_ToAnExpectedValue()
        {
            // Arrange
            _state.Jobs.TryAdd("myjob", new BackgroundJobEntry());

            // Act
            Commit(x => x.ExpireJob("myjob", TimeSpan.FromMinutes(30)));

            // Assert
            var expireAt = _state.Jobs["myjob"].ExpireAt;
            Assert.NotNull(expireAt);
            Assert.Equal(_now.AddMinutes(30), expireAt.Value);
        }

        private void Commit(Action<InMemoryTransaction> action)
        {
            var transaction = new InMemoryTransaction(new InMemoryDispatcherBase(_state));
            action(transaction);

            transaction.Commit();
        }
    }
}
