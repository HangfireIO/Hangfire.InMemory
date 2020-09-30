using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.States;
using Moq;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryTransactionFacts
    {
        private readonly InMemoryState _state;
        private readonly DateTime _now;

        public InMemoryTransactionFacts()
        {
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
        public void ExpireJob_SetsExpirationTime_OfTheGivenJob()
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

        [Fact]
        public void ExpireJob_AddsEntry_ToExpirationIndex()
        {
            var entry = new BackgroundJobEntry();
            _state.Jobs.TryAdd("myjob", entry);

            Commit(x => x.ExpireJob("myjob", TimeSpan.FromMinutes(30)));

            Assert.Same(entry, _state._jobIndex.First());
        }

        [Fact]
        public void PersistJob_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.PersistJob(null)));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void PersistJob_DoesNotThrowAnException_WhenJobDoesNotExist()
        {
            Commit(x => x.PersistJob("some-job"));
        }

        [Fact]
        public void PersistJob_ResetsExpirationTime_OfTheGivenJob()
        {
            _state.Jobs.TryAdd("myjob", new BackgroundJobEntry { ExpireAt = _now });

            Commit(x => x.PersistJob("myjob"));

            Assert.Null(_state.Jobs["myjob"].ExpireAt);
        }

        [Fact]
        public void PersistJob_RemovesEntry_FromExpirationIndex()
        {
            // Arrange
            var entry = new BackgroundJobEntry { ExpireAt = _now };
            _state.Jobs.TryAdd("myjob", entry);
            _state._jobIndex.Add(entry);

            // Act
            Commit(x => x.PersistJob("myjob"));

            // Assert
            Assert.Empty(_state._jobIndex);
        }

        [Fact]
        public void SetJobState_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.SetJobState(null, new Mock<IState>().Object)));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void SetJobState_ThrowsAnException_WhenStateIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.SetJobState("myjob", null)));

            Assert.Equal("state", exception.ParamName);
        }

        [Fact]
        public void SetJobState_ThrowsAnException_WhenStateNameIsNull()
        {
            // Arrange
            var state = new Mock<IState>();
            state.SetupGet(x => x.Name).Returns((string)null);

            _state.Jobs.TryAdd("myjob", new BackgroundJobEntry());

            // Act
            var exception = Assert.Throws<ArgumentException>(() => Commit(
                x => x.SetJobState("myjob", state.Object)));

            // Assert
            Assert.Equal("state", exception.ParamName);
            Assert.Contains("Name property", exception.Message);
        }

        [Fact]
        public void SetJobState_DoesNotThrowAnException_WhenJobDoesNotExist()
        {
            var state = new Mock<IState>();
            state.SetupGet(x => x.Name).Returns("SomeState");

            Commit(x => x.SetJobState("some-job", state.Object));
        }

        [Fact]
        public void SetJobState_SetsStateEntry_OfTheGivenJob()
        {
            // Arrange
            var state = new Mock<IState>();
            state.SetupGet(x => x.Name).Returns("SomeState");
            state.SetupGet(x => x.Reason).Returns("SomeReason");
            state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string> {{ "Key", "Value" }});

            _state.Jobs.TryAdd("myjob", new BackgroundJobEntry());

            // Act
            Commit(x => x.SetJobState("myjob", state.Object));

            // Assert
            var entry = _state.Jobs["myjob"];

            Assert.NotNull(entry.State);
            Assert.Equal("SomeState", entry.State.Name);
            Assert.Equal("SomeReason", entry.State.Reason);
            Assert.Equal("Value", entry.State.Data["Key"]);
            Assert.Equal(_now, entry.State.CreatedAt);
        }

        [Fact]
        public void SetJobState_AppendsStateHistory_WithTheNewEntry()
        {
            // Arrange
            var state = new Mock<IState>();
            state.SetupGet(x => x.Name).Returns("SomeState");

            _state.Jobs.TryAdd("myjob", new BackgroundJobEntry());

            // Act
            Commit(x => x.SetJobState("myjob", state.Object));

            // Assert
            Assert.Equal("SomeState", _state.Jobs["myjob"].History.Single().Name);
        }

        [Fact]
        public void SetJobState_AddsEntry_ToTheStateIndex()
        {
            // Arrange
            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns("SomeState");

            var entry = new BackgroundJobEntry();
            _state.Jobs.TryAdd("myjob", entry);

            // Act
            Commit(x => x.SetJobState("myjob", state.Object));

            // Assert
            Assert.Same(entry, _state._jobStateIndex["SomeState"].Single());
        }

        [Fact]
        public void AddJobState_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.AddJobState(null, new Mock<IState>().Object)));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void AddJobState_ThrowsAnException_WhenStateIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.AddJobState("myjob", null)));

            Assert.Equal("state", exception.ParamName);
        }

        [Fact]
        public void AddJobState_DoesNotThrowAnException_WhenJobDoesNotExist()
        {
            Commit(x => x.AddJobState("some-job", new Mock<IState>().Object));
        }

        [Fact]
        public void AddJobState_AppendsStateHistory_WithTheNewEntry()
        {
            // Arrange
            var state = new Mock<IState>();
            state.SetupGet(x => x.Name).Returns("SomeName");
            state.SetupGet(x => x.Reason).Returns("SomeReason");
            state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string> {{ "Key", "Value" }});

            _state.Jobs.TryAdd("myjob", new BackgroundJobEntry());

            // Act
            Commit(x => x.AddJobState("myjob", state.Object));

            // Assert
            var entry = _state.Jobs["myjob"].History.Single();

            Assert.Equal("SomeName", entry.Name);
            Assert.Equal("SomeReason", entry.Reason);
            Assert.Equal("Value", entry.Data["Key"]);
            Assert.Equal(_now, entry.CreatedAt);
        }

        [Fact]
        public void AddToQueue_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.AddToQueue(null, "myjob")));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void AddToQueue_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.AddToQueue("myqueue", null)));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void AddToQueue_EnqueuesTheGivenJobId_ToTheGivenQueue_WhenItAlreadyExists()
        {
            var entry = new QueueEntry();
            _state.Queues.TryAdd("myqueue", entry);

            Commit(x => x.AddToQueue("myqueue", "jobid"));

            Assert.Equal("jobid", entry.Queue.Single());
        }

        [Fact]
        public void AddToQueue_CreatesEntry_AndEnqueuesTheGivenJobId_ToTheGivenQueue_WhenItDoesNotExist()
        {
            Commit(x => x.AddToQueue("myqueue", "jobid"));
            Assert.Equal("jobid", _state.Queues["myqueue"].Queue.Single());
        }

        [Fact]
        public void AddToQueue_SignalsTheGivenQueue_AfterCommittingChanges()
        {
            using (var semaphore = new SemaphoreSlim(0))
            {
                // Arrange
                var entry = new QueueEntry();
                entry.WaitHead.Next = new InMemoryQueueWaitNode(semaphore);

                _state.Queues.TryAdd("myqueue", entry);

                // Act
                Commit(x => x.AddToQueue("myqueue", "jobid"));

                // Assert
                Assert.Null(entry.WaitHead.Next);
            }
        }

        [Fact]
        public void IncrementCounter_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.IncrementCounter(null)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void IncrementCounter_IncrementsNonExistingCounterValue_WithoutSettingExpirationTime()
        {
            Commit(x => x.IncrementCounter("mycounter"));

            Assert.Equal(1, _state.Counters["mycounter"].Value);
            Assert.Null(_state.Counters["mycounter"].ExpireAt);
        }

        [Fact]
        public void IncrementCounter_IncrementsExistingCounterValue_WithoutSettingExpirationTime()
        {
            Commit(x => x.IncrementCounter("mycounter"));

            Commit(x => x.IncrementCounter("mycounter"));

            Assert.Equal(2, _state.Counters["mycounter"].Value);
            Assert.Null(_state.Counters["mycounter"].ExpireAt);
        }

        [Fact]
        public void IncrementCounter_IncrementExistingExpiringCounterValue_AndDoesNotResetItsExpirationTime()
        {
            // TODO: Make this behavior undefined?
            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromMinutes(30)));

            Commit(x => x.IncrementCounter("somecounter"));

            Assert.Equal(2, _state.Counters["somecounter"].Value);
            Assert.NotNull(_state.Counters["somecounter"]);
            Assert.Equal(_now.AddMinutes(30), _state.Counters["somecounter"].ExpireAt);
        }

        [Fact]
        public void IncrementCounter_RemovesCounterEntry_WhenIncrementingTheMinusOneValue_WithNoExpirationTimeSet()
        {
            Commit(x => x.DecrementCounter("somecounter"));

            Commit(x => x.IncrementCounter("somecounter"));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        [Fact]
        public void IncrementCounter_RemovesCounterEntry_WhenIncrementingTheMinusOneValue_WithExpirationTimeSet()
        {
            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromMinutes(5)));

            Commit(x => x.IncrementCounter("somecounter"));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        [Fact]
        public void IncrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.IncrementCounter(null, TimeSpan.FromMinutes(5))));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void IncrementCounter_WithExpiry_IncrementsNonExistingCounterValue_AndSetsItsExpirationTime()
        {
            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromMinutes(30)));

            Assert.Equal(1, _state.Counters["somecounter"].Value);
            Assert.NotNull(_state.Counters["somecounter"].ExpireAt);
            Assert.Equal(_now.AddMinutes(30), _state.Counters["somecounter"].ExpireAt);
        }

        [Fact]
        public void IncrementCounter_WithExpiry_IncrementsExistingCounterValue_AndUpdatesItsExpirationTime()
        {
            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromMinutes(30)));

            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromHours(1)));

            Assert.Equal(2, _state.Counters["somecounter"].Value);
            Assert.NotNull(_state.Counters["somecounter"].ExpireAt);
            Assert.Equal(_now.AddHours(1), _state.Counters["somecounter"].ExpireAt);
        }

        [Fact]
        public void IncrementCounter_WithExpiry_IncrementsExistingCounterValue_AndSetsExpirationTime_WhenItWasUnset()
        {
            Commit(x => x.IncrementCounter("mycounter"));

            Commit(x => x.IncrementCounter("mycounter", TimeSpan.FromMinutes(30)));

            Assert.Equal(2, _state.Counters["mycounter"].Value);
            Assert.NotNull(_state.Counters["mycounter"].ExpireAt);
            Assert.Equal(_now.AddMinutes(30), _state.Counters["mycounter"].ExpireAt);
        }

        [Fact]
        public void IncrementCounter_WithExpiry_RemovesCounterEntry_WhenIncrementingTheMinusOneValue_WithNoExpirationTimeSet()
        {
            Commit(x => x.DecrementCounter("somecounter"));

            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromMinutes(10)));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        [Fact]
        public void IncrementCounter_WithExpiry_RemovesCounterEntry_WhenIncrementingTheMinusOneValue_WithExpirationTimeSet()
        {
            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromMinutes(5)));

            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromMinutes(10)));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        [Fact]
        public void DecrementCounter_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.DecrementCounter(null)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void DecrementCounter_DecrementsNonExistingCounterValue_WithoutSettingExpirationTime()
        {
            Commit(x => x.DecrementCounter("mycounter"));

            Assert.Equal(-1, _state.Counters["mycounter"].Value);
            Assert.Null(_state.Counters["mycounter"].ExpireAt);
        }

        [Fact]
        public void DecrementCounter_DecrementsExistingCounterValue_WithoutSettingExpirationTime()
        {
            Commit(x => x.DecrementCounter("mycounter"));

            Commit(x => x.DecrementCounter("mycounter"));

            Assert.Equal(-2, _state.Counters["mycounter"].Value);
            Assert.Null(_state.Counters["mycounter"].ExpireAt);
        }

        [Fact]
        public void DecrementCounter_DecrementExistingExpiringCounterValue_AndDoesNotResetItsExpirationTime()
        {
            // TODO: Make this behavior undefined?
            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromMinutes(30)));

            Commit(x => x.DecrementCounter("somecounter"));

            Assert.Equal(-2, _state.Counters["somecounter"].Value);
            Assert.NotNull(_state.Counters["somecounter"]);
            Assert.Equal(_now.AddMinutes(30), _state.Counters["somecounter"].ExpireAt);
        }

        [Fact]
        public void DecrementCounter_RemovesCounterEntry_WhenDecrementingThePlusOneValue_WithNoExpirationTimeSet()
        {
            Commit(x => x.IncrementCounter("somecounter"));

            Commit(x => x.DecrementCounter("somecounter"));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        [Fact]
        public void DecrementCounter_RemovesCounterEntry_WhenDecrementingThePlusOneValue_WithExpirationTimeSet()
        {
            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromMinutes(5)));

            Commit(x => x.DecrementCounter("somecounter"));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        [Fact]
        public void DecrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.DecrementCounter(null, TimeSpan.FromMinutes(5))));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void DecrementCounter_WithExpiry_DecrementsNonExistingCounterValue_AndSetsItsExpirationTime()
        {
            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromMinutes(30)));

            Assert.Equal(-1, _state.Counters["somecounter"].Value);
            Assert.NotNull(_state.Counters["somecounter"].ExpireAt);
            Assert.Equal(_now.AddMinutes(30), _state.Counters["somecounter"].ExpireAt);
        }

        [Fact]
        public void DecrementCounter_WithExpiry_DecrementsExistingCounterValue_AndUpdatesItsExpirationTime()
        {
            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromMinutes(30)));

            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromHours(1)));

            Assert.Equal(-2, _state.Counters["somecounter"].Value);
            Assert.NotNull(_state.Counters["somecounter"].ExpireAt);
            Assert.Equal(_now.AddHours(1), _state.Counters["somecounter"].ExpireAt);
        }

        [Fact]
        public void DecrementCounter_WithExpiry_DecrementsExistingCounterValue_AndSetsExpirationTime_WhenItWasUnset()
        {
            Commit(x => x.DecrementCounter("mycounter"));

            Commit(x => x.DecrementCounter("mycounter", TimeSpan.FromMinutes(30)));

            Assert.Equal(-2, _state.Counters["mycounter"].Value);
            Assert.NotNull(_state.Counters["mycounter"].ExpireAt);
            Assert.Equal(_now.AddMinutes(30), _state.Counters["mycounter"].ExpireAt);
        }

        [Fact]
        public void DecrementCounter_WithExpiry_RemovesCounterEntry_WhenDecrementingThePlusOneValue_WithNoExpirationTimeSet()
        {
            Commit(x => x.IncrementCounter("somecounter"));

            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromMinutes(10)));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        [Fact]
        public void DecrementCounter_WithExpiry_RemovesCounterEntry_WhenDecrementingThePlusOneValue_WithExpirationTimeSet()
        {
            Commit(x => x.IncrementCounter("somecounter", TimeSpan.FromMinutes(5)));

            Commit(x => x.DecrementCounter("somecounter", TimeSpan.FromMinutes(10)));

            Assert.DoesNotContain("somecounter", _state.Counters);
        }

        private void Commit(Action<InMemoryTransaction> action)
        {
            var transaction = new InMemoryTransaction(new InMemoryDispatcherBase(_state));
            action(transaction);

            transaction.Commit();
        }
    }
}
