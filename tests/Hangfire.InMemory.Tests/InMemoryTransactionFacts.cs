using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;

// ReSharper disable StringLiteralTypo

// TODO: Case sensitivity tests, may be for different modes – SQL Server and Redis compatibility
// TODO: Expiration index checks for Increment/DecrementCounter
// TODO: Add checks for key lengths for SQL Server compatibility mode
// TODO: Add mixed namespace for better compatibility with Redis?
// TODO: AddToSet/AddRangeToSet with null value – how other storages behave?
// TODO: Check return values aren't the same and copied for each response for safety.

namespace Hangfire.InMemory.Tests
{
    public class InMemoryTransactionFacts
    {
        private readonly InMemoryState _state;
        private readonly DateTime _now;
        private readonly InMemoryConnection _connection;
        private readonly Dictionary<string,string> _parameters;
        private readonly Job _job;

        public InMemoryTransactionFacts()
        {
            _now = new DateTime(2020, 09, 29, 08, 05, 30, DateTimeKind.Utc);
            _state = new InMemoryState(() => _now);
            _parameters = new Dictionary<string, string>();
            _job = Job.FromExpression(() => MyMethod("value"));
            _connection = new InMemoryConnection(
                new InMemoryDispatcher(_state),
                new InMemoryStorageOptions());
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new InMemoryTransaction(null));
            Assert.Equal("dispatcher", exception.ParamName);
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.CreateJob(null, _parameters, TimeSpan.Zero)));

            Assert.Equal("job", exception.ParamName);
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersArgumentIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.CreateJob(_job, null, TimeSpan.Zero)));

            Assert.Equal("parameters", exception.ParamName);
        }

        [Fact]
        public void CreateExpiredJob_ReturnsUniqueBackgroundJobId_EachTime()
        {
            Commit(transaction =>
            {
                var id1 = transaction.CreateJob(_job, _parameters, TimeSpan.Zero);
                var id2 = transaction.CreateJob(_job, _parameters, TimeSpan.Zero);

                Assert.NotEqual(id1, id2);
            });
        }

        [Fact]
        public void CreateExpiredJob_CreatesCorrespondingBackgroundJobEntry()
        {
            string jobId = null;

            // Act
            Commit(transaction =>
            {
                jobId = transaction.CreateJob(_job, _parameters, TimeSpan.FromMinutes(30));                
            });

            // Assert
            var entry = _state.Jobs[jobId];
            var data = InvocationData.SerializeJob(_job);

            Assert.Equal(jobId, entry.Key);
            Assert.NotSame(_parameters, entry.Parameters);
            Assert.Empty(entry.History);
            Assert.Equal(_now, entry.CreatedAt);
            Assert.Equal(_now.AddMinutes(30), entry.ExpireAt);
            Assert.Equal(data.Type, entry.InvocationData.Type);
            Assert.Equal(data.Method, entry.InvocationData.Method);
            Assert.Equal(data.ParameterTypes, entry.InvocationData.ParameterTypes);
            Assert.Equal(data.Arguments, entry.InvocationData.Arguments);
            Assert.Null(entry.InvocationData.Queue);
        }

        [Fact]
        public void CreateExpiredJob_AddsBackgroundJobEntry_ToExpirationIndex()
        {
            string jobId = null;

            Commit(transaction =>
            {
                jobId = transaction.CreateJob(_job, _parameters, TimeSpan.FromMinutes(30));
            });

            Assert.Contains(_state.Jobs[jobId], _state._jobIndex);
        }

        [Fact]
        public void CreateExpiredJob_PreservesAllTheGivenParameters()
        {
            string jobId = null;

            Commit(transaction =>
            {
                _parameters.Add("RetryCount", "1");
                _parameters.Add("CurrentCulture", "en-US");

                jobId = transaction.CreateJob(_job, _parameters, TimeSpan.FromMinutes(30));
            });

            var parameters = _state.Jobs[jobId].Parameters;
            Assert.Equal(2, parameters.Count);
            Assert.Equal("1", parameters["RetryCount"]);
            Assert.Equal("en-US", parameters["CurrentCulture"]);
        }

        [Fact]
        public void CreateExpiredJob_CapturesJobQueue_Field()
        {
            string jobId = null;

            Commit(transaction =>
            {
                var job = new Job(_job.Type, _job.Method, _job.Args, "critical");
                jobId = transaction.CreateJob(job, _parameters, TimeSpan.FromMinutes(30));
            });

            Assert.Equal("critical", _state.Jobs[jobId].InvocationData.Queue);
        }

        [Fact]
        public void SetJobParameter_ThrowsAnException_WhenIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.SetJobParameter(null, "name", "value")));

            Assert.Equal("id", exception.ParamName);
        }

        [Fact]
        public void SetJobParameter_ThrowsAnException_WhenNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.SetJobParameter("id", null, "value")));

            Assert.Equal("name", exception.ParamName);
        }

        [Fact]
        public void SetJobParameter_DoesNotThrow_WhenValueIsNull()
        {
            string jobId = null;

            Commit(transaction =>
            {
                jobId = transaction.CreateJob(_job, _parameters, TimeSpan.FromMinutes(30));
                transaction.SetJobParameter(jobId, "name", null);
            });

            Assert.Null(_state.Jobs[jobId].Parameters["name"]);
        }

        [Fact]
        public void SetJobParameter_DoesNotThrow_WhenBackgroundJobDoesNotExist()
        {
            Commit(transaction =>
            {
                transaction.SetJobParameter("some-id", "name", "value");
            });
        }

        [Fact]
        public void SetJobParameter_AppendsParameters_WithTheGivenValue()
        {
            string jobId = null;

            Commit(transaction =>
            {
                jobId = transaction.CreateJob(_job, _parameters, TimeSpan.FromMinutes(30));
                transaction.SetJobParameter(jobId, "CurrentCulture", "en-US");
            });

            Assert.Equal("en-US", _state.Jobs[jobId].Parameters["CurrentCulture"]);
        }

        [Fact]
        public void SetJobParameter_OverwritesTheGivenParameter_WithTheNewValue()
        {
            _parameters.Add("RetryCount", "1");
            string jobId = null;

            Commit(transaction =>
            {
                jobId = transaction.CreateJob(_job, _parameters, TimeSpan.FromMinutes(30));
                transaction.SetJobParameter(jobId, "RetryCount", "2");
            });

            Assert.Equal("2", _state.Jobs[jobId].Parameters["RetryCount"]);
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
            Assert.False(_state.Jobs.ContainsKey("some-job"));
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
            Assert.False(_state.Jobs.ContainsKey("some-job"));
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
            Assert.False(_state.Jobs.ContainsKey("some-job"));
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
            Assert.False(_state.Jobs.ContainsKey("some-job"));
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
            using (var semaphore = new AutoResetEvent(false))
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

        [Fact]
        public void AddToSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.AddToSet(null, "value")));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void AddToSet_ThrowsAnException_WhenValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.AddToSet("key", null)));

            Assert.Equal("value", exception.ParamName);
        }

        [Fact]
        public void AddToSet_AddsElementWithZeroScore_ToTheGivenSet()
        {
            Commit(x => x.AddToSet("key", "value"));

            Assert.Equal(0.0D, _state.Sets["key"].Single().Score, 3);
        }

        [Fact]
        public void AddToSet_WithScore_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.AddToSet(null, "value", 1.2D)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void AddToSet_WithScore_ThrowsAnException_WhenValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.AddToSet("key", null, 1.2D)));

            Assert.Equal("value", exception.ParamName);
        }

        [Fact]
        public void AddToSet_WithScore_AddsElementWithTheGivenScore_ToTheGivenSet()
        {
            Commit(x => x.AddToSet("key", "value", 1.2D));

            Assert.Equal(1.2D, _state.Sets["key"].Single().Score, 3);
        }

        [Fact]
        public void AddToSet_WithScore_AddsElementWithTheGivenScore_ToAnExistingSet()
        {
            Commit(x => x.AddToSet("key", "value1", 1.111D));

            Commit(x => x.AddToSet("key", "value2", 2.222D));

            Assert.Equal(2, _state.Sets["key"].Count);
            Assert.Equal(1.111D, _state.Sets["key"].Single(x => x.Value == "value1").Score, 4);
            Assert.Equal(2.222D, _state.Sets["key"].Single(x => x.Value == "value2").Score, 4);
        }

        [Fact]
        public void AddToSet_WithScore_UpdatesScore_WhenValueAlreadyExists()
        {
            Commit(x => x.AddToSet("key", "value", 1.0D));

            Commit(x => x.AddToSet("key", "value", 2000.0D));

            Assert.Equal(2000.0D, _state.Sets["key"].Single().Score);
        }

        [Fact]
        public void AddToSet_WithScore_StoresElements_InDifferentSets()
        {
            Commit(x => x.AddToSet("key1", "value", 1.2D));
            Commit(x => x.AddToSet("key2", "value", 2.3D));

            Assert.Equal(2, _state.Sets.Count);
            Assert.Equal(1.2D, _state.Sets["key1"].Single().Score, 2);
            Assert.Equal(2.3D, _state.Sets["key2"].Single().Score, 2);
        }

        [Fact]
        public void AddToSet_InsertsElementsIntoASortedSet()
        {
            Commit(x => x.AddToSet("key", "value1", 43.0D));
            Commit(x => x.AddToSet("key", "value2", 5.0D));
            Commit(x => x.AddToSet("key", "value3", 743.0D));
            Commit(x => x.AddToSet("key", "value4", -30.0D));

            var results = _state.Sets["key"].Select(x => x.Value).ToArray();
            Assert.Equal(new [] { "value4", "value2", "value1", "value3" }, results);
        }

        [Fact]
        public void RemoveFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.RemoveFromSet(null, "value")));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void RemoveFromSet_ThrowsAnException_WhenValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.RemoveFromSet("key", null)));

            Assert.Equal("value", exception.ParamName);
        }

        [Fact]
        public void RemoveFromSet_DoesNotThrow_WhenTargetSetDoesNotExist()
        {
            Commit(x => x.RemoveFromSet("some-set", "value"));
            Assert.False(_state.Sets.ContainsKey("some-set"));
        }

        [Fact]
        public void RemoveFromSet_RemovesTheGivenElement_FromTheTargetSet()
        {
            Commit(x => x.AddToSet("key", "1", 1.0D));
            Commit(x => x.AddToSet("key", "2", 2.0D));
            Commit(x => x.AddToSet("key", "3", 3.0D));

            Commit(x => x.RemoveFromSet("key", "2"));

            Assert.Equal(
                new [] { "1", "3" },
                _state.Sets["key"].Select(x => x.Value).ToArray());
        }

        [Fact]
        public void RemoveFromSet_RemovesTargetSetEntirely_WhenLastElementIsRemoved()
        {
            Commit(x => x.AddToSet("key", "value"));

            Commit(x => x.RemoveFromSet("key", "value"));

            Assert.False(_state.Sets.ContainsKey("key"));
        }

        [Fact]
        public void RemoveFromSet_DoesNotRemove_TheSameValue_FromOtherSets()
        {
            Commit(x => x.AddToSet("key1", "value"));
            Commit(x => x.AddToSet("key2", "value"));
            Commit(x => x.AddToSet("key3", "value"));

            Commit(x => x.RemoveFromSet("key2", "value"));

            Assert.True (_state.Sets.ContainsKey("key1"));
            Assert.False(_state.Sets.ContainsKey("key2"));
            Assert.True (_state.Sets.ContainsKey("key3"));
        }

        [Fact]
        public void InsertToList_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.InsertToList(null, "value")));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void InsertToList_ThrowsAnException_WhenValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.InsertToList("key", null)));

            Assert.Equal("value", exception.ParamName);
        }

        [Fact]
        public void InsertToList_InsertsANewElement_EvenWhenTargetListDoesNotYetExist()
        {
            Commit(x => x.InsertToList("key", "value"));

            Assert.True(_state.Lists.ContainsKey("key"));
            Assert.Equal(1, _state.Lists["key"].Count);
            Assert.Equal("value", _state.Lists["key"][0]);
        }

        [Fact]
        public void InsertToList_PrependsAnExistingList_WithTheGivenElement()
        {
            Commit(x => x.InsertToList("key", "value1"));
            Commit(x => x.InsertToList("key", "value2"));

            Assert.Equal(2, _state.Lists["key"].Count);
            Assert.Equal("value2", _state.Lists["key"][0]);
            Assert.Equal("value1", _state.Lists["key"][1]);
        }

        [Fact]
        public void InsertToList_IsAbleToHaveMultipleElements_WithTheSameValue()
        {
            Commit(x => x.InsertToList("key", "1"));
            Commit(x => x.InsertToList("key", "2"));
            Commit(x => x.InsertToList("key", "3"));
            Commit(x => x.InsertToList("key", "2"));

            var entry = _state.Lists["key"];
            Assert.Equal(4, entry.Count);
            Assert.Equal("2", entry[0]);
            Assert.Equal("3", entry[1]);
            Assert.Equal("2", entry[2]);
            Assert.Equal("1", entry[3]);
        }

        [Fact]
        public void RemoveFromList_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.RemoveFromList(null, "value")));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void RemoveFromList_ThrowsAnException_WhenValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.RemoveFromList("key", null)));

            Assert.Equal("value", exception.ParamName);
        }

        [Fact]
        public void RemoveFromList_DoesNotThrow_WhenTargetListDoesNotExist()
        {
            Commit(x => x.RemoveFromList("some-key", "some-value"));
            Assert.False(_state.Lists.ContainsKey("some-key"));
        }

        [Fact]
        public void RemoveFromList_RemovesAllOccurrences_OfTheGivenValue_InTheGivenList()
        {
            Commit(x => x.InsertToList("key", "1"));
            Commit(x => x.InsertToList("key", "2"));
            Commit(x => x.InsertToList("key", "3"));
            Commit(x => x.InsertToList("key", "2"));
            Commit(x => x.InsertToList("key", "1"));
            Commit(x => x.InsertToList("key", "2"));

            Commit(x => x.RemoveFromList("key", "2"));

            var entry = _state.Lists["key"];
            Assert.Equal(3, entry.Count);
            Assert.Equal("1", entry[0]);
            Assert.Equal("3", entry[1]);
            Assert.Equal("1", entry[2]);
        }

        [Fact]
        public void RemoveFromList_RemovesAssociatedKey_WhenTargetListBecomesEmpty()
        {
            Commit(x => x.InsertToList("key", "1"));
            Commit(x => x.InsertToList("key", "1"));

            Commit(x => x.RemoveFromList("key", "1"));

            Assert.False(_state.Lists.ContainsKey("key"));
        }

        [Fact]
        public void TrimList_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.TrimList(null, 0, 1)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void TrimList_DoesNotThrow_WhenTargetListDoesNotExists()
        {
            Commit(x => x.TrimList("some-key", 0, 1));
            Assert.False(_state.Lists.ContainsKey("some-key"));
        }

        [Fact]
        public void TrimList_TrimsTheGivenList_ToTheSpecifiedRange()
        {
            Commit(x => x.InsertToList("key", "0"));
            Commit(x => x.InsertToList("key", "1"));
            Commit(x => x.InsertToList("key", "2"));
            Commit(x => x.InsertToList("key", "3"));
            Commit(x => x.InsertToList("key", "4"));

            Commit(x => x.TrimList("key", 1, 2));

            var list = _state.Lists["key"];
            Assert.Equal(2, list.Count);
            Assert.Equal("3", list[0]);
            Assert.Equal("2", list[1]);
        }

        [Fact]
        public void TrimList_RemovesElementsToEnd_WhenKeepEndingAt_IsGreaterThanTheNumberOfElements()
        {
            Commit(x => x.InsertToList("key", "0"));
            Commit(x => x.InsertToList("key", "1"));
            Commit(x => x.InsertToList("key", "2"));

            Commit(x => x.TrimList("key", 1, 100));

            var list = _state.Lists["key"];
            Assert.Equal(2, list.Count);
            Assert.Equal("1", list[0]);
            Assert.Equal("0", list[1]);
        }

        [Fact]
        public void TrimList_RemovesListEntry_WhenResultingListIsEmpty()
        {
            Commit(x => x.InsertToList("key", "0"));

            Commit(x => x.TrimList("key", 5, 6));

            Assert.False(_state.Lists.ContainsKey("key"));
        }

        [Fact]
        public void TrimList_RemovesAllElements_WhenStartingFromIsGreaterThanKeepEndingAt()
        {
            Commit(x => x.InsertToList("key", "0"));
            Commit(x => x.InsertToList("key", "1"));
            Commit(x => x.InsertToList("key", "2"));
            Commit(x => x.InsertToList("key", "3"));

            Commit(x => x.TrimList("key", 2, 1));

            Assert.False(_state.Lists.ContainsKey("key"));
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.SetRangeInHash(null, Enumerable.Empty<KeyValuePair<string, string>>())));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.SetRangeInHash("key", null)));

            Assert.Equal("keyValuePairs", exception.ParamName);
        }

        [Fact]
        public void SetRangeInHash_DoesNotCreateEmptyHashEntry_WhenKeyValuePairsIsEmpty()
        {
            Commit(x => x.SetRangeInHash("key", Enumerable.Empty<KeyValuePair<string, string>>()));

            Assert.False(_state.Hashes.ContainsKey("key"));
        }

        [Fact]
        public void SetRangeInHash_CreatesANewEntry_WithSpecifiedRecords_WhenHashDoesNotExist()
        {
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string>
            {
                { "field1", "value1" },
                { "field2", "value2" }
            }));

            var hash = _state.Hashes["key"];
            Assert.Equal("value1", hash.Value["field1"]);
            Assert.Equal("value2", hash.Value["field2"]);
        }

        [Fact]
        public void SetRangeInHash_InsertsNewEntries_IntoExistingHashEntry_WhenFieldNamesDoNotInterleave()
        {
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string>
            {
                { "field1", "value1" },
                { "field3", "value3" }
            }));

            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string>
            {
                { "field2", "value2" },
                { "field4", "value4" }
            }));

            var hash = _state.Hashes["key"];
            Assert.Equal("value1", hash.Value["field1"]);
            Assert.Equal("value2", hash.Value["field2"]);
            Assert.Equal("value3", hash.Value["field3"]);
            Assert.Equal("value4", hash.Value["field4"]);
        }

        [Fact]
        public void SetRangeInHash_OverwritesAllTheGivenFields_WhenTheyAlreadyExist()
        {
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string>
            {
                { "field1", "value1" },
                { "field2", "value2" },
                { "field3", "value3" }
            }));

            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string>
            {
                { "field1", "newvalue1" },
                { "field3", "newvalue3" }
            }));

            var hash = _state.Hashes["key"];
            Assert.Equal(3, hash.Value.Count);
            Assert.Equal("newvalue1", hash.Value["field1"]);
            Assert.Equal("value2", hash.Value["field2"]);
            Assert.Equal("newvalue3", hash.Value["field3"]);
        }

        [Fact]
        public void SetRangeInHash_CanSetANullValue()
        {
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string>
            {
                { "field1", null },
                { "field2", null }
            }));

            Assert.Null(_state.Hashes["key"].Value["field1"]);
            Assert.Null(_state.Hashes["key"].Value["field2"]);
        }

        [Fact]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.RemoveHash(null)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void RemoveHash_DoesNotThrow_WhenTargetHashDoesNotExist()
        {
            Commit(x => x.RemoveHash("some-key"));
            Assert.False(_state.Hashes.ContainsKey("some-key"));
        }

        [Fact]
        public void RemoveHash_RemovesTheSpecifiedHashEntryImmediately()
        {
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string>
            {
                { "field", "value" }
            }));

            Commit(x => x.RemoveHash("key"));

            Assert.False(_state.Hashes.ContainsKey("key"));
        }

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.AddRangeToSet(null, new List<string>())));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenItemsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.AddRangeToSet("key", null)));

            Assert.Equal("items", exception.ParamName);
        }

        [Fact]
        public void AddRangeToSet_AddsAllTheGivenElements_WithZeroScore()
        {
            Commit(x => x.AddRangeToSet("key", new List<string> { "1", "2", "3" }));

            Assert.Equal(3, _state.Sets["key"].Count);
            Assert.Equal(0.0D, _state.Sets["key"].Single(x => x.Value == "1").Score, 2);
            Assert.Equal(0.0D, _state.Sets["key"].Single(x => x.Value == "2").Score, 2);
            Assert.Equal(0.0D, _state.Sets["key"].Single(x => x.Value == "3").Score, 2);
        }

        [Fact]
        public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => Commit(x => x.RemoveSet(null)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void RemoveSet_DoesNotThrow_WhenTargetSetDoesNotExist()
        {
            Commit(x => x.RemoveSet("some-key"));
            Assert.False(_state.Sets.ContainsKey("some-key"));
        }

        [Fact]
        public void RemoveSet_RemovesTheSpecifiedSetEntryImmediately()
        {
            Commit(x => x.AddToSet("key", "value"));

            Commit(x => x.RemoveSet("key"));

            Assert.False(_state.Sets.ContainsKey("key"));
        }

        [Fact]
        public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.ExpireHash(null, TimeSpan.Zero)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void ExpireHash_DoesNotThrow_WhenTargetHashDoesNotExist()
        {
            Commit(x => x.ExpireHash("some-key", TimeSpan.Zero));
            Assert.False(_state.Hashes.ContainsKey("some-key"));
        }

        [Fact]
        public void ExpireHash_SetsExpirationTime_OfTheGivenHash()
        {
            // Arrange
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string> { { "field", "value" } }));

            // Act
            Commit(x => x.ExpireHash("key", TimeSpan.FromMinutes(30)));

            // Assert
            var expireAt = _state.Hashes["key"].ExpireAt;
            Assert.NotNull(expireAt);
            Assert.Equal(_now.AddMinutes(30), expireAt.Value);
        }

        [Fact]
        public void ExpireHash_AddsEntry_ToExpirationIndex()
        {
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string> { { "field", "value" } }));

            Commit(x => x.ExpireHash("key", TimeSpan.FromMinutes(30)));

            Assert.Equal("key", _state._hashIndex.Single().Key);
        }

        [Fact]
        public void ExpireList_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.ExpireList(null, TimeSpan.Zero)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void ExpireList_DoesNotThrow_WhenTargetListDoesNotExist()
        {
            Commit(x => x.ExpireList("some-key", TimeSpan.Zero));
            Assert.False(_state.Lists.ContainsKey("some-key"));
        }

        [Fact]
        public void ExpireList_SetsExpirationTime_OfTheGivenList()
        {
            // Arrange
            Commit(x => x.InsertToList("key", "value"));

            // Act
            Commit(x => x.ExpireList("key", TimeSpan.FromMinutes(30)));

            // Assert
            var expireAt = _state.Lists["key"].ExpireAt;
            Assert.NotNull(expireAt);
            Assert.Equal(_now.AddMinutes(30), expireAt.Value);
        }

        [Fact]
        public void ExpireList_AddsEntry_ToExpirationIndex()
        {
            Commit(x => x.InsertToList("key", "value"));

            Commit(x => x.ExpireList("key", TimeSpan.FromMinutes(30)));

            Assert.Equal("key", _state._listIndex.Single().Key);
        }

        [Fact]
        public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.ExpireSet(null, TimeSpan.Zero)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void ExpireSet_DoesNotThrow_WhenTargetSetDoesNotExist()
        {
            Commit(x => x.ExpireSet("some-key", TimeSpan.Zero));
            Assert.False(_state.Sets.ContainsKey("some-key"));
        }

        [Fact]
        public void ExpireSet_SetsExpirationTime_OfTheGivenSet()
        {
            // Arrange
            Commit(x => x.AddToSet("key", "value"));

            // Act
            Commit(x => x.ExpireSet("key", TimeSpan.FromMinutes(30)));

            // Assert
            var expireAt = _state.Sets["key"].ExpireAt;
            Assert.NotNull(expireAt);
            Assert.Equal(_now.AddMinutes(30), expireAt.Value);
        }

        [Fact]
        public void ExpireSet_AddsEntry_ToExpirationIndex()
        {
            Commit(x => x.AddToSet("key", "value"));

            Commit(x => x.ExpireSet("key", TimeSpan.FromMinutes(30)));

            Assert.Equal("key", _state._setIndex.Single().Key);
        }

        [Fact]
        public void PersistHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.PersistHash(null)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void PersistHash_DoesNotThrowAnException_WhenHashDoesNotExist()
        {
            Commit(x => x.PersistHash("some-key"));
            Assert.False(_state.Hashes.ContainsKey("some-key"));
        }

        [Fact]
        public void PersistHash_ResetsExpirationTime_OfTheGivenHash()
        {
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string> { { "field", "value" } }));
            Commit(x => x.ExpireHash("key", TimeSpan.FromMinutes(30)));
            Assert.NotNull(_state.Hashes["key"].ExpireAt);

            Commit(x => x.PersistHash("key"));

            Assert.Null(_state.Hashes["key"].ExpireAt);
        }

        [Fact]
        public void PersistHash_RemovesEntry_FromExpirationIndex()
        {
            // Arrange
            Commit(x => x.SetRangeInHash("key", new Dictionary<string, string> { { "field", "value" } }));
            Commit(x => x.ExpireHash("key", TimeSpan.FromMinutes(30)));
            Assert.NotEmpty(_state._hashIndex);

            // Act
            Commit(x => x.PersistHash("key"));

            // Assert
            Assert.Empty(_state._hashIndex);
        }

        [Fact]
        public void PersistList_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.PersistList(null)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void PersistList_DoesNotThrowAnException_WhenListDoesNotExist()
        {
            Commit(x => x.PersistList("some-key"));
            Assert.False(_state.Lists.ContainsKey("some-key"));
        }

        [Fact]
        public void PersistList_ResetsExpirationTime_OfTheGivenList()
        {
            Commit(x => x.InsertToList("key", "value"));
            Commit(x => x.ExpireList("key", TimeSpan.FromMinutes(30)));
            Assert.NotNull(_state.Lists["key"].ExpireAt);

            Commit(x => x.PersistList("key"));

            Assert.Null(_state.Lists["key"].ExpireAt);
        }

        [Fact]
        public void PersistList_RemovesEntry_FromExpirationIndex()
        {
            // Arrange
            Commit(x => x.InsertToList("key", "value"));
            Commit(x => x.ExpireList("key", TimeSpan.FromMinutes(30)));
            Assert.NotEmpty(_state._listIndex);

            // Act
            Commit(x => x.PersistList("key"));

            // Assert
            Assert.Empty(_state._listIndex);
        }

        [Fact]
        public void PersistSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => Commit(
                x => x.PersistSet(null)));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void PersistSet_DoesNotThrowAnException_WhenListDoesNotExist()
        {
            Commit(x => x.PersistSet("some-key"));
            Assert.False(_state.Sets.ContainsKey("some-key"));
        }

        [Fact]
        public void PersistSet_ResetsExpirationTime_OfTheGivenSet()
        {
            Commit(x => x.AddToSet("key", "value"));
            Commit(x => x.ExpireSet("key", TimeSpan.FromMinutes(30)));
            Assert.NotNull(_state.Sets["key"].ExpireAt);

            Commit(x => x.PersistSet("key"));

            Assert.Null(_state.Sets["key"].ExpireAt);
        }

        [Fact]
        public void PersistSet_RemovesEntry_FromExpirationIndex()
        {
            // Arrange
            Commit(x => x.AddToSet("key", "value"));
            Commit(x => x.ExpireSet("key", TimeSpan.FromMinutes(30)));
            Assert.NotEmpty(_state._setIndex);

            // Act
            Commit(x => x.PersistSet("key"));

            // Assert
            Assert.Empty(_state._setIndex);
        }

        [Fact]
        public void Commit_DoesNotThrow_WhenThereAreNoCommandsToRun()
        {
            Commit(x => { });
        }

        private void Commit(Action<InMemoryTransaction> action)
        {
            using (var transaction = new InMemoryTransaction(_connection))
            {
                action(transaction);
                transaction.Commit();
            }
        }

#pragma warning disable xUnit1013 // Public method should be marked as test
        public void MyMethod(string arg)
#pragma warning restore xUnit1013 // Public method should be marked as test
        {
        }
    }
}
