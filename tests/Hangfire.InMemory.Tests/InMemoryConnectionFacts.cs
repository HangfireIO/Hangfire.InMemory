using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryConnectionFacts
    {
        private readonly DateTime _now;
        private readonly InMemoryState _state;
        private readonly Dictionary<string, string> _parameters;
        private readonly Job _job;

        public InMemoryConnectionFacts()
        {
            _now = new DateTime(2020, 09, 29, 08, 05, 30, DateTimeKind.Utc);
            _state = new InMemoryState(() => _now);
            _parameters = new Dictionary<string, string>();
            _job = Job.FromExpression(() => MyMethod("value"));
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
            UseConnection(connection =>
            {
                var transaction = connection.CreateWriteTransaction();
                transaction.AddToSet("key", "value");

                transaction.Commit();

                Assert.Equal("value", _state.Sets["key"].Single().Value);
            });
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(null, _parameters, _now, TimeSpan.Zero));

                Assert.Equal("job", exception.ParamName);
            });
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersArgumentIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(_job, null, _now, TimeSpan.Zero));

                Assert.Equal("parameters", exception.ParamName);
            });
        }

        [Fact]
        public void CreateExpiredJob_ReturnsUniqueBackgroundJobId_EachTime()
        {
            UseConnection(connection =>
            {
                var id1 = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.Zero);
                var id2 = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.Zero);

                Assert.NotEqual(id1, id2);
            });
        }

        [Fact]
        public void CreateExpiredJob_CreatesCorrespondingBackgroundJobEntry()
        {
            UseConnection(connection =>
            {
                // Act
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

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
            });
        }

        [Fact]
        public void CreateExpiredJob_AddsBackgroundJobEntry_ToExpirationIndex()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                Assert.Contains(_state.Jobs[jobId], _state._jobIndex);
            });
        }

        [Fact]
        public void CreateExpiredJob_PreservesAllTheGivenParameters()
        {
            UseConnection(connection =>
            {
                _parameters.Add("RetryCount", "1");
                _parameters.Add("CurrentCulture", "en-US");

                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                var parameters = _state.Jobs[jobId].Parameters;
                Assert.Equal(2, parameters.Count);
                Assert.Equal("1", parameters["RetryCount"]);
                Assert.Equal("en-US", parameters["CurrentCulture"]);
            });
        }

        [Fact]
        public void SetJobParameter_ThrowsAnException_WhenIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact]
        public void SetJobParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("id", null, "value"));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void SetJobParameter_DoesNotThrow_WhenValueIsNull()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                connection.SetJobParameter(jobId, "name", null);

                Assert.Null(_state.Jobs[jobId].Parameters["name"]);
            });
        }

        [Fact]
        public void SetJobParameter_DoesNotThrow_WhenBackgroundJobDoesNotExist()
        {
            UseConnection(connection =>
            {
                connection.SetJobParameter("some-id", "name", "value");
            });
        }

        [Fact]
        public void SetJobParameter_AppendsParameters_WithTheGivenValue()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                connection.SetJobParameter(jobId, "CurrentCulture", "en-US");

                Assert.Equal("en-US", _state.Jobs[jobId].Parameters["CurrentCulture"]);
            });
        }

        [Fact]
        public void SetJobParameter_OverwritesTheGivenParameter_WithTheNewValue()
        {
            UseConnection(connection =>
            {
                _parameters.Add("RetryCount", "1");
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                connection.SetJobParameter(jobId, "RetryCount", "2");

                Assert.Equal("2", _state.Jobs[jobId].Parameters["RetryCount"]);
            });
        }

        [Fact]
        public void GetJobParameter_ThrowsAnException_WhenIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter(null, "name"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact]
        public void GetJobParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter("id", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void GetJobParameter_ReturnsNull_WhenGivenBackgroundJob_DoesNotExist()
        {
            UseConnection(connection =>
            {
                var value = connection.GetJobParameter("jobId", "name");

                Assert.Null(value);
            });
        }

        [Fact]
        public void GetJobParameter_ReturnsNull_WhenGivenParameter_DoesNotExist()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                var value = connection.GetJobParameter(jobId, "name");

                Assert.Null(value);
            });
        }

        [Fact]
        public void GetJobParameter_ReturnsTheActualValue_WhenBackgroundJobAndParameterExist()
        {
            UseConnection(connection =>
            {
                _parameters.Add("name", "value");
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                var value = connection.GetJobParameter(jobId, "name");

                Assert.Equal("value", value);
            });
        }

        [Fact]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobData(null));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void GetJobData_ReturnsNull_WhenBackgroundJobDoesNotExist()
        {
            UseConnection(connection =>
            {
                var data = connection.GetJobData("some-job");

                Assert.Null(data);
            });
        }

        [Fact]
        public void GetJobData_ReturnsActualData_WhenBackgroundJobExists_ButNotYetInitialized()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                var data = connection.GetJobData(jobId);

                Assert.NotNull(data);
                Assert.Equal(_now, data.CreatedAt);
                Assert.Same(_job.Type, data.Job.Type);
                Assert.Same(_job.Method, data.Job.Method);
                Assert.Equal(_job.Args, data.Job.Args);
                Assert.Null(data.State);
            });
        }

        [Fact]
        public void GetJobData_ReturnsCorrectState_WhenBackgroundJobWasInitialized()
        {
            UseConnection(connection =>
            {
                // Arrange
                var state = new Mock<IState>();
                state.SetupGet(x => x.Name).Returns("MyState");

                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                var transaction = connection.CreateWriteTransaction();
                transaction.SetJobState(jobId, state.Object);
                transaction.Commit();

                // Act
                var data = connection.GetJobData(jobId);

                // Assert
                Assert.Same(_job.Type, data.Job.Type);
                Assert.Same(_job.Method, data.Job.Method);
                Assert.Equal(_job.Args, data.Job.Args);
                Assert.Equal("MyState", data.State);
            });
        }

        [Fact]
        public void GetJobData_ContainsLoadException_WhenThereIsAnErrorDuringDeserialization()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));
                _state.Jobs[jobId].InvocationData = new InvocationData("afsasf", "oiaeghgaoiwejg", "ksad", "aiwheg3");

                var data = connection.GetJobData(jobId);

                Assert.Null(data.Job);
                Assert.NotNull(data.LoadException);
                Assert.Equal(_now, data.CreatedAt);
            });
        }

        [Fact]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void GetStateData_ReturnsNull_WhenBackgroundJobDoesNotExist()
        {
            UseConnection(connection =>
            {
                var data = connection.GetStateData("some-job");

                Assert.Null(data);
            });
        }

        [Fact]
        public void GetStateData_ReturnsNull_WhenBackgroundJobExists_ButNotYetInitialized()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                var data = connection.GetStateData(jobId);

                Assert.Null(data);
            });
        }

        [Fact]
        public void GetStateData_ReturnsExpectedResult_WhenBackgroundJobExists_AndInitialized()
        {
            UseConnection(connection =>
            {
                // Arrange
                var stateData = new Dictionary<string, string> {{ "name", "value" }};

                var state = new Mock<IState>();
                state.SetupGet(x => x.Name).Returns("MyState");
                state.SetupGet(x => x.Reason).Returns("MyReason");
                state.Setup(x => x.SerializeData()).Returns(stateData);

                var jobId = connection.CreateExpiredJob(_job, _parameters, _now, TimeSpan.FromMinutes(30));

                var transaction = connection.CreateWriteTransaction();
                transaction.SetJobState(jobId, state.Object);
                transaction.Commit();

                // Act
                var data = connection.GetStateData(jobId);

                // Assert
                Assert.Equal("MyState", data.Name);
                Assert.Equal("MyReason", data.Reason);
                Assert.Equal("value", data.Data["name"]);
                Assert.NotSame(stateData, data.Data);
            });
        }

        private void UseConnection(Action<InMemoryConnection> action)
        {
            using (var connection = CreateConnection())
            {
                action(connection);
            }
        }

        private InMemoryConnection CreateConnection()
        {
            return new InMemoryConnection(new InMemoryDispatcherBase(_state));
        }

        public void MyMethod(string arg)
        {
        }
    }
}
