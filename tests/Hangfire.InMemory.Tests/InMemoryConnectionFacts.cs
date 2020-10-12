using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;

// TODO: Add unit test for ranges where startingFrom is greater than endingAt (and similar)

namespace Hangfire.InMemory.Tests
{
    public class InMemoryConnectionFacts
    {
        private readonly InMemoryState _state;
        private readonly Dictionary<string, string> _parameters;
        private readonly Job _job;
        private DateTime _now;

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
        public void GetJobParameter_ReturnsNull_WhenGivenJobAndParameterExist_ButValueItselfIsNull()
        {
            UseConnection(connection =>
            {
                _parameters.Add("name", null);
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

                Commit(connection, x => x.SetJobState(jobId, state.Object));

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

                Commit(connection, x => x.SetJobState(jobId, state.Object));

                // Act
                var data = connection.GetStateData(jobId);

                // Assert
                Assert.Equal("MyState", data.Name);
                Assert.Equal("MyReason", data.Reason);
                Assert.Equal("value", data.Data["name"]);
                Assert.NotSame(stateData, data.Data);
            });
        }

        [Fact]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromSet(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsNonNullEmptyCollection_WhenTargetSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-key");

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsAllValues_CorrectlySorted()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value3", 3);
                    x.AddToSet("key", "value1", 1);
                    x.AddToSet("key", "value2", 2);
                });

                var result = connection.GetAllItemsFromSet("key");

                Assert.Equal(new [] { "value1", "value2", "value3" }, result);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            // Duplicated from InMemoryTransactionFacts class
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, Enumerable.Empty<KeyValuePair<string, string>>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            // Duplicated from InMemoryTransactionFacts class
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("key", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_DoesNotCreateEmptyHashEntry_WhenKeyValuePairsIsEmpty()
        {
            // Duplicated from InMemoryTransactionFacts class
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", Enumerable.Empty<KeyValuePair<string, string>>());

                Assert.False(_state.Hashes.ContainsKey("key"));
            });
        }

        [Fact]
        public void SetRangeInHash_CreatesANewEntry_WithSpecifiedRecords_WhenHashDoesNotExist()
        {
            // Duplicated from InMemoryTransactionFacts class
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    {"field1", "value1"},
                    {"field2", "value2"}
                });

                var hash = _state.Hashes["key"];
                Assert.Equal("value1", hash.Value["field1"]);
                Assert.Equal("value2", hash.Value["field2"]);
            });
        }

        [Fact]
        public void SetRangeInHash_InsertsNewEntries_IntoExistingHashEntry_WhenFieldNamesDoNotInterleave()
        {
            // Duplicated from InMemoryTransactionFacts class
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    {"field1", "value1"},
                    {"field3", "value3"}
                });

                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    {"field2", "value2"},
                    {"field4", "value4"}
                });

                var hash = _state.Hashes["key"];
                Assert.Equal("value1", hash.Value["field1"]);
                Assert.Equal("value2", hash.Value["field2"]);
                Assert.Equal("value3", hash.Value["field3"]);
                Assert.Equal("value4", hash.Value["field4"]);
            });
        }

        [Fact]
        public void SetRangeInHash_OverwritesAllTheGivenFields_WhenTheyAlreadyExist()
        {
            // Duplicated from InMemoryTransactionFacts class
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    {"field1", "value1"},
                    {"field2", "value2"},
                    {"field3", "value3"}
                });

                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    {"field1", "newvalue1"},
                    {"field3", "newvalue3"}
                });

                var hash = _state.Hashes["key"];
                Assert.Equal(3, hash.Value.Count);
                Assert.Equal("newvalue1", hash.Value["field1"]);
                Assert.Equal("value2", hash.Value["field2"]);
                Assert.Equal("newvalue3", hash.Value["field3"]);
            });
        }

        [Fact]
        public void SetRangeInHash_CanSetANullValue()
        {
            // Duplicated from InMemoryTransactionFacts class
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    {"field1", null},
                    {"field2", null}
                });

                Assert.Null(_state.Hashes["key"].Value["field1"]);
                Assert.Null(_state.Hashes["key"].Value["field2"]);
            });
        }

        [Fact]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllEntriesFromHash(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsNullValue_WhenHashDoesNotExists()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-key");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsAllEntries()
        {
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var result = connection.GetAllEntriesFromHash("key");

                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetListCount(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetListCount_ReturnsZero_WhenTargetListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetListCount("some-key");

                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetListCount_ReturnsTheNumberOfElements_InTheGivenList()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.InsertToList("key", "value");
                    x.InsertToList("key", "value");
                    x.InsertToList("key", "value");
                    x.InsertToList("key", "value");
                });

                var result = connection.GetListCount("key");

                Assert.Equal(4, result);
            });
        }

        [Fact]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetSetCount_ReturnsZero_WhenTargetSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetSetCount("some-key");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetSetCount_ReturnsTheNumberOfElements_InTheGivenSet()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value1");
                    x.AddToSet("key", "value2");
                    x.AddToSet("key", "value3");
                    x.AddToSet("key", "value3"); // Duplicate value, should be ignored
                });

                var result = connection.GetSetCount("key");

                Assert.Equal(3, result);
            });
        }

        [Fact]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetRangeFromSet_ReturnsNonNullEmptyCollection_WhenTargetSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetRangeFromSet("some-key", 0, 1);

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetRangeFromSet_ReturnsTheGivenRange_FromTheGivenSet()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "3", 3.0D);
                    x.AddToSet("key", "1", 1.0D);
                    x.AddToSet("key", "4", 4.0D);
                    x.AddToSet("key", "2", 2.0D);
                });

                var result = connection.GetRangeFromSet("key", 1, 2);

                Assert.Equal(new[] { "2", "3" }, result);
            });
        }

        [Fact]
        public void GetRangeFromSet_ReturnsEmptyRange_WhenStartingAt_GreaterThanTheNumberOfElements()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "4", 4.0D);
                    x.AddToSet("key", "1", 1.0D);
                });

                var result = connection.GetRangeFromSet("key", 3, 100);

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetRangeFromSet_ReadsToEnd_WhenEndingAt_IsGreaterThanTheNumberOfElements()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "3", 3.0D);
                    x.AddToSet("key", "1", 1.0D);
                    x.AddToSet("key", "4", 4.0D);
                    x.AddToSet("key", "2", 2.0D);
                });

                var result = connection.GetRangeFromSet("key", 2, 1000);

                Assert.Equal(new [] { "3", "4" }, result);
            });
        }

        [Fact]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromList(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetRangeFromList_ReturnsNonNullEmptyCollection_WhenTargetSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetRangeFromList("some-key", 0, 1);

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetRangeFromList_ReturnsTheGivenRange_FromTheGivenSet()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.InsertToList("key", "3");
                    x.InsertToList("key", "1");
                    x.InsertToList("key", "4");
                    x.InsertToList("key", "2");
                });

                var result = connection.GetRangeFromList("key", 1, 2);

                Assert.Equal(new[] { "4", "1" }, result);
            });
        }

        [Fact]
        public void GetRangeFromList_ReturnsEmptyRange_WhenStartingAt_GreaterThanTheNumberOfElements()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.InsertToList("key", "4");
                    x.InsertToList("key", "1");
                });

                var result = connection.GetRangeFromList("key", 3, 100);

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetRangeFromList_ReadsToEnd_WhenEndingAt_IsGreaterThanTheNumberOfElements()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.InsertToList("key", "3");
                    x.InsertToList("key", "1");
                    x.InsertToList("key", "4");
                    x.InsertToList("key", "2");
                });

                var result = connection.GetRangeFromList("key", 2, 1000);

                Assert.Equal(new[] { "1", "3" }, result);
            });
        }

        [Fact]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromList(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsNonNullEmptyCollection_WhenTargetListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromList("some-key");

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsAllElements_InTheCorrectOrder()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.InsertToList("key", "3");
                    x.InsertToList("key", "1");
                    x.InsertToList("key", "4");
                    x.InsertToList("key", "2");
                });

                var result = connection.GetAllItemsFromList("key");

                Assert.Equal(new [] { "2", "4", "1", "3" }, result);
            });
        }

        [Fact]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashCount(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetHashCount_ReturnsZero_WhenTargetHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetHashCount("some-key");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetHashCount_ReturnsTheNumberOfKeys_InTheGivenHash()
        {
            UseConnection(connection =>
            {
                Commit(connection, x => x.SetRangeInHash("key", new Dictionary<string, string>
                {
                    { "key1", "value1" },
                    { "key2", null },
                    { "key3", "" }
                }));

                var result = connection.GetHashCount("key");

                Assert.Equal(3, result);
            });
        }

        [Fact]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetCounter(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetCounter_ReturnsZero_WhenTargetCounterDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetCounter("some-key");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetCounter_ReturnsActualValue_WhenCounterExists()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.IncrementCounter("key");
                    x.IncrementCounter("key");
                    x.IncrementCounter("key");
                });

                var result = connection.GetCounter("key");

                Assert.Equal(3, result);
            });
        }

        [Fact]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashTtl(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetHashTtl_ReturnsNegativeValue_WhenTargetHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var expireIn = connection.GetHashTtl("some-key");
                Assert.True(expireIn < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetHashTtl_ReturnsNegativeValue_WhenTargetHashDoesExist_ButNotExpiring()
        {
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string>
                {
                    { "field", "value" }
                });

                var expireIn = connection.GetHashTtl("key");

                Assert.True(expireIn < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetHashTtl_ReturnsRelativeValue_ForExpiringHash()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.SetRangeInHash("key", new Dictionary<string, string> { { "field", "value" } });
                    x.ExpireHash("key", TimeSpan.FromSeconds(35));
                });

                var expireIn = connection.GetHashTtl("key");

                Assert.Equal(35, (int)expireIn.TotalSeconds);
            });
        }

        [Fact]
        public void GetHashTtl_DoesNotReturnNegativeValue_WhenTargetHashIsExpiring_AfterClockSkew()
        {
            UseConnection(connection =>
            {
                // Arrange
                Commit(connection, x =>
                {
                    x.SetRangeInHash("key", new Dictionary<string, string> { { "field", "value" } });
                    x.ExpireHash("key", TimeSpan.FromSeconds(35));
                });

                _now = _now.AddMinutes(5);

                // Act
                var expireIn = connection.GetHashTtl("key");

                // Assert
                Assert.True(expireIn >= TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetListTtl(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetListTtl_ReturnsNegativeValue_WhenTargetListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var expireIn = connection.GetListTtl("some-key");
                Assert.True(expireIn < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetListTtl_ReturnsNegativeValue_WhenTargetListDoesExist_ButNotExpiring()
        {
            UseConnection(connection =>
            {
                Commit(connection, x => x.InsertToList("key", "value"));

                var expireIn = connection.GetListTtl("key");

                Assert.True(expireIn < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetListTtl_ReturnsRelativeValue_ForExpiringList()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.InsertToList("key", "value");
                    x.ExpireList("key", TimeSpan.FromSeconds(35));
                });

                var expireIn = connection.GetListTtl("key");

                Assert.Equal(35, (int)expireIn.TotalSeconds);
            });
        }

        [Fact]
        public void GetListTtl_DoesNotReturnNegativeValue_WhenTargetListIsExpiring_AfterClockSkew()
        {
            UseConnection(connection =>
            {
                // Arrange
                Commit(connection, x =>
                {
                    x.InsertToList("key", "value");
                    x.ExpireList("key", TimeSpan.FromSeconds(35));
                });

                _now = _now.AddMinutes(5);

                // Act
                var expireIn = connection.GetListTtl("key");

                // Assert
                Assert.True(expireIn >= TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetTtl(null));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetSetTtl_ReturnsNegativeValue_WhenTargetSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var expireIn = connection.GetSetTtl("some-key");
                Assert.True(expireIn < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetSetTtl_ReturnsNegativeValue_WhenTargetSetDoesExist_ButNotExpiring()
        {
            UseConnection(connection =>
            {
                Commit(connection, x => x.AddToSet("key", "value"));

                var expireIn = connection.GetSetTtl("key");

                Assert.True(expireIn < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetSetTtl_ReturnsRelativeValue_ForExpiringSet()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value");
                    x.ExpireSet("key", TimeSpan.FromSeconds(35));
                });

                var expireIn = connection.GetSetTtl("key");

                Assert.Equal(35, (int)expireIn.TotalSeconds);
            });
        }

        [Fact]
        public void GetSetTtl_DoesNotReturnNegativeValue_WhenTargetSetIsExpiring_AfterClockSkew()
        {
            UseConnection(connection =>
            {
                // Arrange
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value");
                    x.ExpireSet("key", TimeSpan.FromSeconds(35));
                });

                _now = _now.AddMinutes(5);

                // Act
                var expireIn = connection.GetSetTtl("key");

                // Assert
                Assert.True(expireIn >= TimeSpan.Zero);
            });
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer(null, new ServerContext()));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer("some-id", null));

                Assert.Equal("context", exception.ParamName);
            });
        }

        [Fact]
        public void AnnounceServer_AddsTheGivenServer_WithCorrectDetails()
        {
            UseConnection(connection =>
            {
                // Arrange
                var queues = new[] { "critical", "default" };

                // Act
                connection.AnnounceServer("some-id", new ServerContext { Queues = queues, WorkerCount = 27 });

                // Assert
                Assert.True(_state.Servers.ContainsKey("some-id"));

                var entry = _state.Servers["some-id"];

                Assert.Equal(queues, entry.Context.Queues);
                Assert.NotSame(queues, entry.Context.Queues);
                Assert.Equal(27, entry.Context.WorkerCount);
                Assert.Equal(_now, entry.StartedAt);
                Assert.Equal(_now, entry.HeartbeatAt);
            });
        }

        [Fact]
        public void AnnounceServer_DoesNotThrow_OnRetry()
        {
            UseConnection(connection =>
            {
                connection.AnnounceServer("some-id", new ServerContext());
                connection.AnnounceServer("some-id", new ServerContext());
            });
        }

        [Fact]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.RemoveServer(null));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact]
        public void RemoveServer_DoesNotThrow_WhenGivenServerDoesNotExist()
        {
            UseConnection(connection =>
            {
                connection.RemoveServer("some-server");
                Assert.Empty(_state.Servers);
            });
        }

        [Fact]
        public void RemoveServer_RemovesServerWithTheGivenServerId()
        {
            UseConnection(connection =>
            {
                connection.AnnounceServer("some-server", new ServerContext());
                Assert.NotEmpty(_state.Servers);

                connection.RemoveServer("some-server");
                Assert.Empty(_state.Servers);
            });
        }

        [Fact]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.Heartbeat(null));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact]
        public void Heartbeat_UpdatesHeartbeat_OfTheGivenServer()
        {
            UseConnection(connection =>
            {
                connection.AnnounceServer("some-server", new ServerContext());
                _now = _now.AddMinutes(32);

                connection.Heartbeat("some-server");

                Assert.Equal(_now, _state.Servers["some-server"].HeartbeatAt);
                Assert.Equal(_now.AddMinutes(-32), _state.Servers["some-server"].StartedAt);
            });
        }

        [Fact]
        public void Heartbeat_ThrowsBackgroundServerGoneException_WhenGivenServerDoesNotExist()
        {
            UseConnection(connection =>
            {
                Assert.Throws<BackgroundServerGoneException>(
                    () => connection.Heartbeat("some-id"));
            });
        }

        [Fact]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeoutIsNegative()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(
                    () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-1)));

                Assert.Equal("timeout", exception.ParamName);
            });
        }

        [Fact]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeoutIsZero()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(
                    () => connection.RemoveTimedOutServers(TimeSpan.Zero));

                Assert.Equal("timeout", exception.ParamName);
            });
        }

        [Fact]
        public void RemoveTimedOutServers_WorksCorrectly()
        {
            UseConnection(connection =>
            {
                // Arrange
                connection.AnnounceServer("server-1", new ServerContext());
                connection.AnnounceServer("server-2", new ServerContext());
                _state.Servers["server-2"].HeartbeatAt = _now.AddMinutes(-30);
                connection.AnnounceServer("server-3", new ServerContext());
                _state.Servers["server-3"].HeartbeatAt = _now.AddMinutes(-5);
                connection.AnnounceServer("server-4", new ServerContext());
                _state.Servers["server-4"].HeartbeatAt = _now.AddMinutes(-60);

                // Act
                var result = connection.RemoveTimedOutServers(TimeSpan.FromMinutes(15));

                // Assert
                Assert.Equal(2, result);
                Assert.Equal(new [] { "server-1", "server-3" }, _state.Servers.Keys.ToArray());
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        // TODO: Remove GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore from SQL Server implementation
        // TODO: Also remove GetFirstByLowestScoreFromSet_ThrowsArgException_WhenRequestingLessThanZero there

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTargetSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetFirstByLowestScoreFromSet("some-key", 0, 1);
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsAnElementWithTheLowestScore()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value2", 2.0D);
                    x.AddToSet("key", "value1", 1.0D);
                    x.AddToSet("key", "value3", 3.0D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", 0.0D, 5.0D);

                Assert.Equal("value1", result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_IgnoresElementsOutsideOfTheGivenRange()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value1", -100.0D);
                    x.AddToSet("key", "value2", 50.0D);
                    x.AddToSet("key", "value3", -23.0D);
                    x.AddToSet("key", "value4", 125.0D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", -25.0D, 100.0D);

                Assert.Equal("value3", result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenNoElementMatchesTheGivenRange()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value1", -100.0D);
                    x.AddToSet("key", "value2", 50.0D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", 200.0D, 500.0D);

                Assert.Null(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_UsesStrictComparisonOperations_ForFromScoreArgument_WithNegativeValues()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value1", -2.01D);
                    x.AddToSet("key", "value2", -2.00D);
                    x.AddToSet("key", "value3", 2.0D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", -2.0D, 2.0D);

                Assert.Equal("value2", result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_UsesStrictComparisonOperations_ForFromScoreArgument_WithPositiveValues()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value1", 2.01D);
                    x.AddToSet("key", "value2", 2.00D);
                    x.AddToSet("key", "value3", 2.1D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", 2.0D, 2.01D);

                Assert.Equal("value2", result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_UsesStrictComparisonOperations_ForToScoreArgument()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value2", -2.00D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", -100.0D, -2.0D);

                Assert.Equal("value2", result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_WithCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1, 10));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_WithCount_ReturnsEmptyCollection_WhenTargetSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetFirstByLowestScoreFromSet("some-key", 0, 1, 10);
                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_WithCount_ReturnsTheGivenRange_FromTheGivenSet()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "3", 3.0D);
                    x.AddToSet("key", "1", 1.0D);
                    x.AddToSet("key", "4", 4.0D);
                    x.AddToSet("key", "2", 2.0D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", 2.0D, 10.0D, 2);

                Assert.Equal(new[] { "2", "3" }, result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_WithCount_ReturnsEmptyRange_WhenStartingAt_GreaterThanTheNumberOfElements()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "4", 4.0D);
                    x.AddToSet("key", "1", 1.0D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", 5.0D, 10.0D, 10);

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_WithCount_ReadsToEnd_WhenEndingAt_IsGreaterThanTheNumberOfElements()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "3", 3.0D);
                    x.AddToSet("key", "1", 1.0D);
                    x.AddToSet("key", "4", 4.0D);
                    x.AddToSet("key", "2", 2.0D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", 3.0D, 10.0D, 10);

                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_WithCount_UsesStrictComparisonOperations()
        {
            UseConnection(connection =>
            {
                Commit(connection, x =>
                {
                    x.AddToSet("key", "value1", -2.01D);
                    x.AddToSet("key", "value2", -2.00D);
                    x.AddToSet("key", "value3", 2.0D);
                    x.AddToSet("key", "value4", 2.01D);
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", -2.0D, 2.0D, 10);

                Assert.Equal(new [] { "value2", "value3" }, result);
            });
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash(null, "name"));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash("key", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void GetValueFromHash_ReturnsNull_WhenTargetHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetValueFromHash("some-key", "name");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetValueFromHash_ReturnsValue_WhenTargetHashAndFieldExist()
        {
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string> { { "name", "value" } });

                var result = connection.GetValueFromHash("key", "name");

                Assert.Equal("value", result);
            });
        }

        [Fact]
        public void GetValueFromHash_ReturnsNull_WhenTargetHashExists_ButGivenFieldDoesNot()
        {
            UseConnection(connection =>
            {
                connection.SetRangeInHash("key", new Dictionary<string, string> {{ "name", "value" }});

                var result = connection.GetValueFromHash("key", "another-name");

                Assert.Null(result);
            });
        }

        [Fact]
        public void FetchNextJob_ThrowsAnException_WhenQueuesArgumentIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.FetchNextJob(null, CancellationToken.None));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        public void FetchNextJob_ThrowsAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(
                    () => connection.FetchNextJob(new string[0], CancellationToken.None));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        public void FetchNextJob_ThrowsAnException_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseConnection(connection =>
            {
                using (var cts = new CancellationTokenSource())
                {
                    cts.Cancel();

                    Assert.Throws<OperationCanceledException>(() => connection.FetchNextJob(
                        new [] { "default" },
                        // ReSharper disable once AccessToDisposedClosure
                        cts.Token));
                }
            });
        }

        [Fact]
        public void FetchNextJob_WaitsIndefinitely_OnCancellationToken_WhenThereAreNoJobs()
        {
            UseConnection(connection =>
            {
                using (var cts = new CancellationTokenSource(millisecondsDelay: 500))
                {
                    Assert.Throws<OperationCanceledException>(() => connection.FetchNextJob(
                        new [] { "default" },
                        // ReSharper disable once AccessToDisposedClosure
                        cts.Token));
                }
            });
        }

        [Fact]
        public void FetchNextJob_ReturnsJobIdFromTheGivenQueue()
        {
            UseConnection(connection =>
            {
                Commit(connection, x => x.AddToQueue("default", "some-id"));

                using (var result = connection.FetchNextJob(new[] {"default"}, CancellationToken.None))
                {
                    Assert.Equal("some-id", result.JobId);
                }
            });
        }

        [Fact]
        public void FetchNextJob_ReturnsTheEarliestQueuedJobId()
        {
            UseConnection(connection =>
            {
                Commit(connection, x => x.AddToQueue("default", "job-1"));
                Commit(connection, x => x.AddToQueue("default", "job-2"));

                using (var result = connection.FetchNextJob(new[] {"default"}, CancellationToken.None))
                {
                    Assert.Equal("job-1", result.JobId);
                }
            });
        }

        [Fact]
        public void FetchNextJob_IsWaitingForJobIds_ToBeQueued()
        {
            UseConnection(connection =>
            {
                using (var cts = new CancellationTokenSource(millisecondsDelay: 500))
                {
                    cts.Token.Register(() =>
                    {
                        Commit(connection, x => x.AddToQueue("default", "job-id"));
                    });

                    using (var result = connection.FetchNextJob(new[] {"default"}, CancellationToken.None))
                    {
                        Assert.Equal("job-id", result.JobId);
                    }
                }
            });
        }

        [Fact]
        public void FetchNextJob_CanFetchJobIdsFromMultipleQueues_InTheGivenOrder()
        {
            UseConnection(connection =>
            {
                // Arrange
                Commit(connection, x => x.AddToQueue("default", "1"));
                Commit(connection, x => x.AddToQueue("critical", "2"));

                var queues = new[] { "critical", "default" };

                // Act
                using (var job1 = connection.FetchNextJob(queues, CancellationToken.None))
                using (var job2 = connection.FetchNextJob(queues, CancellationToken.None))
                {
                    // Assert
                    Assert.Equal("2", job1.JobId);
                    Assert.Equal("1", job2.JobId);
                }
            });
        }

        [Fact]
        public void FetchNextJob_IsWaitingForJobIds_ToBeQueuedWhenUsingMultipleQueues()
        {
            UseConnection(connection =>
            {
                using (var cts = new CancellationTokenSource(millisecondsDelay: 500))
                {
                    // Arrange
                    cts.Token.Register(() =>
                    {
                        Commit(connection, x => x.AddToQueue("default", "1"));
                        Thread.Sleep(millisecondsTimeout: 100);
                        Commit(connection, x => x.AddToQueue("critical", "2"));
                    });

                    var queues = new[] { "critical", "default" };

                    // Act
                    using (var job1 = connection.FetchNextJob(queues, CancellationToken.None))
                    using (var job2 = connection.FetchNextJob(queues, CancellationToken.None))
                    {
                        // Assert
                        Assert.Equal("1", job1.JobId);
                        Assert.Equal("2", job2.JobId);
                    }
                }
            });
        }

        private void UseConnection(Action<InMemoryConnection> action)
        {
            using (var connection = CreateConnection())
            {
                action(connection);
            }
        }

        private static void Commit(IStorageConnection connection, Action<JobStorageTransaction> action)
        {
            using (var transaction = connection.CreateWriteTransaction())
            {
                action((JobStorageTransaction)transaction);
                transaction.Commit();
            }
        }

        private InMemoryConnection CreateConnection()
        {
            return new InMemoryConnection(new InMemoryDispatcherBase(_state));
        }

#pragma warning disable xUnit1013 // Public method should be marked as test
        public void MyMethod(string arg)
#pragma warning restore xUnit1013 // Public method should be marked as test
        {
        }
    }
}
