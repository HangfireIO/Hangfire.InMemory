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

        [Fact]
        public void Queues_ReturnEmptyQueue_WhenQueueExistWithoutAnyJobs()
        {
            // Arrange
            _state.QueueGetOrCreate("default");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var defaultQueue = result.Single();

            Assert.Equal("default", defaultQueue.Name);
            Assert.Equal(0, defaultQueue.Length);
            Assert.Null(defaultQueue.Fetched);
            Assert.Empty(defaultQueue.FirstJobs);
        }

        [Fact]
        public void Queues_ReturnsCorrectJobs_WhichWillBeDequeuedNext()
        {
            // Arrange
            var jobId = SimpleEnqueueJob(
                "critical",
                job: Job.FromExpression<ITestServices>(x => x.Empty()),
                state: new EnqueuedState());

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var criticalQueue = result.Single();
            var queuedJob = criticalQueue.FirstJobs.Single();

            Assert.Equal("critical", criticalQueue.Name);
            Assert.Equal(1, criticalQueue.Length);
            Assert.Equal(jobId, queuedJob.Key);

            Assert.True(queuedJob.Value.InEnqueuedState);
            Assert.Equal("Enqueued", queuedJob.Value.State, StringComparer.OrdinalIgnoreCase);
            Assert.Equal(_now, queuedJob.Value.EnqueuedAt);

            Assert.Equal(typeof(ITestServices), queuedJob.Value.Job.Type);
            Assert.Equal("Empty", queuedJob.Value.Job.Method.Name);
        }

        [Fact]
        public void Queues_IsAbleToHandle_JobIdWithoutCorrespondingBackgroundJobEntry()
        {
            // Arrange
            SimpleEnqueueJob("default", jobId: "some-job");
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var someJob = result.Single().FirstJobs.Single();

            Assert.Equal("some-job", someJob.Key);
            Assert.Null(someJob.Value.Job);
            Assert.False(someJob.Value.InEnqueuedState);
            Assert.Null(someJob.Value.State);
            Assert.Null(someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void Queues_IsAbleToHandle_BackgroundJobEntry_WithNullState()
        {
            // Arrange
            var jobId = SimpleEnqueueJob("test", state: null);
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var someJob = result.Single().FirstJobs.Single();

            Assert.Equal(jobId, someJob.Key);
            Assert.False(someJob.Value.InEnqueuedState);
            Assert.Null(someJob.Value.State);
            Assert.Null(someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void Queues_IsAbleToHandle_BackgroundJobEntry_WithAnotherState()
        {
            // Arrange
            var jobId = SimpleEnqueueJob("default", state: new DeletedState());
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var someJob = result.Single().FirstJobs.Single();

            Assert.Equal(jobId, someJob.Key);
            Assert.False(someJob.Value.InEnqueuedState);
            Assert.Equal("Deleted", someJob.Value.State, StringComparer.OrdinalIgnoreCase);
            Assert.Null(someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void Queues_IsAbleToHandle_EnqueuedLikeStates_AsEnqueued()
        {
            // Arrange
            var state = new Mock<IState>();
            state.SetupGet(x => x.Name).Returns("EnQUEued");

            var jobId = SimpleEnqueueJob("default", state: state.Object);
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var someJob = result.Single().FirstJobs.Single();

            Assert.Equal(jobId, someJob.Key);
            Assert.True(someJob.Value.InEnqueuedState);
            Assert.Equal("EnQUEued", someJob.Value.State);
            Assert.Equal(_now, someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void Queues_ReturnsTop5Jobs_FromItsHead()
        {
            // Arrange
            var jobId1 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId2 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId3 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId4 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId5 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId6 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId7 = SimpleEnqueueJob("critical", state: new EnqueuedState());

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var defaultQueue = result.Single(x => x.Name == "default");
            Assert.Equal(6, defaultQueue.Length);
            Assert.Equal(5, defaultQueue.FirstJobs.Count);
            Assert.Equal(jobId1, defaultQueue.FirstJobs.First().Key);
            Assert.Equal(jobId5, defaultQueue.FirstJobs.Last().Key);

            var criticalQueue = result.Single(x => x.Name == "critical");
            Assert.Equal(1, criticalQueue.Length);
            Assert.Single(criticalQueue.FirstJobs);
            Assert.Equal(jobId7, criticalQueue.FirstJobs.Single().Key);
        }

        [Fact]
        public void Queues_IsAbleToHandleSerializationProblems_InJobs()
        {
            // Arrange
            var jobId = SimpleEnqueueJob("default", state: new EnqueuedState());
            _state.Jobs[jobId].InvocationData = new InvocationData("asfasf", "232", "afasf", "gg");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues();

            // Assert
            var queuedJob = result.Single().FirstJobs.Single();
            Assert.Equal(jobId, queuedJob.Key);
            Assert.Null(queuedJob.Value.Job);
            Assert.True(queuedJob.Value.InEnqueuedState);
            Assert.Equal("Enqueued", queuedJob.Value.State, StringComparer.OrdinalIgnoreCase);
        }

        [Fact]
        public void Servers_ReturnsEmptyCollection_WhenThereAreNoServers()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.Servers();

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void JobDetails_ThrowsAnException_WhenJobIdIsNull()
        {
            var monitoring = CreateMonitoringApi();

            var exception = Assert.Throws<ArgumentNullException>(
                () => monitoring.JobDetails(null));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void JobDetails_ReturnsNull_WhenTargetJobDoesNotExist()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.JobDetails("some-job");

            Assert.Null(result);
        }

        [Fact]
        public void GetStatistics_ReturnsEmptyStatistics_WhenNothingIsCreatedYet()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.GetStatistics();

            Assert.Equal(0, result.Deleted);
            Assert.Equal(0, result.Enqueued);
            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.Processing);
            Assert.Equal(0, result.Queues);
            Assert.Equal(0, result.Recurring);
            Assert.Equal(0, result.Scheduled);
            Assert.Equal(0, result.Servers);
            Assert.Equal(0, result.Succeeded);
        }

        [Fact]
        public void EnqueuedJobs_ThrowsAnException_WhenQueueNamesIsNull()
        {
            var monitoring = CreateMonitoringApi();

            var exception = Assert.Throws<ArgumentNullException>(
                () => monitoring.EnqueuedJobs(null, 0, 10));

            Assert.Equal("queueName", exception.ParamName);
        }

        [Fact]
        public void EnqueuedJobs_ReturnsEmptyCollection_WhenThereIsNoSuchQueue()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.EnqueuedJobs("critical", 0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void FetchedJobs_ThrowsAnException_WhenQueueIsNull()
        {
            var monitoring = CreateMonitoringApi();

            var exception = Assert.Throws<ArgumentNullException>(
                () => monitoring.FetchedJobs(null, 0, 10));

            Assert.Equal("queueName", exception.ParamName);
        }

        [Fact]
        public void FetchedJobs_ReturnsEmptyCollection()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.FetchedJobs("default", 0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void ProcessingJobs_ReturnsEmptyCollection_WhenThereAreNoProcessingJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.ProcessingJobs(0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void ScheduledJobs_ReturnsEmptyCollection_WhenThereAreNoScheduledJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.ScheduledJobs(0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void SucceededJobs_ReturnsEmptyCollection_WhenThereAreNoSucceededJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.SucceededJobs(0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void FailedJobs_ReturnsEmptyCollection_WhenThereAreNoFailedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.FailedJobs(0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void DeletedJobs_ReturnsEmptyCollection_WhenThereAreNoDeletedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.DeletedJobs(0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void ScheduledCount_ReturnsZero_WhenThereAreNoScheduledJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.ScheduledCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void EnqueuedCount_ThrowsAnException_WhenQueueNameIsNull()
        {
            var monitoring = CreateMonitoringApi();

            var exception = Assert.Throws<ArgumentNullException>(
                () => monitoring.EnqueuedCount(null));

            Assert.Equal("queueName", exception.ParamName);
        }

        [Fact]
        public void EnqueuedCount_ReturnsZero_WhenTargetQueueDoesNotExist()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.EnqueuedCount("critical");

            Assert.Equal(0, result);
        }

        [Fact]
        public void FetchedCount_ThrowsAnException_WhenQueueIsNull()
        {
            var monitoring = CreateMonitoringApi();

            var exception = Assert.Throws<ArgumentNullException>(
                () => monitoring.FetchedCount(null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void FetchedCount_ReturnsZero_WhenTargetQueueDoesNotExist()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.FetchedCount("critical");

            Assert.Equal(0, result);
        }

        [Fact]
        public void FailedCount_ReturnsZero_WhenThereAreNoFailedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.FailedCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void ProcessingCount_ReturnsZero_WhenThereAreNoProcessingJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.ProcessingCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void SucceededListCount_ReturnsZero_WhenThereAreNoSucceededJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.SucceededListCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void DeletedListCount_ReturnsZero_WhenThereAreNoDeletedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.DeletedListCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void SucceededByDatesCount_ReturnsEntriesForTheWholeWeek_EvenWhenThereAreNoSucceededJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.SucceededByDatesCount();

            Assert.Equal(7, result.Count);
            Assert.Equal(0, result[_now.Date]);
            Assert.Equal(0, result[_now.Date.AddDays(-1)]);
            Assert.Equal(0, result[_now.Date.AddDays(-2)]);
            Assert.Equal(0, result[_now.Date.AddDays(-3)]);
            Assert.Equal(0, result[_now.Date.AddDays(-4)]);
            Assert.Equal(0, result[_now.Date.AddDays(-5)]);
            Assert.Equal(0, result[_now.Date.AddDays(-6)]);
        }

        [Fact]
        public void FailedByDatesCount_ReturnsEntriesForTheWholeWeek_EvenWhenThereAreNoFailedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.FailedByDatesCount();

            Assert.Equal(7, result.Count);
            Assert.Equal(0, result[_now.Date]);
            Assert.Equal(0, result[_now.Date.AddDays(-1)]);
            Assert.Equal(0, result[_now.Date.AddDays(-2)]);
            Assert.Equal(0, result[_now.Date.AddDays(-3)]);
            Assert.Equal(0, result[_now.Date.AddDays(-4)]);
            Assert.Equal(0, result[_now.Date.AddDays(-5)]);
            Assert.Equal(0, result[_now.Date.AddDays(-6)]);
        }

        [Fact]
        public void HourlySucceededJobs_ReturnsEntriesForTheWholeDay_EvenWhenThereAreNoSucceededJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.HourlySucceededJobs();

            Assert.Equal(24, result.Count);
            Assert.Equal(0, result[_now]);
            Assert.Equal(0, result[_now.AddHours(-3)]);
            Assert.Equal(0, result[_now.AddHours(-6)]);
            Assert.Equal(0, result[_now.AddHours(-9)]);
            Assert.Equal(0, result[_now.AddHours(-12)]);
            Assert.Equal(0, result[_now.AddHours(-15)]);
            Assert.Equal(0, result[_now.AddHours(-18)]);
            Assert.Equal(0, result[_now.AddHours(-21)]);
        }

        [Fact]
        public void HourlyFailedJobs_ReturnsEntriesForTheWholeDay_EvenWhenThereAreNoFailedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.HourlyFailedJobs();

            Assert.Equal(24, result.Count);
            Assert.Equal(0, result[_now]);
            Assert.Equal(0, result[_now.AddHours(-3)]);
            Assert.Equal(0, result[_now.AddHours(-6)]);
            Assert.Equal(0, result[_now.AddHours(-9)]);
            Assert.Equal(0, result[_now.AddHours(-12)]);
            Assert.Equal(0, result[_now.AddHours(-15)]);
            Assert.Equal(0, result[_now.AddHours(-18)]);
            Assert.Equal(0, result[_now.AddHours(-21)]);
        }

        private string SimpleEnqueueJob(string queue, string jobId = null, IState state = null, Job job = null)
        {
            var createdId = UseConnection(connection =>
            {
                jobId = jobId ?? connection.CreateExpiredJob(
                    job ?? Job.FromExpression<ITestServices>(x => x.Empty()),
                    new Dictionary<string, string>(),
                    _now,
                    TimeSpan.Zero);

                using (var transaction = connection.CreateWriteTransaction())
                {
                    if (state != null) transaction.SetJobState(jobId, state);
                    transaction.AddToQueue(queue, jobId);
                    transaction.Commit();
                }

                return jobId;
            });
            return createdId;
        }

        private InMemoryMonitoringApi CreateMonitoringApi()
        {
            return new InMemoryMonitoringApi(new InMemoryDispatcherBase(_state));
        }

        private T UseConnection<T>(Func<InMemoryConnection, T> action)
        {
            using (var connection = new InMemoryConnection(new InMemoryDispatcherBase(_state)))
            {
                return action(connection);
            }
        }
    }
}
