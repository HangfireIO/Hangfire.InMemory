// This file is part of Hangfire.InMemory. Copyright © 2020 Hangfire OÜ.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryMonitoringApiFacts
    {
        private readonly InMemoryStorageOptions _options;
        private readonly InMemoryState _state;
        private MonotonicTime _now;

        public InMemoryMonitoringApiFacts()
        {
            _options = new InMemoryStorageOptions { StringComparer = StringComparer.Ordinal };
            _now = MonotonicTime.GetCurrent();
            _state = new InMemoryState(() => _now, _options);
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
            AssertWithinSecond(_now.ToUtcDateTime(), queuedJob.Value.EnqueuedAt);

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
            AssertWithinSecond(_now.ToUtcDateTime(), someJob.Value.EnqueuedAt);
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
        public void Queues_ProducesSortedList_RegardlessOfActualEnqueueOrder()
        {
            // Arrange
            SimpleEnqueueJob("b-queue", state: new EnqueuedState());
            SimpleEnqueueJob("a-queue", state: new EnqueuedState());
            SimpleEnqueueJob("c-queue", state: new EnqueuedState());

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Queues().ToArray();

            // Assert
            Assert.Equal("a-queue", result[0].Name);
            Assert.Equal("b-queue", result[1].Name);
            Assert.Equal("c-queue", result[2].Name);
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
        public void Servers_ReturnsAllTheRegisteredServers_WithCorrectDetails()
        {
            // Arrange
            UseConnection(connection =>
            {
                connection.AnnounceServer("server1", new ServerContext { Queues = new []{ "default" }, WorkerCount = 100 });
                connection.AnnounceServer("server2", new ServerContext { Queues = new[] { "critical" }, WorkerCount = 17 });
                connection.AnnounceServer("server3", new ServerContext { Queues = new[] { "alpha", "beta" }, WorkerCount = 0 });
                return true;
            });

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Servers();

            // Assert
            Assert.Equal(3, result.Count);

            var server1 = result.Single(x => x.Name == "server1");
            Assert.Equal(new [] { "default" }, server1.Queues);
            Assert.Equal(100, server1.WorkersCount);
            AssertWithinSecond(_now.ToUtcDateTime(), server1.StartedAt);
            AssertWithinSecond(_now.ToUtcDateTime(), server1.Heartbeat);

            var server2 = result.Single(x => x.Name == "server2");
            Assert.Equal(new[] { "critical" }, server2.Queues);
            Assert.Equal(17, server2.WorkersCount);
            AssertWithinSecond(_now.ToUtcDateTime(), server2.StartedAt);
            AssertWithinSecond(_now.ToUtcDateTime(), server2.Heartbeat);

            var server3 = result.Single(x => x.Name == "server3");
            Assert.Equal(new[] { "alpha", "beta" }, server3.Queues);
            Assert.Equal(0, server3.WorkersCount);
            AssertWithinSecond(_now.ToUtcDateTime(), server3.StartedAt);
            AssertWithinSecond(_now.ToUtcDateTime(), server3.Heartbeat);
        }

        [Fact]
        public void Servers_ProducesSortedList_RegardlessOfActualAnnouncementOrder()
        {
            // Arrange
            UseConnection(connection =>
            {
                connection.AnnounceServer("server3", new ServerContext { Queues = new[] { "default" }, WorkerCount = 4 });
                connection.AnnounceServer("server1", new ServerContext { Queues = new[] { "default" }, WorkerCount = 4 });
                connection.AnnounceServer("server2", new ServerContext { Queues = new[] { "default" }, WorkerCount = 4 });
                return true;
            });

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.Servers().ToArray();

            // Assert
            Assert.Equal("server1", result[0].Name);
            Assert.Equal("server2", result[1].Name);
            Assert.Equal("server3", result[2].Name);
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
        public void JobDetails_CorrectlyHandles_CreatedButNotInitializedJob()
        {
            // Arrange
            var jobId = UseConnection(connection => connection.CreateExpiredJob(
                    Job.FromExpression<ITestServices>(x => x.Empty()),
                    new Dictionary<string, string> { { "CurrentCulture", "en-US" }, { "RetryCount", "5" } },
                    _now.ToUtcDateTime(),
                    TimeSpan.FromMinutes(37)));

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.JobDetails(jobId);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(typeof(ITestServices), result.Job.Type);
            Assert.Equal("Empty", result.Job.Method.Name);
            Assert.Equal("en-US", result.Properties["CurrentCulture"]);
            Assert.Equal("5", result.Properties["RetryCount"]);
            AssertWithinSecond(_now.ToUtcDateTime(), result.CreatedAt);
            AssertWithinSecond(_now.ToUtcDateTime().AddMinutes(37), result.ExpireAt);
        }

        [Fact]
        public void JobDetails_CorrectlyShowsCreated_AndInitializedJob()
        {
            // Arrange
            var createdId = UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression<ITestServices>(x => x.Empty()),
                    new Dictionary<string, string>(), 
                    _now.ToUtcDateTime(),
                    TimeSpan.Zero);

                using (var transaction = connection.CreateWriteTransaction())
                {
                    transaction.SetJobState(jobId, new EnqueuedState("critical") { Reason = "Some reason" });
                    transaction.Commit();
                }

                _now = _now.Add(TimeSpan.FromMinutes(1));

                using (var transaction = connection.CreateWriteTransaction())
                {
                    transaction.SetJobState(jobId, new DeletedState());
                    transaction.Commit();
                }

                return jobId;
            });

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.JobDetails(createdId);

            // Assert
            Assert.Equal("Deleted", result.History.First().StateName);
            Assert.Null(result.History.First().Reason);
            AssertWithinSecond(_now.ToUtcDateTime(), result.History.First().CreatedAt);

            Assert.Equal("Enqueued", result.History.Last().StateName, StringComparer.OrdinalIgnoreCase);
            Assert.Equal("Some reason", result.History.Last().Reason);
            Assert.Equal("critical", result.History.Last().Data["Queue"]);
            AssertWithinSecond(_now.Add(TimeSpan.FromMinutes(-1)).ToUtcDateTime(), result.History.Last().CreatedAt);
        }

        [Fact]
        public void JobDetails_IsAbleToHandleSerializationProblems()
        {
            // Arrange
            var jobId = UseConnection(connection => connection.CreateExpiredJob(
                Job.FromExpression<ITestServices>(x => x.Empty()),
                new Dictionary<string, string>(),
                _now.ToUtcDateTime(),
                TimeSpan.Zero));

            _state.Jobs[jobId].InvocationData = new InvocationData("asfasf", "232", "afasf", "gg");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.JobDetails(jobId);

            // Assert
            Assert.NotNull(result);
            Assert.Null(result.Job);
            Assert.Empty(result.Properties);
            Assert.Empty(result.History);
            AssertWithinSecond(_now.ToUtcDateTime(), result.CreatedAt);
            AssertWithinSecond(_now.ToUtcDateTime(), result.ExpireAt);
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
            Assert.Equal(0, result.Retries);
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
        public void EnqueuedJobs_ReturnsCorrectJobs_WhichWillBeDequeuedNext()
        {
            // Arrange
            var jobId = SimpleEnqueueJob(
                "critical",
                job: Job.FromExpression<ITestServices>(x => x.Empty()),
                state: new EnqueuedState());

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("critical", 0, 10);

            // Assert
            var queuedJob = result.Single();

            Assert.Equal(jobId, queuedJob.Key);
            Assert.True(queuedJob.Value.InEnqueuedState);
            Assert.Equal("Enqueued", queuedJob.Value.State, StringComparer.OrdinalIgnoreCase);
            AssertWithinSecond(_now.ToUtcDateTime(), queuedJob.Value.EnqueuedAt);

            Assert.Equal(typeof(ITestServices), queuedJob.Value.Job.Type);
            Assert.Equal("Empty", queuedJob.Value.Job.Method.Name);
        }

        [Fact]
        public void EnqueuedJobs_ReturnsJobs_InTheAscendingOrder()
        {
            // Arrange
            var jobId1 = SimpleEnqueueJob("critical");
            var jobId2 = SimpleEnqueueJob("critical");
            var jobId3 = SimpleEnqueueJob("critical");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("critical", 0, 10).ToArray();

            // Assert
            Assert.Equal(jobId1, result[0].Key);
            Assert.Equal(jobId2, result[1].Key);
            Assert.Equal(jobId3, result[2].Key);
        }

        [Fact]
        public void EnqueuedJobs_IsAbleToHandle_JobIdWithoutCorrespondingBackgroundJobEntry()
        {
            // Arrange
            SimpleEnqueueJob("default", jobId: "some-job");
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("default", 0, 10);

            // Assert
            var someJob = result.Single();

            Assert.Equal("some-job", someJob.Key);
            Assert.Null(someJob.Value.Job);
            Assert.False(someJob.Value.InEnqueuedState);
            Assert.Null(someJob.Value.State);
            Assert.Null(someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void EnqueuedJobs_IsAbleToHandle_BackgroundJobEntry_WithNullState()
        {
            // Arrange
            var jobId = SimpleEnqueueJob("test", state: null);
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("test", 0, 10);

            // Assert
            var someJob = result.Single();

            Assert.Equal(jobId, someJob.Key);
            Assert.False(someJob.Value.InEnqueuedState);
            Assert.Null(someJob.Value.State);
            Assert.Null(someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void EnqueuedJobs_IsAbleToHandle_BackgroundJobEntry_WithAnotherState()
        {
            // Arrange
            var jobId = SimpleEnqueueJob("default", state: new DeletedState());
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("default", 0, 10);

            // Assert
            var someJob = result.Single();

            Assert.Equal(jobId, someJob.Key);
            Assert.False(someJob.Value.InEnqueuedState);
            Assert.Equal("Deleted", someJob.Value.State, StringComparer.OrdinalIgnoreCase);
            Assert.Null(someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void EnqueuedJobs_IsAbleToHandle_EnqueuedLikeStates_AsEnqueued()
        {
            // Arrange
            var state = new Mock<IState>();
            state.SetupGet(x => x.Name).Returns("EnQUEued");

            var jobId = SimpleEnqueueJob("default", state: state.Object);
            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("default", 0, 10);

            // Assert
            var someJob = result.Single();

            Assert.Equal(jobId, someJob.Key);
            Assert.True(someJob.Value.InEnqueuedState);
            Assert.Equal("EnQUEued", someJob.Value.State);
            AssertWithinSecond(_now.ToUtcDateTime(), someJob.Value.EnqueuedAt);
        }

        [Fact]
        public void EnqueuedJobs_ReturnsJobs_WithinTheGivenRange_FromTheGivenQueue()
        {
            // Arrange
            var jobId1 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId2 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobIdX = SimpleEnqueueJob("critical", state: new EnqueuedState());
            var jobId3 = SimpleEnqueueJob("default", state: new EnqueuedState());
            var jobId4 = SimpleEnqueueJob("default", state: new EnqueuedState());

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("default", 1, 2);

            // Assert
            Assert.Equal(2, result.Count);
            Assert.Equal(jobId2, result.First().Key);
            Assert.Equal(jobId3, result.Last().Key);
        }

        [Fact]
        public void EnqueuedJobs_IsAbleToHandleSerializationProblems_InJobs()
        {
            // Arrange
            var jobId = SimpleEnqueueJob("default", state: new EnqueuedState());
            _state.Jobs[jobId].InvocationData = new InvocationData("asfasf", "232", "afasf", "gg");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedJobs("default", 0, 10);

            // Assert
            var queuedJob = result.Single();
            Assert.Equal(jobId, queuedJob.Key);
            Assert.Null(queuedJob.Value.Job);
            Assert.True(queuedJob.Value.InEnqueuedState);
            Assert.Equal("Enqueued", queuedJob.Value.State, StringComparer.OrdinalIgnoreCase);
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
        public void ProcessingJobs_ReturnsCorrectJobs_InTheProcessingState()
        {
            // Arrange
            var jobId = SimpleProcessingJob("server-1", "worker-1", job: Job.FromExpression<ITestServices>(x => x.Empty()));
            var monitoring = CreateMonitoringApi();
            
            // Act
            var result = monitoring.ProcessingJobs(0, 10);

            // Assert
            var processingJob = result.Single();
            
            Assert.Equal(jobId, processingJob.Key);
            Assert.True(processingJob.Value.InProcessingState);
            Assert.Equal("server-1", processingJob.Value.ServerId);
            AssertWithinSecond(_now.ToUtcDateTime(), processingJob.Value.StartedAt);
            
            Assert.Equal(typeof(ITestServices), processingJob.Value.Job.Type);
            Assert.Equal("Empty", processingJob.Value.Job.Method.Name);
        }

        [Fact]
        public void ProcessingJobs_ReturnsJobs_InTheAscendingOrder()
        {
            // Arrange
            var jobId1 = SimpleProcessingJob();
            var jobId2 = SimpleProcessingJob();
            var jobId3 = SimpleProcessingJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.ProcessingJobs(0, 10).ToArray();

            // Assert
            Assert.Equal(jobId1, result[0].Key);
            Assert.Equal(jobId2, result[1].Key);
            Assert.Equal(jobId3, result[2].Key);
        }

        [Fact]
        public void ProcessingJobs_ReturnsJobs_WithinTheGivenRange()
        {
            // Arrange
            var jobId1 = SimpleProcessingJob();
            var jobId2 = SimpleProcessingJob();
            var jobId3 = SimpleProcessingJob();
            var jobId4 = SimpleProcessingJob();
            var jobId5 = SimpleProcessingJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.ProcessingJobs(1, 2).ToArray();

            // Assert
            Assert.Equal(2, result.Length);
            Assert.Equal(jobId2, result[0].Key);
            Assert.Equal(jobId3, result[1].Key);
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
        public void ScheduledJobs_ReturnsCorrectJobs_InTheScheduledState_WithNonCompositeIndex()
        {
            // Arrange
            var jobId = SimpleScheduledJob(
                TimeSpan.FromDays(1),
                job: Job.FromExpression<ITestServices>(x => x.Empty()));

            var monitoring = CreateMonitoringApi();
            
            // Act
            var result = monitoring.ScheduledJobs(0, 10);

            // Assert
            var scheduledJob = result.Single();

            Assert.Equal(jobId, scheduledJob.Key);
            Assert.True(scheduledJob.Value.InScheduledState);
            AssertWithinSecond(_now.Add(TimeSpan.FromDays(1)).ToUtcDateTime(), scheduledJob.Value.EnqueueAt);
            AssertWithinSecond(_now.ToUtcDateTime(), scheduledJob.Value.ScheduledAt);

            Assert.Equal(typeof(ITestServices), scheduledJob.Value.Job.Type);
            Assert.Equal("Empty", scheduledJob.Value.Job.Method.Name);
        }

        [Fact]
        public void ScheduledJobs_ReturnsCorrectJobs_InTheScheduledState_WithCompositeIndex()
        {
            // Arrange
            var jobId = SimpleScheduledJob(
                TimeSpan.FromHours(1),
                job: Job.FromExpression<ITestServices>(x => x.Empty(), "critical"));

            var monitoring = CreateMonitoringApi();
            
            // Act
            var result = monitoring.ScheduledJobs(0, 10);

            // Assert
            var scheduledJob = result.Single();
            Assert.Equal(jobId, scheduledJob.Key);
            Assert.Equal("critical", scheduledJob.Value.Job.Queue);
        }

        [Fact]
        public void ScheduledJobs_ReturnsJobs_InTheAscendingOrder()
        {
            // Arrange
            var jobId1 = SimpleScheduledJob();
            var jobId2 = SimpleScheduledJob();
            var jobId3 = SimpleScheduledJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.ScheduledJobs(0, 10).ToArray();

            // Assert
            Assert.Equal(jobId1, result[0].Key);
            Assert.Equal(jobId2, result[1].Key);
            Assert.Equal(jobId3, result[2].Key);
        }

        [Fact]
        public void ScheduledJobs_ReturnsJobs_WithinTheGivenRange()
        {
            // Arrange
            var jobId1 = SimpleScheduledJob();
            var jobId2 = SimpleScheduledJob();
            var jobId3 = SimpleScheduledJob();
            var jobId4 = SimpleScheduledJob();
            var jobId5 = SimpleScheduledJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.ScheduledJobs(1, 2).ToArray();

            // Assert
            Assert.Equal(2, result.Length);
            Assert.Equal(jobId2, result[0].Key);
            Assert.Equal(jobId3, result[1].Key);
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
        public void SucceededJobs_ReturnsCorrectJobs_InTheSucceededState()
        {
            // Arrange
            var jobId = SimpleSucceededJob(
                "hello", 123, 456,
                job: Job.FromExpression<ITestServices>(x => x.Empty()));

            var monitoring = CreateMonitoringApi();
            
            // Act
            var result = monitoring.SucceededJobs(0, 10);

            // Assert
            var succeededJob = result.Single();
            
            Assert.Equal(jobId, succeededJob.Key);
            Assert.True(succeededJob.Value.InSucceededState);
            Assert.Equal("\"hello\"", succeededJob.Value.Result);
            Assert.Equal(123 + 456, succeededJob.Value.TotalDuration);
            AssertWithinSecond(_now.ToUtcDateTime(), succeededJob.Value.SucceededAt);
            
            Assert.Equal(typeof(ITestServices), succeededJob.Value.Job.Type);
            Assert.Equal("Empty", succeededJob.Value.Job.Method.Name);
        }

        [Fact]
        public void SucceededJobs_ReturnsJobs_InTheDescendingOrder()
        {
            // Arrange
            var jobId1 = SimpleSucceededJob();
            var jobId2 = SimpleSucceededJob();
            var jobId3 = SimpleSucceededJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.SucceededJobs(0, 10).ToArray();

            // Assert
            Assert.Equal(jobId3, result[0].Key);
            Assert.Equal(jobId2, result[1].Key);
            Assert.Equal(jobId1, result[2].Key);
        }

        [Fact]
        public void SucceededJobs_ReturnsJobs_WithinTheGivenRange()
        {
            // Arrange
            var jobId1 = SimpleSucceededJob();
            var jobId2 = SimpleSucceededJob();
            var jobId3 = SimpleSucceededJob();
            var jobId4 = SimpleSucceededJob();
            var jobId5 = SimpleSucceededJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.SucceededJobs(1, 2).ToArray();

            // Assert
            Assert.Equal(2, result.Length);
            Assert.Equal(jobId4, result[0].Key);
            Assert.Equal(jobId3, result[1].Key);
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
        public void FailedJobs_ReturnsCorrectJobs_InTheFailedState()
        {
            // Arrange
            var jobId = SimpleJob(
                job: Job.FromExpression<ITestServices>(x => x.Empty()),
                state: new FailedState(new InvalidOperationException("Hello, world!")) { Reason = "Some reason" });

            var monitoring = CreateMonitoringApi();
            
            // Act
            var result = monitoring.FailedJobs(0, 10);

            // Assert
            var failedJob = result.Single();
            
            Assert.Equal(jobId, failedJob.Key);
            Assert.True(failedJob.Value.InFailedState);
            Assert.Equal("Some reason", failedJob.Value.Reason);
            Assert.Equal(typeof(InvalidOperationException).FullName, failedJob.Value.ExceptionType);
            Assert.Equal("Hello, world!", failedJob.Value.ExceptionMessage);
            AssertWithinSecond(_now.ToUtcDateTime(), failedJob.Value.FailedAt);
            
            Assert.Equal(typeof(ITestServices), failedJob.Value.Job.Type);
            Assert.Equal("Empty", failedJob.Value.Job.Method.Name);
        }

        [Fact]
        public void FailedJobs_ReturnsJobs_InTheDescendingOrder()
        {
            // Arrange
            var jobId1 = SimpleFailedJob(new InvalidOperationException());
            var jobId2 = SimpleFailedJob(new OperationCanceledException());
            var jobId3 = SimpleFailedJob(new NotSupportedException());

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.FailedJobs(0, 10).ToArray();

            // Assert
            Assert.Equal(jobId3, result[0].Key);
            Assert.Equal(jobId2, result[1].Key);
            Assert.Equal(jobId1, result[2].Key);
        }

        [Fact]
        public void FailedJobs_ReturnsJobs_WithinTheGivenRange()
        {
            // Arrange
            var jobId1 = SimpleFailedJob();
            var jobId2 = SimpleFailedJob();
            var jobId3 = SimpleFailedJob();
            var jobId4 = SimpleFailedJob();
            var jobId5 = SimpleFailedJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.FailedJobs(1, 2).ToArray();

            // Assert
            Assert.Equal(2, result.Length);
            Assert.Equal(jobId4, result[0].Key);
            Assert.Equal(jobId3, result[1].Key);
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
        public void DeletedJobs_ReturnsCorrectJobs_InTheDeletedState()
        {
            // Arrange
            var jobId = SimpleDeletedJob(
                exception: new InvalidOperationException("Hello, world!"), 
                job: Job.FromExpression<ITestServices>(x => x.Empty()));

            var monitoring = CreateMonitoringApi();
            
            // Act
            var result = monitoring.DeletedJobs(0, 10);

            // Assert
            var deletedJob = result.Single();
            
            Assert.Equal(jobId, deletedJob.Key);
            Assert.True(deletedJob.Value.InDeletedState);
            AssertWithinSecond(_now.ToUtcDateTime(), deletedJob.Value.DeletedAt);
            
            Assert.Equal(typeof(ITestServices), deletedJob.Value.Job.Type);
            Assert.Equal("Empty", deletedJob.Value.Job.Method.Name);
        }

        [Fact]
        public void DeletedJobs_ReturnsJobs_InTheDescendingOrder()
        {
            // Arrange
            var jobId1 = SimpleDeletedJob(new InvalidOperationException());
            var jobId2 = SimpleDeletedJob(new NotSupportedException());
            var jobId3 = SimpleDeletedJob(new ArgumentNullException());

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.DeletedJobs(0, 10).ToArray();

            // Assert
            Assert.Equal(jobId3, result[0].Key);
            Assert.Equal(jobId2, result[1].Key);
            Assert.Equal(jobId1, result[2].Key);
        }

        [Fact]
        public void DeletedJobs_ReturnsJobs_WithinTheGivenRange()
        {
            // Arrange
            var jobId1 = SimpleDeletedJob();
            var jobId2 = SimpleDeletedJob();
            var jobId3 = SimpleDeletedJob();
            var jobId4 = SimpleDeletedJob();
            var jobId5 = SimpleDeletedJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.DeletedJobs(1, 2).ToArray();

            // Assert
            Assert.Equal(2, result.Length);
            Assert.Equal(jobId4, result[0].Key);
            Assert.Equal(jobId3, result[1].Key);
        }

        [Fact]
        public void AwaitingJobs_ReturnsEmptyCollection_WhenThereAreNoAwaitingJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.AwaitingJobs(0, 10);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void AwaitingJobs_ReturnsCorrectJobs_InTheAwaitingState()
        {
            // Arrange
            var processingId = SimpleProcessingJob("server-1", "worker-1");
            var jobId = SimpleAwaitingJob(
                processingId,
                job: Job.FromExpression<ITestServices>(x => x.Empty()));

            var monitoring = CreateMonitoringApi();
            
            // Act
            var result = monitoring.AwaitingJobs(0, 10);

            // Assert
            var awaitingJob = result.Single();
            
            Assert.Equal(jobId, awaitingJob.Key);
            Assert.True(awaitingJob.Value.InAwaitingState);
            Assert.Equal("Processing", awaitingJob.Value.ParentStateName);
            AssertWithinSecond(_now.ToUtcDateTime(), awaitingJob.Value.AwaitingAt);
            
            Assert.Equal(typeof(ITestServices), awaitingJob.Value.Job.Type);
            Assert.Equal("Empty", awaitingJob.Value.Job.Method.Name);
        }

        [Fact]
        public void AwaitingJobs_ReturnsJobs_InTheAscendingOrder()
        {
            // Arrange
            var jobId1 = SimpleAwaitingJob("123");
            var jobId2 = SimpleAwaitingJob("456");
            var jobId3 = SimpleAwaitingJob("789");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.AwaitingJobs(0, 10).ToArray();

            // Assert
            Assert.Equal(jobId1, result[0].Key);
            Assert.Equal(jobId2, result[1].Key);
            Assert.Equal(jobId3, result[2].Key);
        }

        [Fact]
        public void AwaitingJobs_ReturnsJobs_WithinTheGivenRange()
        {
            // Arrange
            var jobId1 = SimpleAwaitingJob("1");
            var jobId2 = SimpleAwaitingJob("1");
            var jobId3 = SimpleAwaitingJob("2");
            var jobId4 = SimpleAwaitingJob("1");
            var jobId5 = SimpleAwaitingJob("1");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.AwaitingJobs(1, 2).ToArray();

            // Assert
            Assert.Equal(2, result.Length);
            Assert.Equal(jobId2, result[0].Key);
            Assert.Equal(jobId3, result[1].Key);
        }

        [Fact]
        public void ScheduledCount_ReturnsZero_WhenThereAreNoScheduledJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.ScheduledCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void ScheduledCount_ReturnsTheCorrectNumber_OfScheduledJobs()
        {
            // Arrange
            SimpleScheduledJob(TimeSpan.FromHours(1));
            SimpleScheduledJob(TimeSpan.FromHours(2));

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.ScheduledCount();

            // Assert
            Assert.Equal(2, result);
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
        public void EnqueuedCount_ReturnsTheCorrectNumber_OfEnqueuedJobs_OfTheGivenQueue()
        {
            // Arrange
            SimpleEnqueueJob("critical");
            SimpleEnqueueJob("default");
            SimpleEnqueueJob("critical");
            SimpleEnqueueJob("critical");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.EnqueuedCount("critical");

            // Assert
            Assert.Equal(3, result);
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
        public void FailedCount_ReturnsTheCorrectNumber_OfFailedJobs()
        {
            // Arrange
            SimpleFailedJob();
            SimpleFailedJob();
            SimpleFailedJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.FailedCount();

            // Assert
            Assert.Equal(3, result);
        }

        [Fact]
        public void ProcessingCount_ReturnsZero_WhenThereAreNoProcessingJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.ProcessingCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void ProcessingCount_ReturnsTheCorrectNumber_OfProcessingJobs()
        {
            // Arrange
            SimpleProcessingJob("1", "1");
            SimpleProcessingJob("2", "2");
            SimpleProcessingJob("3", "3");
            SimpleProcessingJob("1", "1");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.ProcessingCount();

            // Assert
            Assert.Equal(4, result);
        }

        [Fact]
        public void SucceededListCount_ReturnsZero_WhenThereAreNoSucceededJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.SucceededListCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void SucceededListCount_ReturnsTheCorrectNumber_OfSucceededJobs_CurrentlyInIndex()
        {
            // Arrange
            SimpleSucceededJob();
            SimpleSucceededJob();
            SimpleSucceededJob();
            SimpleSucceededJob();
            SimpleSucceededJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.SucceededListCount();

            // Assert
            Assert.Equal(5, result);
        }

        [Fact]
        public void DeletedListCount_ReturnsZero_WhenThereAreNoDeletedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.DeletedListCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void DeletedListCount_ReturnsTheCorrectNumber_OfDeletedJobs_CurrentlyInIndex()
        {
            // Arrange
            SimpleDeletedJob();
            SimpleDeletedJob();
            SimpleDeletedJob();

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.DeletedListCount();

            // Assert
            Assert.Equal(3, result);
        }

        [Fact]
        public void AwaitingCount_ReturnsZero_WhenThereAreNoAwaitingJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.AwaitingCount();

            Assert.Equal(0, result);
        }

        [Fact]
        public void AwaitingCount_ReturnsTheCorrectNumber_OfAwaitingJobs()
        {
            // Arrange
            SimpleAwaitingJob("1");
            SimpleAwaitingJob("2");
            SimpleAwaitingJob("3");
            SimpleAwaitingJob("1");

            var monitoring = CreateMonitoringApi();

            // Act
            var result = monitoring.AwaitingCount();

            // Assert
            Assert.Equal(4, result);
        }

        [Fact]
        public void SucceededByDatesCount_ReturnsEntriesForTheWholeWeek_EvenWhenThereAreNoSucceededJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.SucceededByDatesCount();
            var date = _now.ToUtcDateTime().Date;

            Assert.Equal(7, result.Count);
            Assert.Equal(0, result[date]);
            Assert.Equal(0, result[date.AddDays(-1)]);
            Assert.Equal(0, result[date.AddDays(-2)]);
            Assert.Equal(0, result[date.AddDays(-3)]);
            Assert.Equal(0, result[date.AddDays(-4)]);
            Assert.Equal(0, result[date.AddDays(-5)]);
            Assert.Equal(0, result[date.AddDays(-6)]);
        }

        [Fact]
        public void FailedByDatesCount_ReturnsEntriesForTheWholeWeek_EvenWhenThereAreNoFailedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.FailedByDatesCount();
            var date = _now.ToUtcDateTime().Date;

            Assert.Equal(7, result.Count);
            Assert.Equal(0, result[date]);
            Assert.Equal(0, result[date.AddDays(-1)]);
            Assert.Equal(0, result[date.AddDays(-2)]);
            Assert.Equal(0, result[date.AddDays(-3)]);
            Assert.Equal(0, result[date.AddDays(-4)]);
            Assert.Equal(0, result[date.AddDays(-5)]);
            Assert.Equal(0, result[date.AddDays(-6)]);
        }

        [Fact]
        public void DeletedByDatesCount_ReturnsEntriesForTheWholeWeek_EvenWhenThereAreNoDeletedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.DeletedByDatesCount();
            var date = _now.ToUtcDateTime().Date;

            Assert.Equal(7, result.Count);
            Assert.Equal(0, result[date]);
            Assert.Equal(0, result[date.AddDays(-1)]);
            Assert.Equal(0, result[date.AddDays(-2)]);
            Assert.Equal(0, result[date.AddDays(-3)]);
            Assert.Equal(0, result[date.AddDays(-4)]);
            Assert.Equal(0, result[date.AddDays(-5)]);
            Assert.Equal(0, result[date.AddDays(-6)]);
        }

        [Fact]
        public void HourlySucceededJobs_ReturnsEntriesForTheWholeDay_EvenWhenThereAreNoSucceededJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.HourlySucceededJobs();

            Assert.Equal(24, result.Count);
            Assert.Equal(0, result[result.Keys.ElementAt(0)]);
            Assert.Equal(0, result[result.Keys.ElementAt(3)]);
            Assert.Equal(0, result[result.Keys.ElementAt(6)]);
            Assert.Equal(0, result[result.Keys.ElementAt(9)]);
            Assert.Equal(0, result[result.Keys.ElementAt(12)]);
            Assert.Equal(0, result[result.Keys.ElementAt(15)]);
            Assert.Equal(0, result[result.Keys.ElementAt(18)]);
            Assert.Equal(0, result[result.Keys.ElementAt(21)]);
        }

        [Fact]
        public void HourlyFailedJobs_ReturnsEntriesForTheWholeDay_EvenWhenThereAreNoFailedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.HourlyFailedJobs();

            Assert.Equal(24, result.Count);
            Assert.Equal(0, result[result.Keys.ElementAt(0)]);
            Assert.Equal(0, result[result.Keys.ElementAt(3)]);
            Assert.Equal(0, result[result.Keys.ElementAt(6)]);
            Assert.Equal(0, result[result.Keys.ElementAt(9)]);
            Assert.Equal(0, result[result.Keys.ElementAt(12)]);
            Assert.Equal(0, result[result.Keys.ElementAt(15)]);
            Assert.Equal(0, result[result.Keys.ElementAt(18)]);
            Assert.Equal(0, result[result.Keys.ElementAt(21)]);
        }

        [Fact]
        public void HourlyDeletedJobs_ReturnsEntriesForTheWholeDay_EvenWhenThereAreNoDeletedJobs()
        {
            var monitoring = CreateMonitoringApi();

            var result = monitoring.HourlyDeletedJobs();

            Assert.Equal(24, result.Count);
            Assert.Equal(0, result[result.Keys.ElementAt(0)]);
            Assert.Equal(0, result[result.Keys.ElementAt(3)]);
            Assert.Equal(0, result[result.Keys.ElementAt(6)]);
            Assert.Equal(0, result[result.Keys.ElementAt(9)]);
            Assert.Equal(0, result[result.Keys.ElementAt(12)]);
            Assert.Equal(0, result[result.Keys.ElementAt(15)]);
            Assert.Equal(0, result[result.Keys.ElementAt(18)]);
            Assert.Equal(0, result[result.Keys.ElementAt(21)]);
        }

        private string SimpleProcessingJob(string serverId = null, string workerId = null, Job job = null)
        {
            var type = typeof(ProcessingState);
            var ctor = type.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic);
            var processingState = (ProcessingState)ctor.First().Invoke(new object[] { serverId ?? "server-1", workerId ?? "worker-1" });

            return SimpleJob(state: processingState, job: job);
        }

        private string SimpleScheduledJob(TimeSpan? delay = null, Job job = null)
        {
            var state = new ScheduledState(delay ?? TimeSpan.FromHours(1));
            return SimpleJob(job: job, state: state, transactionAction: (transaction, assignedJobId, assignedJob) =>
            {
                transaction.AddToSet(
                    "schedule",
                    assignedJob.Queue != null ? $"{assignedJob.Queue}:{assignedJobId}" : assignedJobId,
                    JobHelper.ToTimestamp(state.EnqueueAt));
            });
        }

        private string SimpleEnqueueJob(string queue, string jobId = null, IState state = null, Job job = null)
        {
            return SimpleJob(jobId, job, state, (transaction, assignedJobId, _) =>
            {
                transaction.AddToQueue(queue, assignedJobId);
            });
        }

        private string SimpleSucceededJob(object result = null, long latency = 0, long duration = 0, Job job = null)
        {
            return SimpleJob(
                job: job,
                state: new SucceededState(result, latency, duration));
        }

        private string SimpleFailedJob(Exception exception = null, Job job = null)
        {
            return SimpleJob(
                job: job,
                state: new FailedState(exception ?? new InvalidOperationException()));
        }

        private string SimpleDeletedJob(Exception exception = null, Job job = null)
        {
            return SimpleJob(
                job: job,
                state: new DeletedState(new ExceptionInfo(exception ?? new InvalidOperationException())));
        }

        private string SimpleAwaitingJob(string parentId, Job job = null)
        {
            return SimpleJob(
                job: job,
                state: new AwaitingState(parentId));
        }

        private string SimpleJob(string jobId = null, Job job = null, IState state = null, Action<IWriteOnlyTransaction, string, Job> transactionAction = null)
        {
            var createdId = UseConnection(connection =>
            {
                job = job ?? Job.FromExpression<ITestServices>(x => x.Empty()); 
                jobId = jobId ?? connection.CreateExpiredJob(
                    job,
                    new Dictionary<string, string>(),
                    _now.ToUtcDateTime(),
                    TimeSpan.Zero);

                using (var transaction = connection.CreateWriteTransaction())
                {
                    if (state != null) transaction.SetJobState(jobId, state);
                    transactionAction?.Invoke(transaction, jobId, job);
                    transaction.Commit();
                }

                return jobId;
            });

            _now = _now.Add(TimeSpan.FromTicks(1));

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

        private static void AssertWithinSecond(DateTime date1, DateTime? date2)
        {
            Assert.True((date1 - date2.Value).TotalSeconds <= 1.0D);
        }
    }
}
