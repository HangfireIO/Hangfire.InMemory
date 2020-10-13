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

        private InMemoryMonitoringApi CreateMonitoringApi()
        {
            return new InMemoryMonitoringApi(new InMemoryDispatcherBase(_state));
        }
    }
}
