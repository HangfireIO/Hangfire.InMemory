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
using System.Globalization;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.InMemory.State;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryMonitoringApi<TKey> : JobStorageMonitor
        where TKey : IComparable<TKey>
    {
        private readonly DispatcherBase<TKey> _dispatcher;
        private readonly IKeyProvider<TKey> _keyProvider;

        public InMemoryMonitoringApi([NotNull] DispatcherBase<TKey> dispatcher, [NotNull] IKeyProvider<TKey> keyProvider)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
            _keyProvider = keyProvider ?? throw new ArgumentNullException(nameof(keyProvider));
        }

        public override IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var queues = _dispatcher.QueryReadAndWait(new MonitoringQueries.QueuesGetAll<TKey>());
            return queues.Select(entry => new QueueWithTopEnqueuedJobsDto
            {
                Length = entry.Length,
                Name = entry.Name,
                FirstJobs = new JobList<EnqueuedJobDto>(entry.First.Select(x =>
                    new KeyValuePair<string, EnqueuedJobDto>(
                        _keyProvider.ToString(x.Key),
                        new EnqueuedJobDto
                        {
                            InvocationData = x.InvocationData,
                            Job = x.InvocationData.TryGetJob(out var loadException),
                            LoadException = loadException,
                            State = x.State,
                            StateData = x.StateData?.ToDictionary(d => d.Key, d => d.Value, x.StringComparer),
                            InEnqueuedState = x.InEnqueuedState,
                            EnqueuedAt = x.EnqueuedAt?.ToUtcDateTime()
                        })))
            }).ToList();
        }

        public override IList<ServerDto> Servers()
        {
            var servers = _dispatcher.QueryReadAndWait(new MonitoringQueries.ServersGetAll<TKey>());
            return servers.Select(entry => new ServerDto
            {
                Name = entry.Name,
                Queues = entry.Queues,
                WorkersCount = entry.WorkersCount,
                Heartbeat = entry.Heartbeat.ToUtcDateTime(),
                StartedAt = entry.StartedAt.ToUtcDateTime()
            }).ToList();
        }

        public override JobDetailsDto? JobDetails([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_keyProvider.TryParse(jobId, out var jobKey))
            {
                return null;
            }

            var details = _dispatcher.QueryReadAndWait(new MonitoringQueries.JobGetDetails<TKey>(jobKey));
            if (details == null) return null;

            return new JobDetailsDto
            {
                Job = details.InvocationData.TryGetJob(out var loadException),
                LoadException = loadException,
                InvocationData = details.InvocationData,
                Properties = details.Parameters.ToDictionary(x => x.Key, x => x.Value, details.StringComparer),
                History = details.History.Select(x => new StateHistoryDto
                {
                    CreatedAt = x.CreatedAt.ToUtcDateTime(),
                    StateName = x.Name,
                    Reason = x.Reason,
                    Data = x.Data.ToDictionary(d => d.Key, d => d.Value, details.StringComparer)
                }).Reverse().ToList(),
                CreatedAt = details.CreatedAt.ToUtcDateTime(),
                ExpireAt = details.ExpireAt?.ToUtcDateTime()
            };
        }

        public override StatisticsDto GetStatistics()
        {
            var statistics = _dispatcher.QueryReadAndWait(new MonitoringQueries.StatisticsGetAll<TKey>());
            return new StatisticsDto
            {
                Enqueued = statistics.Enqueued,
                Scheduled = statistics.Scheduled,
                Processing = statistics.Processing,
                Failed = statistics.Failed,
                Succeeded = statistics.Succeeded,
                Deleted = statistics.Deleted,
                Queues = statistics.Queues,
                Servers = statistics.Servers,
                Recurring = statistics.Recurring,
                Retries = statistics.Retries,
                Awaiting = statistics.Awaiting
            };
        }

        public override JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string queueName, int from, int perPage)
        {
            if (queueName == null) throw new ArgumentNullException(nameof(queueName));

            var enqueued =
                _dispatcher.QueryReadAndWait(new MonitoringQueries.JobGetEnqueued<TKey>(queueName, from, perPage));

            return new JobList<EnqueuedJobDto>(enqueued.Select(entry => new KeyValuePair<string, EnqueuedJobDto>(
                _keyProvider.ToString(entry.Key),
                new EnqueuedJobDto
                {
                    InvocationData = entry.InvocationData,
                    Job =  entry.InvocationData.TryGetJob(out var loadException),
                    LoadException = loadException,
                    InEnqueuedState = entry.InEnqueuedState,
                    State = entry.StateName,
                    StateData = entry.StateData?.ToDictionary(d => d.Key, d => d.Value, entry.StringComparer),
                    EnqueuedAt = entry.EnqueuedAt?.ToUtcDateTime()
                })));
        }

        public override JobList<FetchedJobDto> FetchedJobs([NotNull] string queueName, int from, int perPage)
        {
            if (queueName == null) throw new ArgumentNullException(nameof(queueName));
            return new JobList<FetchedJobDto>([]);
        }

        public override JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            var processing = _dispatcher.QueryReadAndWait(new MonitoringQueries.JobsGetByState<TKey>(
                ProcessingState.StateName,
                from,
                count));

            return new JobList<ProcessingJobDto>(processing.Select(entry => new KeyValuePair<string, ProcessingJobDto>(
                _keyProvider.ToString(entry.Key),
                new ProcessingJobDto
                {
                    InProcessingState = entry.InRequiredState,
                    InvocationData = entry.InvocationData,
                    Job = entry.InvocationData.TryGetJob(out var loadException),
                    LoadException = loadException,
                    StateData = entry.StateData?.ToDictionary(
                        x => x.Key,
                        x => x.Value,
                        entry.StringComparer),
                    StartedAt = entry.StateCreatedAt?.ToUtcDateTime(),
                    ServerId = entry.StateData?.TryGetValue("ServerId", out var serverId) ?? false
                        ? serverId
                        : null
                })));
        }

        public override JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            var scheduled = _dispatcher.QueryReadAndWait(new MonitoringQueries.JobsGetByState<TKey>(
                ScheduledState.StateName,
                from,
                count));

            return new JobList<ScheduledJobDto>(scheduled.Select(entry => new KeyValuePair<string, ScheduledJobDto>(
                _keyProvider.ToString(entry.Key),
                new ScheduledJobDto
                {
                    InScheduledState = entry.InRequiredState,
                    InvocationData = entry.InvocationData,
                    Job = entry.InvocationData.TryGetJob(out var loadException),
                    LoadException = loadException,
                    StateData = entry.StateData?.ToDictionary(
                        x => x.Key,
                        x => x.Value,
                        entry.StringComparer),
                    ScheduledAt = entry.StateCreatedAt?.ToUtcDateTime(),
                    EnqueueAt = (entry.StateData?.TryGetValue("EnqueueAt", out var enqueueAt) ?? false
                        ? JobHelper.DeserializeNullableDateTime(enqueueAt)
                        : null) ?? DateTime.MinValue
                })));
        }

        public override JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            var succeeded = _dispatcher.QueryReadAndWait(new MonitoringQueries.JobsGetByState<TKey>(
                SucceededState.StateName,
                from,
                count,
                reversed: true));

            return new JobList<SucceededJobDto>(succeeded.Select(entry => new KeyValuePair<string, SucceededJobDto>(
                _keyProvider.ToString(entry.Key),
                new SucceededJobDto
                {
                    InSucceededState = entry.InRequiredState,
                    InvocationData = entry.InvocationData,
                    Job = entry.InvocationData.TryGetJob(out var loadException),
                    LoadException = loadException,
                    StateData = entry.StateData?.ToDictionary(
                        x => x.Key,
                        x => x.Value,
                        entry.StringComparer),
                    SucceededAt = entry.StateCreatedAt?.ToUtcDateTime(),
                    Result = entry.StateData?.TryGetValue("Result", out var result) ?? false
                        ? result
                        : null,
                    TotalDuration = (entry.StateData?.TryGetValue("PerformanceDuration", out var duration) ?? false) && 
                                    (entry.StateData?.TryGetValue("Latency", out var latency) ?? false) 
                        ? long.Parse(duration, CultureInfo.InvariantCulture) + long.Parse(latency, CultureInfo.InvariantCulture)
                        : null
                })));
        }

        public override JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            var failed = _dispatcher.QueryReadAndWait(new MonitoringQueries.JobsGetByState<TKey>(
                FailedState.StateName,
                from,
                count,
                reversed: true));

            return new JobList<FailedJobDto>(failed.Select(entry => new KeyValuePair<string, FailedJobDto>(
                _keyProvider.ToString(entry.Key),
                new FailedJobDto
                {
                    InFailedState = entry.InRequiredState,
                    InvocationData = entry.InvocationData,
                    Job = entry.InvocationData.TryGetJob(out var loadException),
                    LoadException = loadException,
                    StateData = entry.StateData?.ToDictionary(
                        x => x.Key,
                        x => x.Value,
                        entry.StringComparer),
                    Reason = entry.StateReason,
                    FailedAt = entry.StateCreatedAt?.ToUtcDateTime(),
                    ExceptionDetails = entry.StateData?.TryGetValue("ExceptionDetails", out var details) ?? false 
                        ? details
                        : null,
                    ExceptionType = entry.StateData?.TryGetValue("ExceptionType", out var type) ?? false
                        ? type
                        : null,
                    ExceptionMessage = entry.StateData?.TryGetValue("ExceptionMessage", out var message) ?? false
                        ? message
                        : null,
                })));
        }

        public override JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            var deleted = _dispatcher.QueryReadAndWait(new MonitoringQueries.JobsGetByState<TKey>(
                DeletedState.StateName,
                from,
                count,
                reversed: true));

            return new JobList<DeletedJobDto>(deleted.Select(entry => new KeyValuePair<string, DeletedJobDto>(
                _keyProvider.ToString(entry.Key),
                new DeletedJobDto
                {
                    InDeletedState = entry.InRequiredState,
                    InvocationData = entry.InvocationData,
                    Job = entry.InvocationData.TryGetJob(out var loadException),
                    LoadException = loadException,
                    StateData = entry.StateData?.ToDictionary(
                        x => x.Key,
                        x => x.Value,
                        entry.StringComparer),
                    DeletedAt = entry.StateCreatedAt?.ToUtcDateTime()
                })));
        }

        public override JobList<AwaitingJobDto> AwaitingJobs(int from, int count)
        {
            var deleted = _dispatcher.QueryReadAndWait(new MonitoringQueries.JobGetAwaiting<TKey>(from, count, _keyProvider));

            return new JobList<AwaitingJobDto>(deleted.Select(entry => new KeyValuePair<string, AwaitingJobDto>(
                _keyProvider.ToString(entry.Key),
                new AwaitingJobDto
                {
                    InAwaitingState = entry.InAwaitingState,
                    InvocationData = entry.InvocationData,
                    Job = entry.InvocationData.TryGetJob(out var loadException),
                    LoadException = loadException,
                    StateData = entry.StateData?.ToDictionary(
                        x => x.Key,
                        x => x.Value,
                        entry.StringComparer),
                    AwaitingAt = entry.AwaitingAt?.ToUtcDateTime(),
                    ParentStateName = entry.ParentState
                })));
        }

        public override long ScheduledCount()
        {
            return GetCountByStateName(ScheduledState.StateName);
        }

        public override long EnqueuedCount([NotNull] string queueName)
        {
            if (queueName == null) throw new ArgumentNullException(nameof(queueName));
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.QueueGetCount<TKey>(queueName));
        }

        public override long FetchedCount([NotNull] string queue)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            return 0;
        }

        public override long FailedCount()
        {
            return GetCountByStateName(FailedState.StateName);
        }

        public override long ProcessingCount()
        {
            return GetCountByStateName(ProcessingState.StateName);
        }

        public override long SucceededListCount()
        {
            return GetCountByStateName(SucceededState.StateName);
        }

        public override long DeletedListCount()
        {
            return GetCountByStateName(DeletedState.StateName);
        }

        public override long AwaitingCount()
        {
            return GetCountByStateName(AwaitingState.StateName);
        }

        public override IDictionary<DateTime, long> SucceededByDatesCount()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.CounterGetDailyTimeline<TKey>(now, "succeeded"));
        }

        public override IDictionary<DateTime, long> FailedByDatesCount()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.CounterGetDailyTimeline<TKey>(now, "failed"));
        }

        public override IDictionary<DateTime, long> DeletedByDatesCount()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.CounterGetDailyTimeline<TKey>(now, "deleted"));
        }

        public override IDictionary<DateTime, long> HourlySucceededJobs()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.CounterGetHourlyTimeline<TKey>(now, "succeeded"));
        }

        public override IDictionary<DateTime, long> HourlyFailedJobs()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.CounterGetHourlyTimeline<TKey>(now, "failed"));
        }

        public override IDictionary<DateTime, long> HourlyDeletedJobs()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.CounterGetHourlyTimeline<TKey>(now, "deleted"));
        }

        private long GetCountByStateName(string stateName)
        {
            return _dispatcher.QueryReadAndWait(new MonitoringQueries.JobGetCountByState<TKey>(stateName));
        }
    }
}