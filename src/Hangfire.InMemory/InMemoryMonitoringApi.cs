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
using System.Diagnostics.CodeAnalysis;
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
        [SuppressMessage("ReSharper", "StaticMemberInGenericType")] 
        private static readonly string[] StatisticsStates =
        [
            EnqueuedState.StateName, ScheduledState.StateName, ProcessingState.StateName, FailedState.StateName,
            AwaitingState.StateName
        ];

        [SuppressMessage("ReSharper", "StaticMemberInGenericType")]
        private static readonly IReadOnlyDictionary<string, string> StatisticsCounters = new Dictionary<string, string>
        {
            { "Succeeded", "stats:succeeded" },
            { "Deleted", "stats:deleted" }
        };

        [SuppressMessage("ReSharper", "StaticMemberInGenericType")]
        private static readonly IReadOnlyDictionary<string, string> StatisticsSets = new Dictionary<string, string>
        {
            { "Recurring", "recurring-jobs" },
            { "Retries", "retries" }
        };

        private readonly DispatcherBase<TKey> _dispatcher;
        private readonly IKeyProvider<TKey> _keyProvider;

        public InMemoryMonitoringApi([NotNull] DispatcherBase<TKey> dispatcher, [NotNull] IKeyProvider<TKey> keyProvider)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
            _keyProvider = keyProvider ?? throw new ArgumentNullException(nameof(keyProvider));
        }

        public override IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var queues = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.QueuesGetAll(), static (q, s) => q.Execute(s));

            var jobKeys = new List<TKey>();
            foreach (var queue in queues)
            {
                jobKeys.AddRange(queue.First);
            }

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(jobKeys), static (q, s) => q.Execute(s));

            return queues.Select(entry => new QueueWithTopEnqueuedJobsDto
            {
                Length = entry.Length,
                Name = entry.Name,
                FirstJobs = new JobList<EnqueuedJobDto>(entry.First.Select(key =>
                    new KeyValuePair<string, EnqueuedJobDto>(
                        _keyProvider.ToString(key),
                        new EnqueuedJobDto
                        {
                            InEnqueuedState = TryGetJobRecord(jobs, key, EnqueuedState.StateName, out var job),
                            Job = job.Job,
                            State = job.StateName,
#if !HANGFIRE_170
                            InvocationData = job.InvocationData,
                            LoadException = job.LoadException,
                            StateData = job.StateData,
#endif
                            EnqueuedAt = job.StateCreatedAt
                        })))
            }).ToList();
        }

        public override IList<ServerDto> Servers()
        {
            var servers = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.ServersGetAll(), static (q, s) => q.Execute(s));
            return servers.Select(static entry => new ServerDto
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

            var details = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobGetDetails(jobKey), static (q, s) => q.Execute(s));
            if (details == null) return null;

            return new JobDetailsDto
            {
                Job = details.InvocationData.TryGetJob(out var loadException),
#if !HANGFIRE_170
                LoadException = loadException,
                InvocationData = details.InvocationData,
#endif
                Properties = details.Parameters.ToDictionary(static x => x.Key, static x => x.Value, details.StringComparer),
                History = details.History.Select(x => new StateHistoryDto
                {
                    CreatedAt = x.CreatedAt.ToUtcDateTime(),
                    StateName = x.Name,
                    Reason = x.Reason,
                    Data = x.Data.ToDictionary(static d => d.Key, static d => d.Value, details.StringComparer)
                }).Reverse().ToList(),
                CreatedAt = details.CreatedAt.ToUtcDateTime(),
                ExpireAt = details.ExpireAt?.ToUtcDateTime()
            };
        }

        public override StatisticsDto GetStatistics()
        {
            var statistics = _dispatcher.QueryReadAndWait(
                new MonitoringQueries<TKey>.StatisticsGetAll(StatisticsStates, StatisticsCounters, StatisticsSets),
                static (q, s) => q.Execute(s));

            return new StatisticsDto
            {
                Enqueued = statistics.States[EnqueuedState.StateName],
                Scheduled = statistics.States[ScheduledState.StateName],
                Processing = statistics.States[ProcessingState.StateName],
                Failed = statistics.States[FailedState.StateName],
#if !HANGFIRE_170
                Awaiting = statistics.States[AwaitingState.StateName],
#endif
                Succeeded = statistics.Counters["Succeeded"],
                Deleted = statistics.Counters["Deleted"],
                Queues = statistics.Queues,
                Servers = statistics.Servers,
#if !HANGFIRE_170
                Retries = statistics.Sets["Retries"],
#endif
                Recurring = statistics.Sets["Recurring"]
            };
        }

        public override JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string queue, int from, int perPage)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            var enqueued =
                _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.QueueGetEnqueued(queue, from, perPage), static (q, s) => q.Execute(s));

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(enqueued), static (q, s) => q.Execute(s));

            return new JobList<EnqueuedJobDto>(enqueued.Select(key => new KeyValuePair<string, EnqueuedJobDto>(
                _keyProvider.ToString(key),
                new EnqueuedJobDto
                {
                    InEnqueuedState = TryGetJobRecord(jobs, key, EnqueuedState.StateName, out var job),
                    Job =  job.Job,
                    State = job.StateName,
#if !HANGFIRE_170
                    InvocationData = job.InvocationData,
                    LoadException = job.LoadException,
                    StateData = job.StateData,
#endif
                    EnqueuedAt = job.StateCreatedAt
                })));
        }

        public override JobList<FetchedJobDto> FetchedJobs([NotNull] string queue, int from, int perPage)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            return new JobList<FetchedJobDto>([]);
        }

        public override JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            var processing = _dispatcher.QueryReadAndWait(
                new MonitoringQueries<TKey>.JobsGetByState(ProcessingState.StateName, from, count),
                static (q, s) => q.Execute(s));

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(processing), static (q, s) => q.Execute(s));

            return new JobList<ProcessingJobDto>(processing.Select(key => new KeyValuePair<string, ProcessingJobDto>(
                _keyProvider.ToString(key),
                new ProcessingJobDto
                {
                    InProcessingState = TryGetJobRecord(jobs, key, ProcessingState.StateName, out var job),
                    Job = job.Job,
#if !HANGFIRE_170
                    InvocationData = job.InvocationData,
                    LoadException = job.LoadException,
                    StateData = job.StateData,
#endif
                    StartedAt = job.StateCreatedAt,
                    ServerId = job.StateData?.TryGetValue("ServerId", out var serverId) ?? false
                        ? serverId
                        : null
                })));
        }

        public override JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            var scheduled = _dispatcher.QueryReadAndWait(
                new MonitoringQueries<TKey>.JobsGetByState(ScheduledState.StateName, from, count),
                static (q, s) => q.Execute(s));

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(scheduled), static (q, s) => q.Execute(s));

            return new JobList<ScheduledJobDto>(scheduled.Select(key => new KeyValuePair<string, ScheduledJobDto>(
                _keyProvider.ToString(key),
                new ScheduledJobDto
                {
                    InScheduledState = TryGetJobRecord(jobs, key, ScheduledState.StateName, out var job),
                    Job = job.Job,
#if !HANGFIRE_170
                    InvocationData = job.InvocationData,
                    LoadException = job.LoadException,
                    StateData = job.StateData,
#endif
                    ScheduledAt = job.StateCreatedAt,
                    EnqueueAt = (job.StateData?.TryGetValue("EnqueueAt", out var enqueueAt) ?? false
                        ? JobHelper.DeserializeNullableDateTime(enqueueAt)
                        : null) ?? DateTime.MinValue
                })));
        }

        public override JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            var succeeded = _dispatcher.QueryReadAndWait(
                new MonitoringQueries<TKey>.JobsGetByState(SucceededState.StateName, from, count, reversed: true),
                static (q, s) => q.Execute(s));

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(succeeded), static (q, s) => q.Execute(s));

            return new JobList<SucceededJobDto>(succeeded.Select(key => new KeyValuePair<string, SucceededJobDto>(
                _keyProvider.ToString(key),
                new SucceededJobDto
                {
                    InSucceededState = TryGetJobRecord(jobs, key, SucceededState.StateName, out var job),
                    Job = job.Job,
#if !HANGFIRE_170
                    InvocationData = job.InvocationData,
                    LoadException = job.LoadException,
                    StateData = job.StateData,
#endif
                    SucceededAt = job.StateCreatedAt,
                    Result = job.StateData?.TryGetValue("Result", out var result) ?? false
                        ? result
                        : null,
                    TotalDuration = (job.StateData?.TryGetValue("PerformanceDuration", out var duration) ?? false) && 
                                    (job.StateData?.TryGetValue("Latency", out var latency) ?? false) 
                        ? long.Parse(duration, CultureInfo.InvariantCulture) + long.Parse(latency, CultureInfo.InvariantCulture)
                        : null
                })));
        }

        public override JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            var failed = _dispatcher.QueryReadAndWait(
                new MonitoringQueries<TKey>.JobsGetByState(FailedState.StateName, from, count, reversed: true),
                static (q, s) => q.Execute(s));

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(failed), static (q, s) => q.Execute(s));

            return new JobList<FailedJobDto>(failed.Select(key => new KeyValuePair<string, FailedJobDto>(
                _keyProvider.ToString(key),
                new FailedJobDto
                {
                    InFailedState = TryGetJobRecord(jobs, key, FailedState.StateName, out var job),
                    Job = job.Job,
#if !HANGFIRE_170
                    InvocationData = job.InvocationData,
                    LoadException = job.LoadException,
                    StateData = job.StateData,
#endif
                    Reason = job.StateReason,
                    FailedAt = job.StateCreatedAt,
                    ExceptionDetails = job.StateData?.TryGetValue("ExceptionDetails", out var details) ?? false 
                        ? details
                        : null,
                    ExceptionType = job.StateData?.TryGetValue("ExceptionType", out var type) ?? false
                        ? type
                        : null,
                    ExceptionMessage = job.StateData?.TryGetValue("ExceptionMessage", out var message) ?? false
                        ? message
                        : null,
                })));
        }

        public override JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            var deleted = _dispatcher.QueryReadAndWait(
                new MonitoringQueries<TKey>.JobsGetByState(DeletedState.StateName, from, count, reversed: true),
                static (q, s) => q.Execute(s));

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(deleted), static (q, s) => q.Execute(s));

            return new JobList<DeletedJobDto>(deleted.Select(key => new KeyValuePair<string, DeletedJobDto>(
                _keyProvider.ToString(key),
                new DeletedJobDto
                {
                    InDeletedState = TryGetJobRecord(jobs, key, DeletedState.StateName, out var job),
                    Job = job.Job,
#if !HANGFIRE_170
                    InvocationData = job.InvocationData,
                    LoadException = job.LoadException,
                    StateData = job.StateData,
#endif
                    DeletedAt = job.StateCreatedAt
                })));
        }

#if !HANGFIRE_170
        public override JobList<AwaitingJobDto> AwaitingJobs(int from, int count)
        {
            var awaiting = _dispatcher.QueryReadAndWait(
                new MonitoringQueries<TKey>.JobsGetByState(AwaitingState.StateName, from, count),
                static (q, s) => q.Execute(s));

            var jobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(awaiting), static (q, s) => q.Execute(s));

            var parentKeys = new List<TKey>();
            foreach (var job in jobs)
            {
                if ((job.Value?.StateName?.Equals(AwaitingState.StateName, StringComparison.OrdinalIgnoreCase) ?? false) &&
                    (job.Value?.StateData?.TryGetValue("ParentId", out var parentId) ?? false) &&
                    _keyProvider.TryParse(parentId, out var key))
                {
                    parentKeys.Add(key);
                }
            }

            var parentJobs = _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobsGetByKey(parentKeys), static (q, s) => q.Execute(s));

            return new JobList<AwaitingJobDto>(awaiting.Select(key => new KeyValuePair<string, AwaitingJobDto>(
                _keyProvider.ToString(key),
                new AwaitingJobDto
                {
                    InAwaitingState = TryGetJobRecord(jobs, key, AwaitingState.StateName, out var job),
                    InvocationData = job.InvocationData,
                    Job = job.Job,
                    LoadException = job.LoadException,
                    StateData = job.StateData,
                    AwaitingAt = job.StateCreatedAt,
                    ParentStateName = (job.StateData?.TryGetValue("ParentId", out var parentId) ?? false) &&
                                      _keyProvider.TryParse(parentId, out var parentKey) &&
                                      parentJobs.TryGetValue(parentKey, out var parent)
                                      ? parent?.StateName
                                      : null
                })));
        }
#endif

        public override long ScheduledCount()
        {
            return GetCountByStateName(ScheduledState.StateName);
        }

        public override long EnqueuedCount([NotNull] string queue)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.QueueGetCount(queue), static (q, s) => q.Execute(s));
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

#if !HANGFIRE_170
        public override long AwaitingCount()
        {
            return GetCountByStateName(AwaitingState.StateName);
        }
#endif

        public override IDictionary<DateTime, long> SucceededByDatesCount()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.CounterGetDailyTimeline(now, "succeeded"), static (q, s) => q.Execute(s));
        }

        public override IDictionary<DateTime, long> FailedByDatesCount()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.CounterGetDailyTimeline(now, "failed"), static (q, s) => q.Execute(s));
        }

#if !HANGFIRE_170
        public override IDictionary<DateTime, long> DeletedByDatesCount()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.CounterGetDailyTimeline(now, "deleted"), static (q, s) => q.Execute(s));
        }
#endif

        public override IDictionary<DateTime, long> HourlySucceededJobs()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.CounterGetHourlyTimeline(now, "succeeded"), static (q, s) => q.Execute(s));
        }

        public override IDictionary<DateTime, long> HourlyFailedJobs()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.CounterGetHourlyTimeline(now, "failed"), static (q, s) => q.Execute(s));
        }

#if !HANGFIRE_170
        public override IDictionary<DateTime, long> HourlyDeletedJobs()
        {
            var now = _dispatcher.GetMonotonicTime();
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.CounterGetHourlyTimeline(now, "deleted"), static (q, s) => q.Execute(s));
        }
#endif

        private long GetCountByStateName(string stateName)
        {
            return _dispatcher.QueryReadAndWait(new MonitoringQueries<TKey>.JobGetCountByState(stateName), static (q, s) => q.Execute(s));
        }

        private static bool TryGetJobRecord(
            IReadOnlyDictionary<TKey, MonitoringQueries<TKey>.JobsGetByKey.Record?> jobs,
            TKey key,
            string targetState,
            out JobRecord jobRecord)
        {
            InvocationData? data = null;
            Job? job = null;
            JobLoadException? loadException = null;
            string? stateName = null;

            if (jobs.TryGetValue(key, out var record) && record != null)
            {
                data = record.InvocationData;
                job = record.InvocationData.TryGetJob(out loadException);
                stateName = record.StateName;

                if (targetState.Equals(record.StateName, StringComparison.OrdinalIgnoreCase))
                {
                    var stateData = record.StateData?.ToDictionary(static x => x.Key, static x => x.Value, record.StringComparer);
                    var stateCreatedAt = record.StateCreatedAt?.ToUtcDateTime();

                    jobRecord = new JobRecord(data, job, loadException, stateName, record.StateReason, stateData, stateCreatedAt);
                    return true;
                }
            }

            jobRecord = new JobRecord(data, job, loadException, stateName, null, null, null);
            return false;
        }

        private sealed class JobRecord(
            InvocationData? invocationData,
            Job? job,
            JobLoadException? loadException,
            string? stateName,
            string? stateReason,
            IDictionary<string, string>? stateData,
            DateTime? stateCreatedAt)
        {
            public InvocationData? InvocationData { get; } = invocationData;
            public Job? Job { get; } = job;
            public JobLoadException? LoadException { get; } = loadException;
            public string? StateName { get; } = stateName;
            public string? StateReason { get; } = stateReason;
            public IDictionary<string, string>? StateData { get; } = stateData;
            public DateTime? StateCreatedAt { get; } = stateCreatedAt;
        }
    }
}