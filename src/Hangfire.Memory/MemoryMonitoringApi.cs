using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Memory
{
    internal sealed class MemoryMonitoringApi : IMonitoringApi
    {
        private readonly IMemoryDispatcher _dispatcher;

        public MemoryMonitoringApi(IMemoryDispatcher dispatcher)
        {
            _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new List<QueueWithTopEnqueuedJobsDto>();

                foreach (var queueEntry in state._queues)
                {
                    var queueResult = new JobList<EnqueuedJobDto>(Enumerable.Empty<KeyValuePair<string, EnqueuedJobDto>>());
                    var counter = 0;
                    var from = 0;
                    var perPage = 5;

                    foreach (var message in queueEntry.Value)
                    {
                        if (counter < from) continue;
                        if (counter >= from + perPage) break;

                        Job job = null;

                        if (state.JobTryGet(message, out var jobEntry))
                        {
                            try
                            {
                                job = jobEntry.InvocationData.Deserialize();
                            }
                            catch (JobLoadException)
                            {
                            }
                        }

                        queueResult.Add(new KeyValuePair<string, EnqueuedJobDto>(message, new EnqueuedJobDto
                        {
                            Job = job,
                            State = jobEntry?.State?.Name,
                            InEnqueuedState = EnqueuedState.StateName.Equals(jobEntry?.State?.Name,
                                StringComparison.OrdinalIgnoreCase),
                            EnqueuedAt = jobEntry?.State?.CreatedAt
                        }));

                        counter++;
                    }

                    result.Add(new QueueWithTopEnqueuedJobsDto
                    {
                        Fetched = 0,
                        Length = queueEntry.Value.Count,
                        Name = queueEntry.Key,
                        FirstJobs = queueResult
                    });
                }

                return result;
            });
        }

        public IList<ServerDto> Servers()
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new List<ServerDto>(state._servers.Count);
                foreach (var entry in state._servers)
                {
                    result.Add(new ServerDto
                    {
                        Name = entry.Key,
                        Queues = entry.Value.Context.Queues.ToArray(),
                        WorkersCount = entry.Value.Context.WorkerCount,
                        Heartbeat = entry.Value.HeartbeatAt,
                        StartedAt = entry.Value.StartedAt,
                    });
                }

                return result;
            });
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                if (!state.JobTryGet(jobId, out var entry))
                {
                    return null;
                }

                Job job = null;

                try
                {
                    job = entry.InvocationData.Deserialize();
                }
                catch (JobLoadException)
                {
                }

                return new JobDetailsDto
                {
                    CreatedAt = entry.CreatedAt,
                    ExpireAt = entry.ExpireAt,
                    Job = job,
                    Properties = entry.Parameters.ToDictionary(x => x.Key, x => x.Value),
                    History = entry.History.Select(x => new StateHistoryDto
                    {
                        CreatedAt = x.CreatedAt,
                        StateName = x.Name,
                        Reason = x.Reason,
                        Data = x.Data.ToDictionary(y => y.Key, y => y.Value)
                    }).OrderByDescending(x => x.CreatedAt).ToList()
                };
            });
        }

        public StatisticsDto GetStatistics()
        {
            return _dispatcher.QueryAndWait(state => new StatisticsDto
            {
                Enqueued = GetCountByStateName(EnqueuedState.StateName, state),
                Scheduled = GetCountByStateName(ScheduledState.StateName, state),
                Processing = GetCountByStateName(ProcessingState.StateName, state),
                Failed = GetCountByStateName(FailedState.StateName, state),
                Succeeded = GetCountByStateName(SucceededState.StateName, state),
                Deleted = GetCountByStateName(DeletedState.StateName, state),
                Queues = state._queues.Count,
                Servers = state._servers.Count,
                Recurring = state.SetTryGet("recurring-jobs", out var recurring)
                    ? recurring.Count
                    : 0
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queueName, int @from, int perPage)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new JobList<EnqueuedJobDto>(Enumerable.Empty<KeyValuePair<string, EnqueuedJobDto>>());

                if (state._queues.TryGetValue(queueName, out var queue))
                {
                    var counter = 0;

                    foreach (var message in queue)
                    {
                        if (counter < from) continue;
                        if (counter >= from + perPage) break;

                        Job job = null;

                        if (state.JobTryGet(message, out var jobEntry))
                        {
                            try
                            {
                                job = jobEntry.InvocationData.Deserialize();
                            }
                            catch (JobLoadException)
                            {
                            }
                        }

                        result.Add(new KeyValuePair<string, EnqueuedJobDto>(message, new EnqueuedJobDto
                        {
                            Job = job,
                            State = jobEntry?.State?.Name,
                            InEnqueuedState = EnqueuedState.StateName.Equals(jobEntry?.State?.Name, StringComparison.OrdinalIgnoreCase),
                            EnqueuedAt = jobEntry?.State?.CreatedAt
                        }));

                        counter++;
                    }
                }

                return result;
            });
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
            return new JobList<FetchedJobDto>(Enumerable.Empty<KeyValuePair<string, FetchedJobDto>>());
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new JobList<ProcessingJobDto>(Enumerable.Empty<KeyValuePair<string, ProcessingJobDto>>());
                if (state._jobStateIndex.TryGetValue(ProcessingState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry)
                    {
                        if (index < from) continue;
                        if (index >= from + count) break;

                        Job job = null;

                        try
                        {
                            job = entry.InvocationData.Deserialize();
                        }
                        catch (JobLoadException)
                        {
                        }

                        result.Add(new KeyValuePair<string, ProcessingJobDto>(entry.Key, new ProcessingJobDto
                        {
                            ServerId = entry.State?.Data.ContainsKey("ServerId") ?? false ? entry.State.Data["ServerId"] : null,
                            Job = job,
                            InProcessingState = ProcessingState.StateName.Equals(entry.State?.Name, StringComparison.OrdinalIgnoreCase),
                            StartedAt = entry.State?.CreatedAt,
                            
                        }));

                        index++;
                    }
                }

                return result;
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new JobList<ScheduledJobDto>(Enumerable.Empty<KeyValuePair<string, ScheduledJobDto>>());
                if (state.SetTryGet("schedule", out var setEntry))
                {
                    var index = 0;

                    foreach (var entry in setEntry)
                    {
                        if (index < from) continue;
                        if (index >= from + count) break;

                        Job job = null;
                        if (state.JobTryGet(entry.Value, out var backgroundJob))
                        {
                            try
                            {
                                job = backgroundJob.InvocationData.Deserialize();
                            }
                            catch (JobLoadException)
                            {
                            }
                        }

                        result.Add(new KeyValuePair<string, ScheduledJobDto>(entry.Value, new ScheduledJobDto
                        {
                            EnqueueAt = JobHelper.FromTimestamp((long)entry.Score),
                            Job = job,
                            InScheduledState = ScheduledState.StateName.Equals(backgroundJob?.State?.Name, StringComparison.OrdinalIgnoreCase),
                            ScheduledAt = backgroundJob?.State?.CreatedAt,

                        }));

                        index++;
                    }
                }

                return result;
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new JobList<SucceededJobDto>(Enumerable.Empty<KeyValuePair<string, SucceededJobDto>>());
                if (state._jobStateIndex.TryGetValue(SucceededState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry)
                    {
                        if (index < from) continue;
                        if (index >= from + count) break;

                        Job job = null;

                        try
                        {
                            job = entry.InvocationData.Deserialize();
                        }
                        catch (JobLoadException)
                        {
                        }

                        result.Add(new KeyValuePair<string, SucceededJobDto>(entry.Key, new SucceededJobDto
                        {
                            Result = entry.State?.Data.ContainsKey("Result") ?? false ? entry.State.Data["Result"] : null,
                            TotalDuration = (entry.State?.Data.ContainsKey("PerformanceDuration") ?? false) && (entry.State?.Data.ContainsKey("Latency") ?? false) 
                                ? long.Parse(entry.State.Data["PerformanceDuration"]) + long.Parse(entry.State.Data["Latency"])
                                : (long?)null,
                            Job = job,
                            InSucceededState = SucceededState.StateName.Equals(entry.State?.Name, StringComparison.OrdinalIgnoreCase),
                            SucceededAt = entry.State?.CreatedAt,

                        }));

                        index++;
                    }
                }

                return result;
            });
        }

        public JobList<FailedJobDto> FailedJobs(int @from, int count)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new JobList<FailedJobDto>(Enumerable.Empty<KeyValuePair<string, FailedJobDto>>());
                if (state._jobStateIndex.TryGetValue(FailedState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry)
                    {
                        if (index < from) continue;
                        if (index >= from + count) break;

                        Job job = null;

                        try
                        {
                            job = entry.InvocationData.Deserialize();
                        }
                        catch (JobLoadException)
                        {
                        }

                        result.Add(new KeyValuePair<string, FailedJobDto>(entry.Key, new FailedJobDto
                        {
                            Job = job,
                            ExceptionDetails = entry.State?.Data.ContainsKey("ExceptionDetails") ?? false ? entry.State.Data["ExceptionDetails"] : null,
                            ExceptionType = entry.State?.Data.ContainsKey("ExceptionType") ?? false ? entry.State.Data["ExceptionType"] : null,
                            ExceptionMessage = entry.State?.Data.ContainsKey("ExceptionMessage") ?? false ? entry.State.Data["ExceptionMessage"] : null,
                            Reason = entry.State?.Reason,
                            InFailedState = FailedState.StateName.Equals(entry.State?.Name, StringComparison.OrdinalIgnoreCase),
                            FailedAt = entry.State?.CreatedAt,

                        }));

                        index++;
                    }
                }

                return result;
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return _dispatcher.QueryAndWait(state =>
            {
                var result = new JobList<DeletedJobDto>(Enumerable.Empty<KeyValuePair<string, DeletedJobDto>>());
                if (state._jobStateIndex.TryGetValue(DeletedState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry)
                    {
                        if (index < from) continue;
                        if (index >= from + count) break;

                        Job job = null;

                        try
                        {
                            job = entry.InvocationData.Deserialize();
                        }
                        catch (JobLoadException)
                        {
                        }

                        result.Add(new KeyValuePair<string, DeletedJobDto>(entry.Key, new DeletedJobDto
                        {
                            Job = job,
                            InDeletedState = DeletedState.StateName.Equals(entry.State?.Name, StringComparison.OrdinalIgnoreCase),
                            DeletedAt = entry.State?.CreatedAt,

                        }));

                        index++;
                    }
                }

                return result;
            });
        }

        public long ScheduledCount()
        {
            return GetCountByStateName(ScheduledState.StateName);
        }

        public long EnqueuedCount(string queueName)
        {
            return _dispatcher.QueryAndWait(state => state._queues.TryGetValue(queueName, out var queue) 
                ? queue.Count
                : 0);
        }

        public long FetchedCount(string queue)
        {
            return 0;
        }

        public long FailedCount()
        {
            return GetCountByStateName(FailedState.StateName);
        }

        public long ProcessingCount()
        {
            return GetCountByStateName(ProcessingState.StateName);
        }

        public long SucceededListCount()
        {
            return GetCountByStateName(SucceededState.StateName);
        }

        public long DeletedListCount()
        {
            return GetCountByStateName(DeletedState.StateName);
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return _dispatcher.QueryAndWait(state => GetTimelineStats(state, "succeeded"));
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return _dispatcher.QueryAndWait(state => GetTimelineStats(state, "failed"));
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return _dispatcher.QueryAndWait(state => GetHourlyTimelineStats(state, "succeeded"));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return _dispatcher.QueryAndWait(state => GetHourlyTimelineStats(state, "failed"));
        }

        private long GetCountByStateName(string stateName)
        {
            return _dispatcher.QueryAndWait(state => GetCountByStateName(stateName, state));
        }

        private static int GetCountByStateName(string stateName, MemoryState state)
        {
            if (state._jobStateIndex.TryGetValue(stateName, out var index))
            {
                return index.Count;
            }

            return 0;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(MemoryState state, string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => $"stats:{type}:{x:yyyy-MM-dd-HH}").ToArray();
            var valuesMap = keys.Select(key => state.CounterTryGet(key, out var entry) ? entry.Value : 0).ToArray();

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                result.Add(dates[i], valuesMap[i]);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetTimelineStats(MemoryState state, string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate < endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => $"stats:{type}:{x}").ToArray();
            var valuesMap = keys.Select(key => state.CounterTryGet(key, out var entry) ? entry.Value : 0).ToArray();

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < stringDates.Count; i++)
            {
                result.Add(dates[i], valuesMap[i]);
            }

            return result;
        }
    }
}