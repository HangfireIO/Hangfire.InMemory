// This file is part of Hangfire.InMemory. Copyright © 2024 Hangfire OÜ.
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
using Hangfire.InMemory.Entities;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.InMemory.State
{
    internal static class MonitoringQueries
    {
        public sealed class StatisticsGetAll<TKey> : Command<TKey, StatisticsGetAll<TKey>.Data>
            where TKey : IComparable<TKey>
        {
            protected override Data Execute(MemoryState<TKey> state)
            {
                return new Data
                {
                    Enqueued = state.GetCountByStateName(EnqueuedState.StateName),
                    Scheduled = state.GetCountByStateName(ScheduledState.StateName),
                    Processing = state.GetCountByStateName(ProcessingState.StateName),
                    Failed = state.GetCountByStateName(FailedState.StateName),
                    Succeeded = state.Counters.TryGetValue("stats:succeeded", out var succeeded) ? succeeded.Value : 0,
                    Deleted = state.Counters.TryGetValue("stats:deleted", out var deleted) ? deleted.Value : 0,
                    Queues = state.Queues.Count,
                    Servers = state.Servers.Count,
                    Recurring = state.Sets.TryGetValue("recurring-jobs", out var recurring)
                        ? recurring.Count
                        : 0,
                    Retries = state.Sets.TryGetValue("retries", out var retries)
                        ? retries.Count
                        : 0,
                    Awaiting = (int)state.GetCountByStateName(AwaitingState.StateName)
                };
            }

            public sealed class Data
            {
                public long Enqueued { get; init; }
                public long Scheduled { get; init; }
                public long Processing { get; init; }
                public long Failed { get; init; }
                public long Succeeded { get; init; }
                public long Deleted { get; init; }
                public int Queues { get; init; }
                public int Servers { get; init; }
                public int Recurring { get; init; }
                public int Retries { get; init; }
                public int Awaiting { get; init; }
            }
        }

        public sealed class QueuesGetAll<TKey> : Command<TKey, IReadOnlyList<QueuesGetAll<TKey>.QueueRecord>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<QueueRecord> Execute(MemoryState<TKey> state)
            {
                var result = new List<QueueRecord>();

                foreach (var queueEntry in state.Queues)
                {
                    var queueResult = new List<JobRecord>();
                    var index = 0;
                    const int count = 5;

                    foreach (var message in queueEntry.Value.Queue)
                    {
                        if (index++ >= count) break;

                        state.Jobs.TryGetValue(message, out var jobEntry);

                        var stateName = jobEntry?.State?.Name;
                        var inEnqueuedState = EnqueuedState.StateName.Equals(
                            stateName,
                            StringComparison.OrdinalIgnoreCase);

                        queueResult.Add(new JobRecord(
                            message,
                            jobEntry?.InvocationData,
                            stateName,
                            inEnqueuedState && jobEntry?.State != null
                                ? jobEntry.State.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            inEnqueuedState,
                            inEnqueuedState ? jobEntry?.State?.CreatedAt : null,
                            state.StringComparer));
                    }

                    result.Add(new QueueRecord(
                        queueEntry.Value.Queue.Count,
                        queueEntry.Key,
                        queueResult.AsReadOnly()));
                }

                return result.OrderBy(x => x.Name, state.StringComparer).ToList().AsReadOnly();
            }

            public sealed class QueueRecord(long length, string name, IReadOnlyList<JobRecord> first)
            {
                public long Length { get; } = length;
                public string Name { get; } = name;
                public IReadOnlyList<JobRecord> First { get; } = first;
            }

            public sealed class JobRecord(
                TKey key,
                InvocationData? invocationData,
                string? state,
                IReadOnlyDictionary<string, string>? stateData,
                bool inEnqueuedState,
                MonotonicTime? enqueuedAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public InvocationData? InvocationData { get; } = invocationData;
                public string? State { get; } = state;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public bool InEnqueuedState { get; } = inEnqueuedState;
                public MonotonicTime? EnqueuedAt { get; } = enqueuedAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class QueueGetCount<TKey>(string queueName) : ValueCommand<TKey, long>
            where TKey : IComparable<TKey>
        {
            protected override long Execute(MemoryState<TKey> state)
            {
                return state.Queues.TryGetValue(queueName, out var queue)
                    ? queue.Queue.Count
                    : 0;
            }
        }

        public sealed class JobGetEnqueued<TKey>(string queueName, int from, int count) : Command<TKey, IReadOnlyList<JobGetEnqueued<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.Queues.TryGetValue(queueName, out var queue))
                {
                    var counter = 0;

                    foreach (var message in queue.Queue)
                    {
                        if (counter < from) { counter++; continue; }
                        if (counter >= from + count) break;

                        state.Jobs.TryGetValue(message, out var jobEntry);

                        var stateName = jobEntry?.State?.Name;
                        var inEnqueuedState = EnqueuedState.StateName.Equals(
                            stateName,
                            StringComparison.OrdinalIgnoreCase);

                        result.Add(new Record(
                            message,
                            jobEntry?.InvocationData,
                            inEnqueuedState,
                            stateName,
                            inEnqueuedState && jobEntry?.State != null
                                ? jobEntry.State.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            inEnqueuedState ? jobEntry?.State?.CreatedAt : null,
                            state.StringComparer));

                        counter++;
                    }
                }

                return result.AsReadOnly();
            }

            public sealed class Record(
                TKey key,
                InvocationData? invocationData,
                bool inEnqueuedState,
                string? stateName,
                IReadOnlyDictionary<string, string>? stateData,
                MonotonicTime? enqueuedAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public bool InEnqueuedState { get; } = inEnqueuedState;
                public string? StateName { get; } = stateName;
                public InvocationData? InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public MonotonicTime? EnqueuedAt { get; } = enqueuedAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobGetDetails<TKey>(TKey key) : Command<TKey, JobGetDetails<TKey>.Data?>
            where TKey : IComparable<TKey>
        {
            protected override Data? Execute(MemoryState<TKey> state)
            {
                if (!state.Jobs.TryGetValue(key, out var entry))
                {
                    return null;
                }

                return new Data(
                    entry.InvocationData,
                    entry.GetParameters(),
                    entry.History.ToArray(),
                    entry.CreatedAt,
                    entry.ExpireAt,
                    state.StringComparer);
            }

            public sealed class Data(
                InvocationData invocationData,
                KeyValuePair<string, string>[] parameters,
                StateEntry[] history,
                MonotonicTime createdAt,
                MonotonicTime? expireAt,
                StringComparer stringComparer)
            {
                public InvocationData InvocationData { get; } = invocationData;
                public KeyValuePair<string, string>[] Parameters { get; } = parameters;
                public StateEntry[] History { get; } = history;
                public MonotonicTime CreatedAt { get; } = createdAt;
                public MonotonicTime? ExpireAt { get; } = expireAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobsGetByState<TKey>(string stateName, int from, int count, bool reversed = false) : Command<TKey, IReadOnlyList<JobsGetByState<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.JobStateIndex.TryGetValue(stateName, out var indexEntry))
                {
                    var index = 0;
                    var collection = reversed ? indexEntry.Reverse() : indexEntry;

                    foreach (var entry in collection)
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        var inRequiredState = stateName.Equals(
                            entry.State?.Name,
                            StringComparison.OrdinalIgnoreCase);

                        result.Add(new Record(
                            entry.Key,
                            inRequiredState,
                            entry.InvocationData,
                            inRequiredState
                                ? entry.State?.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            entry.State?.Reason,
                            entry.State?.CreatedAt,
                            state.StringComparer));

                        index++;
                    }
                }

                return result.AsReadOnly();
            }

            public sealed class Record(
                TKey key,
                bool inRequiredState,
                InvocationData invocationData,
                IReadOnlyDictionary<string, string>? stateData,
                string? stateReason,
                MonotonicTime? stateCreatedAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public bool InRequiredState { get; } = inRequiredState;
                public InvocationData InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public string? StateReason { get; } = stateReason;
                public MonotonicTime? StateCreatedAt { get; } = stateCreatedAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobGetAwaiting<TKey>(int from, int count, IKeyProvider<TKey> keyProvider)
            : Command<TKey, IReadOnlyList<JobGetAwaiting<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.JobStateIndex.TryGetValue(AwaitingState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry)
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        var inAwaitingState = AwaitingState.StateName.Equals(
                            entry.State?.Name,
                            StringComparison.OrdinalIgnoreCase);

                        string? parentStateName = null;

                        if (inAwaitingState && entry.State?.Data != null &&
                            entry.State.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer).TryGetValue("ParentId", out var parentId) &&
                            keyProvider.TryParse(parentId, out var parentKey) &&
                            state.Jobs.TryGetValue(parentKey, out var parentEntry))
                        {
                            parentStateName = parentEntry.State?.Name;
                        }

                        result.Add(new Record(
                            entry.Key,
                            inAwaitingState,
                            entry.InvocationData,
                            inAwaitingState
                                ? entry.State?.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            entry.State?.CreatedAt,
                            state.StringComparer,
                            parentStateName));

                        index++;
                    }
                }

                return result.AsReadOnly();
            }

            public sealed class Record(
                TKey key,
                bool inAwaitingState,
                InvocationData invocationData,
                IReadOnlyDictionary<string, string>? stateData,
                MonotonicTime? awaitingAt,
                StringComparer stringComparer,
                string? parentState)
            {
                public TKey Key { get; } = key;
                public bool InAwaitingState { get; } = inAwaitingState;
                public InvocationData InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public MonotonicTime? AwaitingAt { get; } = awaitingAt;
                public StringComparer StringComparer { get; } = stringComparer;
                public string? ParentState { get; } = parentState;
            }
        }

        public sealed class JobGetCountByState<TKey>(string stateName) : ValueCommand<TKey, long>
            where TKey : IComparable<TKey>
        {
            protected override long Execute(MemoryState<TKey> state)
            {
                return state.GetCountByStateName(stateName);
            }
        }

        public sealed class ServersGetAll<TKey> : Command<TKey, IReadOnlyList<ServersGetAll<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>(state.Servers.Count);

                foreach (var entry in state.Servers)
                {
                    result.Add(new Record(
                        entry.Key,
                        entry.Value.Context.Queues.ToArray(),
                        entry.Value.Context.WorkerCount,
                        entry.Value.HeartbeatAt,
                        entry.Value.StartedAt));
                }
                
                return result.OrderBy(x => x.Name, state.StringComparer).ToList().AsReadOnly();
            }

            public sealed class Record(
                string name,
                string[] queues,
                int workersCount,
                MonotonicTime heartbeat,
                MonotonicTime startedAt)
            {
                public string Name { get; } = name;
                public string[] Queues { get; } = queues;
                public int WorkersCount { get; } = workersCount;
                public MonotonicTime Heartbeat { get; } = heartbeat;
                public MonotonicTime StartedAt { get; } = startedAt;
            }
        }

        public sealed class CounterGetDailyTimeline<TKey>(MonotonicTime now, string type) : Command<TKey, IDictionary<DateTime, long>>
            where TKey : IComparable<TKey>
        {
            protected override IDictionary<DateTime, long> Execute(MemoryState<TKey> state)
            {
                var endDate = now.ToUtcDateTime().Date;
                var startDate = endDate.AddDays(-7);
                var dates = new List<DateTime>();

                while (startDate < endDate)
                {
                    dates.Add(endDate);
                    endDate = endDate.AddDays(-1);
                }

                var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).ToList();
                var keys = stringDates.Select(x => $"stats:{type}:{x}").ToArray();
                var valuesMap = keys.Select(key => state.Counters.TryGetValue(key, out var entry) ? entry.Value : 0).ToArray();

                var result = new Dictionary<DateTime, long>();
                for (var i = 0; i < stringDates.Count; i++)
                {
                    result.Add(dates[i], valuesMap[i]);
                }

                return result;
            }
        }

        public sealed class CounterGetHourlyTimeline<TKey>(MonotonicTime now, string type) : Command<TKey, IDictionary<DateTime, long>>
            where TKey : IComparable<TKey>
        {
            protected override IDictionary<DateTime, long> Execute(MemoryState<TKey> state)
            {
                var endDate = now.ToUtcDateTime();
                var dates = new List<DateTime>();
                for (var i = 0; i < 24; i++)
                {
                    dates.Add(endDate);
                    endDate = endDate.AddHours(-1);
                }

                var keys = dates.Select(x => $"stats:{type}:{x:yyyy-MM-dd-HH}").ToArray();
                var valuesMap = keys.Select(key => state.Counters.TryGetValue(key, out var entry) ? entry.Value : 0).ToArray();

                var result = new Dictionary<DateTime, long>();
                for (var i = 0; i < dates.Count; i++)
                {
                    result.Add(dates[i], valuesMap[i]);
                }

                return result;
            }
        }
    }
}