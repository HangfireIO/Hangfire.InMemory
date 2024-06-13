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
using Hangfire.Storage;

namespace Hangfire.InMemory.State
{
    internal static class MonitoringQueries
    {
        public sealed class StatisticsGetAll<TKey>(IReadOnlyCollection<string> states, IReadOnlyDictionary<string, string> counters, IReadOnlyDictionary<string, string> sets)
            where TKey : IComparable<TKey>
        {
            public Data Execute(MemoryState<TKey> state)
            {
                var stateCounts = new Dictionary<string, long>(states.Count);
                foreach (var stateName in states)
                {
                    stateCounts.Add(
                        stateName,
                        state.JobStateIndex.TryGetValue(stateName, out var indexEntry) ? indexEntry.Count : 0);
                }

                var counterCounts = new Dictionary<string, long>(counters.Count);
                foreach (var counter in counters)
                {
                    counterCounts.Add(
                        counter.Key,
                        state.Counters.TryGetValue(counter.Value, out var entry) ? entry.Value : 0);
                }

                var setCounts = new Dictionary<string, long>(sets.Count);
                foreach (var set in sets)
                {
                    setCounts.Add(
                        set.Key,
                        state.Sets.TryGetValue(set.Value, out var entry) ? entry.Count : 0);
                }

                return new Data(stateCounts, counterCounts, setCounts)
                {
                    Queues = state.Queues.Count,
                    Servers = state.Servers.Count
                };
            }

            public sealed class Data(
                IReadOnlyDictionary<string, long> states,
                IReadOnlyDictionary<string, long> counters,
                IReadOnlyDictionary<string, long> sets)
            {
                public IReadOnlyDictionary<string, long> States { get; } = states;
                public IReadOnlyDictionary<string, long> Counters { get; } = counters;
                public IReadOnlyDictionary<string, long> Sets { get; } = sets;
                public int Queues { get; init; }
                public int Servers { get; init; }
            }
        }

        public sealed class QueuesGetAll<TKey>
            where TKey : IComparable<TKey>
        {
            public static IReadOnlyList<QueueRecord> Execute(MemoryState<TKey> state)
            {
                var result = new List<QueueRecord>();

                foreach (var entry in state.Queues)
                {
                    var queueResult = new List<TKey>();
                    var index = 0;
                    const int count = 5;

                    foreach (var message in entry.Value.Queue)
                    {
                        if (index++ >= count) break;
                        queueResult.Add(message);
                    }

                    result.Add(new QueueRecord(
                        entry.Value.Queue.Count,
                        entry.Key,
                        queueResult.AsReadOnly()));
                }

                return result.OrderBy(static x => x.Name, state.StringComparer).ToList().AsReadOnly();
            }

            public sealed class QueueRecord(long length, string name, IReadOnlyList<TKey> first)
            {
                public long Length { get; } = length;
                public string Name { get; } = name;
                public IReadOnlyList<TKey> First { get; } = first;
            }
        }

        public sealed class QueueGetCount<TKey>(string queueName)
            where TKey : IComparable<TKey>
        {
            public long Execute(MemoryState<TKey> state)
            {
                return state.Queues.TryGetValue(queueName, out var entry)
                    ? entry.Queue.Count
                    : 0;
            }
        }

        public sealed class QueueGetEnqueued<TKey>(string queueName, int from, int count)
            where TKey : IComparable<TKey>
        {
            public IReadOnlyList<TKey> Execute(MemoryState<TKey> state)
            {
                var result = new List<TKey>();

                if (state.Queues.TryGetValue(queueName, out var entry))
                {
                    var index = 0;

                    foreach (var message in entry.Queue)
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        result.Add(message);
                        index++;
                    }
                }

                return result.AsReadOnly();
            }
        }

        public sealed class JobGetDetails<TKey>(TKey key)
            where TKey : IComparable<TKey>
        {
            public Data? Execute(MemoryState<TKey> state)
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
                StateRecord[] history,
                MonotonicTime createdAt,
                MonotonicTime? expireAt,
                StringComparer stringComparer)
            {
                public InvocationData InvocationData { get; } = invocationData;
                public KeyValuePair<string, string>[] Parameters { get; } = parameters;
                public StateRecord[] History { get; } = history;
                public MonotonicTime CreatedAt { get; } = createdAt;
                public MonotonicTime? ExpireAt { get; } = expireAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobsGetByKey<TKey>(IEnumerable<TKey> keys)
            where TKey : IComparable<TKey>
        {
            public IReadOnlyDictionary<TKey, Record?> Execute(MemoryState<TKey> state)
            {
                var result = new Dictionary<TKey, Record?>();

                foreach (var key in keys)
                {
                    Record? record = null;
                    
                    if (state.Jobs.TryGetValue(key, out var entry))
                    {
                        record = new Record(
                            entry.InvocationData,
                            entry.State?.Name,
                            entry.State?.Data.ToDictionary(static x => x.Key, static x => x.Value, state.StringComparer),
                            entry.State?.Reason,
                            entry.State?.CreatedAt,
                            state.StringComparer);
                    }

                    result.Add(key, record);
                }

                return result;
            }

            public sealed class Record(
                InvocationData invocationData,
                string? stateName,
                IReadOnlyDictionary<string, string>? stateData,
                string? stateReason,
                MonotonicTime? stateCreatedAt,
                StringComparer stringComparer)
            {
                public InvocationData InvocationData { get; } = invocationData;
                public string? StateName { get; } = stateName;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public string? StateReason { get; } = stateReason;
                public MonotonicTime? StateCreatedAt { get; } = stateCreatedAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobsGetByState<TKey>(string stateName, int from, int count, bool reversed = false)
            where TKey : IComparable<TKey>
        {
            public IReadOnlyList<TKey> Execute(MemoryState<TKey> state)
            {
                var result = new List<TKey>();

                if (state.JobStateIndex.TryGetValue(stateName, out var indexEntry))
                {
                    var index = 0;
                    var collection = reversed ? indexEntry.Reverse() : indexEntry;

                    foreach (var entry in collection)
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        result.Add(entry.Key);
                        index++;
                    }
                }

                return result.AsReadOnly();
            }
        }

        public sealed class JobGetCountByState<TKey>(string stateName)
            where TKey : IComparable<TKey>
        {
            public long Execute(MemoryState<TKey> state)
            {
                if (state.JobStateIndex.TryGetValue(stateName, out var indexEntry))
                {
                    return indexEntry.Count;
                }

                return 0;
            }
        }

        public sealed class ServersGetAll<TKey>
            where TKey : IComparable<TKey>
        {
            public static IReadOnlyList<Record> Execute(MemoryState<TKey> state)
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
                
                return result.OrderBy(static x => x.Name, state.StringComparer).ToList().AsReadOnly();
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

        public sealed class CounterGetDailyTimeline<TKey>(MonotonicTime now, string type)
            where TKey : IComparable<TKey>
        {
            public IDictionary<DateTime, long> Execute(MemoryState<TKey> state)
            {
                var endDate = now.ToUtcDateTime().Date;
                var startDate = endDate.AddDays(-7);
                var dates = new List<DateTime>();

                while (startDate < endDate)
                {
                    dates.Add(endDate);
                    endDate = endDate.AddDays(-1);
                }

                var stringDates = dates.Select(static x => x.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).ToList();
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

        public sealed class CounterGetHourlyTimeline<TKey>(MonotonicTime now, string type)
            where TKey : IComparable<TKey>
        {
            public IDictionary<DateTime, long> Execute(MemoryState<TKey> state)
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