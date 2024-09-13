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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using Hangfire.InMemory.Entities;
using Hangfire.Storage;

namespace Hangfire.InMemory.State
{
    internal static class MonitoringQueries<TKey> where TKey : IComparable<TKey>
    {
        public readonly struct StatisticsGetAll(
            IReadOnlyCollection<string> states, IReadOnlyDictionary<string, string> counters, IReadOnlyDictionary<string, string> sets)
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

                return new Data
                {
                    States = stateCounts,
                    Counters = counterCounts,
                    Sets = setCounts,
                    Queues = state.Queues.Count,
                    Servers = state.Servers.Count
                };
            }

            public sealed class Data
            {
                public required IReadOnlyDictionary<string, long> States { get; init; }
                public required IReadOnlyDictionary<string, long> Counters { get; init; }
                public required IReadOnlyDictionary<string, long> Sets { get; init; }
                public required int Queues { get; init; }
                public required int Servers { get; init; }
            }
        }

        public readonly struct QueuesGetAll
        {
            [SuppressMessage("Performance", "CA1822:Mark members as static")]
            public IReadOnlyList<QueueRecord> Execute(MemoryState<TKey> state)
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

                    result.Add(new QueueRecord(entry.Value.Queue.Count, entry.Key, queueResult.AsReadOnly()));
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

        public readonly struct QueueGetCount(string queueName)
        {
            public long Execute(MemoryState<TKey> state)
            {
                return state.Queues.TryGetValue(queueName, out var entry)
                    ? entry.Queue.Count
                    : 0;
            }
        }

        public readonly struct QueueGetEnqueued(string queueName, int from, int count)
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

        public readonly struct JobGetDetails(TKey key)
        {
            public Data? Execute(MemoryState<TKey> state)
            {
                if (!state.Jobs.TryGetValue(key, out var entry))
                {
                    return null;
                }

                return new Data
                {
                    InvocationData = entry.InvocationData,
                    Parameters = entry.GetParameters(),
                    History = entry.History.ToArray(),
                    CreatedAt = entry.CreatedAt,
                    ExpireAt = entry.ExpireAt,
                    StringComparer = state.StringComparer
                };
            }

            public sealed class Data
            {
                public required InvocationData InvocationData { get; init; }
                public required KeyValuePair<string, string>[] Parameters { get; init; }
                public required StateRecord[] History { get; init; }
                public required MonotonicTime CreatedAt { get; init; }
                public required MonotonicTime? ExpireAt { get; init; }
                public required StringComparer StringComparer { get; init; }
            }
        }

        public readonly struct JobsGetByKey(IEnumerable<TKey> keys)
        {
            public IReadOnlyDictionary<TKey, Record?> Execute(MemoryState<TKey> state)
            {
                var result = new Dictionary<TKey, Record?>();

                foreach (var key in keys)
                {
                    if (result.ContainsKey(key)) continue;

                    Record? record = null;

                    if (state.Jobs.TryGetValue(key, out var entry))
                    {
                        record = new Record
                        {
                            InvocationData = entry.InvocationData,
                            StateName = entry.State?.Name,
                            StateData = entry.State?.Data.ToDictionary(static x => x.Key, static x => x.Value,
                                state.StringComparer),
                            StateReason = entry.State?.Reason,
                            StateCreatedAt = entry.State?.CreatedAt,
                            StringComparer = state.StringComparer
                        };
                    }

                    result.Add(key, record);
                }

                return result;
            }

            public sealed class Record
            {
                public required InvocationData InvocationData { get; init; }
                public required string? StateName { get; init; }
                public required IReadOnlyDictionary<string, string>? StateData { get; init; }
                public required string? StateReason { get; init; }
                public required MonotonicTime? StateCreatedAt { get; init; }
                public required StringComparer StringComparer { get; init; }
            }
        }

        public readonly struct JobsGetByState(string stateName, int from, int count, bool reversed = false)
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

        public readonly struct JobGetCountByState(string stateName)
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

        public readonly struct ServersGetAll
        {
            [SuppressMessage("Performance", "CA1822:Mark members as static")]
            public IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>(state.Servers.Count);

                foreach (var entry in state.Servers)
                {
                    result.Add(new Record
                    {
                        Name = entry.Key,
                        Queues = entry.Value.Context.Queues.ToArray(),
                        WorkersCount = entry.Value.Context.WorkerCount,
                        Heartbeat = entry.Value.HeartbeatAt,
                        StartedAt = entry.Value.StartedAt
                    });
                }
                
                return result.OrderBy(static x => x.Name, state.StringComparer).ToList().AsReadOnly();
            }

            public sealed class Record
            {
                public required string Name { get; init; }
                public required string[] Queues { get; init; }
                public required int WorkersCount { get; init; }
                public required MonotonicTime Heartbeat { get; init; }
                public required MonotonicTime StartedAt { get; init; }
            }
        }

        public readonly struct CounterGetDailyTimeline(MonotonicTime now, string type)
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
                var statsType = type;
                var keys = stringDates.Select(x => $"stats:{statsType}:{x}").ToArray();
                var valuesMap = keys.Select(key => state.Counters.TryGetValue(key, out var entry) ? entry.Value : 0).ToArray();

                var result = new Dictionary<DateTime, long>();
                for (var i = 0; i < stringDates.Count; i++)
                {
                    result.Add(dates[i], valuesMap[i]);
                }

                return result;
            }
        }

        public readonly struct CounterGetHourlyTimeline(MonotonicTime now, string type)
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

                var statsType = type;

                var keys = dates.Select(x => $"stats:{statsType}:{x:yyyy-MM-dd-HH}").ToArray();
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