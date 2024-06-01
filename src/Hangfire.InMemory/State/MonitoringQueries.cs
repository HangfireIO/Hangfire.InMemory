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
using System.Linq;
using Hangfire.InMemory.Entities;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.InMemory.State
{
    internal static class MonitoringQueries
    {
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

        public sealed class JobGetProcessing<TKey>(int from, int count) : Command<TKey, IReadOnlyList<JobGetProcessing<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.JobStateIndex.TryGetValue(ProcessingState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry)
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        var inProcessingState = ProcessingState.StateName.Equals(
                            entry.State?.Name,
                            StringComparison.OrdinalIgnoreCase);

                        result.Add(new Record(
                            entry.Key,
                            inProcessingState,
                            entry.InvocationData,
                            inProcessingState
                                ? entry.State?.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            entry.State?.CreatedAt,
                            state.StringComparer));

                        index++;
                    }
                }

                return result.AsReadOnly();
            }

            public sealed class Record(
                TKey key,
                bool inProcessingState,
                InvocationData invocationData,
                IReadOnlyDictionary<string, string>? stateData,
                MonotonicTime? startedAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public bool InProcessingState { get; } = inProcessingState;
                public InvocationData InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public MonotonicTime? StartedAt { get; } = startedAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobGetScheduled<TKey>(int from, int count) : Command<TKey, IReadOnlyList<JobGetScheduled<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.JobStateIndex.TryGetValue(ScheduledState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry)
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        var inScheduledState = ScheduledState.StateName.Equals(
                            entry.State?.Name,
                            StringComparison.OrdinalIgnoreCase);

                        result.Add(new Record(
                            entry.Key,
                            inScheduledState,
                            entry.InvocationData,
                            inScheduledState
                                ? entry.State?.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            entry.State?.CreatedAt,
                            state.StringComparer));

                        index++;
                    }
                }

                return result.AsReadOnly();
            }

            public sealed class Record(
                TKey key,
                bool inScheduledState,
                InvocationData invocationData,
                IReadOnlyDictionary<string, string>? stateData,
                MonotonicTime? scheduledAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public bool InScheduledState { get; } = inScheduledState;
                public InvocationData InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public MonotonicTime? ScheduledAt { get; } = scheduledAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobGetSucceeded<TKey>(int from, int count) : Command<TKey, IReadOnlyList<JobGetSucceeded<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.JobStateIndex.TryGetValue(SucceededState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry.Reverse())
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        var inSucceededState = SucceededState.StateName.Equals(
                            entry.State?.Name,
                            StringComparison.OrdinalIgnoreCase);

                        result.Add(new Record(
                            entry.Key,
                            inSucceededState,
                            entry.InvocationData,
                            inSucceededState
                                ? entry.State?.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            entry.State?.CreatedAt,
                            state.StringComparer));

                        index++;
                    }
                }

                return result.AsReadOnly();
            }

            public sealed class Record(
                TKey key,
                bool inSucceededState,
                InvocationData invocationData,
                IReadOnlyDictionary<string, string>? stateData,
                MonotonicTime? succeededAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public bool InSucceededState { get; } = inSucceededState;
                public InvocationData InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public MonotonicTime? SucceededAt { get; } = succeededAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobGetFailed<TKey>(int from, int count) : Command<TKey, IReadOnlyList<JobGetFailed<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.JobStateIndex.TryGetValue(FailedState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry.Reverse())
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        var inFailedState = FailedState.StateName.Equals(
                            entry.State?.Name,
                            StringComparison.OrdinalIgnoreCase);

                        result.Add(new Record(
                            entry.Key,
                            inFailedState,
                            entry.InvocationData,
                            inFailedState
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
                bool inFailedState,
                InvocationData invocationData,
                IReadOnlyDictionary<string, string>? stateData,
                string? reason,
                MonotonicTime? failedAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public bool InFailedState { get; } = inFailedState;
                public InvocationData InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public string? Reason { get; } = reason;
                public MonotonicTime? FailedAt { get; } = failedAt;
                public StringComparer StringComparer { get; } = stringComparer;
            }
        }

        public sealed class JobGetDeleted<TKey>(int from, int count) : Command<TKey, IReadOnlyList<JobGetDeleted<TKey>.Record>>
            where TKey : IComparable<TKey>
        {
            protected override IReadOnlyList<Record> Execute(MemoryState<TKey> state)
            {
                var result = new List<Record>();

                if (state.JobStateIndex.TryGetValue(DeletedState.StateName, out var indexEntry))
                {
                    var index = 0;

                    foreach (var entry in indexEntry.Reverse())
                    {
                        if (index < from) { index++; continue; }
                        if (index >= from + count) break;

                        var inDeletedState = DeletedState.StateName.Equals(
                            entry.State?.Name,
                            StringComparison.OrdinalIgnoreCase);

                        result.Add(new Record(
                            entry.Key,
                            inDeletedState,
                            entry.InvocationData,
                            inDeletedState
                                ? entry.State?.Data.ToDictionary(x => x.Key, x => x.Value, state.StringComparer)
                                : null,
                            entry.State?.CreatedAt,
                            state.StringComparer));

                        index++;
                    }
                }

                return result.AsReadOnly();
            }

            public sealed class Record(
                TKey key,
                bool inDeletedState,
                InvocationData invocationData,
                IReadOnlyDictionary<string, string>? stateData,
                MonotonicTime? deletedAt,
                StringComparer stringComparer)
            {
                public TKey Key { get; } = key;
                public bool InDeletedState { get; } = inDeletedState;
                public InvocationData InvocationData { get; } = invocationData;
                public IReadOnlyDictionary<string, string>? StateData { get; } = stateData;
                public MonotonicTime? DeletedAt { get; } = deletedAt;
                public StringComparer StringComparer { get; } = stringComparer;
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
    }
}