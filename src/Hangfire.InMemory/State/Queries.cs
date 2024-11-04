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
using Hangfire.Storage;

namespace Hangfire.InMemory.State
{
    internal static class Queries<TKey> where TKey : IComparable<TKey>
    {
        public readonly struct JobGetData(TKey key)
        {
            public Data? Execute(IMemoryState<TKey> state)
            {
                if (!state.JobTryGet(key, out var entry))
                {
                    return null;
                }

                return new Data
                {
                    InvocationData = entry.InvocationData,
                    State = entry.State?.Name,
                    CreatedAt = entry.CreatedAt,
                    Parameters = entry.GetParameters(),
                    StringComparer = state.StringComparer 
                };
            }

            public readonly struct Data
            {
                public required InvocationData InvocationData { get; init; }
                public required string? State { get; init; }
                public required MonotonicTime CreatedAt { get; init; }
                public required KeyValuePair<string, string>[] Parameters { get; init; }
                public required StringComparer StringComparer { get; init; }
            }
        }

        public readonly struct JobGetState(TKey key)
        {
            public Data? Execute(IMemoryState<TKey> state)
            {
                if (!state.JobTryGet(key, out var entry) || entry.State == null)
                {
                    return null;
                }

                return new Data
                {
                    Name = entry.State.Name,
                    Reason = entry.State.Reason,
                    StateData = entry.State.Data,
                    StringComparer = state.StringComparer
                };
            }
            
            public readonly struct Data
            {
                public required string Name { get; init; }
                public required string? Reason { get; init; }
                public required KeyValuePair<string, string>[] StateData { get; init; }
                public required StringComparer StringComparer { get; init; }
            }
        }

        public readonly struct JobGetParameter(TKey key, string name)
        {
            public string? Execute(IMemoryState<TKey> state)
            {
                return state.JobTryGet(key, out var entry)
                    ? entry.GetParameter(name, state.StringComparer)
                    : null;
            }
        }

        public readonly struct SortedSetGetAll(string key)
        {
            public HashSet<string> Execute(IMemoryState<TKey> state)
            {
                var result = new HashSet<string>(state.StringComparer);

                if (state.SetTryGet(key, out var entry))
                {
                    foreach (var item in entry)
                    {
                        result.Add(item.Value);
                    }
                }

                return result;
            }
        }

        public readonly struct SortedSetFirstByLowestScore(string key, double fromScore, double toScore)
        {
            public string? Execute(IMemoryState<TKey> state)
            {
                if (state.SetTryGet(key, out var entry))
                {
                    return entry.GetFirstBetween(fromScore, toScore);
                }

                return null;
            }
        }

        public readonly struct SortedSetFirstByLowestScoreMultiple(string key, double fromScore, double toScore, int count)
        {
            public List<string> Execute(IMemoryState<TKey> state)
            {
                if (state.SetTryGet(key, out var entry))
                {
                    return entry.GetViewBetween(fromScore, toScore, count);
                }

                return new List<string>();
            }
        }

        public readonly struct SortedSetRange(string key, int startingFrom, int endingAt)
        {
            public List<string> Execute(IMemoryState<TKey> state)
            {
                var result = new List<string>();

                if (state.SetTryGet(key, out var entry))
                {
                    var counter = 0;

                    foreach (var item in entry)
                    {
                        if (counter < startingFrom) { counter++; continue; }
                        if (counter > endingAt) break;

                        result.Add(item.Value);

                        counter++;
                    }
                }

                return result;
            }
        }

        public readonly struct SortedSetContains(string key, string value)
        {
            public bool Execute(IMemoryState<TKey> state)
            {
                return state.SetTryGet(key, out var entry) && entry.Contains(value);
            }
        }

        public readonly struct SortedSetCount(string key)
        {
            public int Execute(IMemoryState<TKey> state)
            {
                return state.SetTryGet(key, out var entry) ? entry.Count : 0;
            }
        }

        public readonly struct SortedSetCountMultiple(IEnumerable<string> keys, int limit)
        {
            public int Execute(IMemoryState<TKey> state)
            {
                var count = 0;

                foreach (var key in keys)
                {
                    if (count >= limit) break;
                    count += state.SetTryGet(key, out var entry) ? entry.Count : 0;
                }

                return Math.Min(count, limit);
            }
        }

        public readonly struct SortedSetTimeToLive(string key)
        {
            public MonotonicTime? Execute(IMemoryState<TKey> state)
            {
                if (state.SetTryGet(key, out var entry) && entry.ExpireAt.HasValue)
                {
                    return entry.ExpireAt;
                }

                return null;
            }
        }

        public readonly struct HashGetAll(string key)
        {
            public Dictionary<string, string>? Execute(IMemoryState<TKey> state)
            {
                if (state.HashTryGet(key, out var entry))
                {
                    return entry.Value.ToDictionary(static x => x.Key, static x => x.Value, state.StringComparer);
                }

                return null;
            }
        }

        public readonly struct HashGet(string key, string name)
        {
            public string? Execute(IMemoryState<TKey> state)
            {
                if (state.HashTryGet(key, out var entry) && entry.Value.TryGetValue(name, out var result))
                {
                    return result;
                }

                return null;
            }
        }

        public readonly struct HashFieldCount(string key)
        {
            public int Execute(IMemoryState<TKey> state)
            {
                return state.HashTryGet(key, out var entry) ? entry.Value.Count : 0;
            }
        }

        public readonly struct HashTimeToLive(string key)
        {
            public MonotonicTime? Execute(IMemoryState<TKey> state)
            {
                if (state.HashTryGet(key, out var entry) && entry.ExpireAt.HasValue)
                {
                    return entry.ExpireAt;
                }

                return null;
            }
        }

        public readonly struct ListGetAll(string key)
        {
            public List<string> Execute(IMemoryState<TKey> state)
            {
                if (state.ListTryGet(key, out var entry))
                {
                    return new List<string>(entry);
                }

                return new List<string>();
            }
        }

        public readonly struct ListRange(string key, int startingFrom, int endingAt)
        {
            public List<string> Execute(IMemoryState<TKey> state)
            {
                var result = new List<string>();

                if (state.ListTryGet(key, out var entry))
                {
                    var count = endingAt - startingFrom + 1;
                    var skip = startingFrom;
                    foreach (var item in entry)
                    {
                        if (skip-- > 0) continue;
                        if (count-- == 0) break;

                        result.Add(item);
                    }
                }

                return result;
            }
        }

        public readonly struct ListCount(string key)
        {
            public int Execute(IMemoryState<TKey> state)
            {
                return state.ListTryGet(key, out var entry) ? entry.Count : 0;
            }
        }

        public readonly struct ListTimeToLive(string key)
        {
            public MonotonicTime? Execute(IMemoryState<TKey> state)
            {
                if (state.ListTryGet(key, out var entry) && entry.ExpireAt.HasValue)
                {
                    return entry.ExpireAt;
                }

                return null;
            }
        }

        public readonly struct CounterGet(string key)
        {
            public long Execute(IMemoryState<TKey> state)
            {
                return state.CounterTryGet(key, out var entry) ? entry.Value : 0;
            }
        }
    }
}