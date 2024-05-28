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

namespace Hangfire.InMemory
{
    internal static class InMemoryQueries
    {
        public sealed class JobGetData<TKey>(TKey key) : InMemoryCommand<TKey, JobGetData<TKey>.Data>
            where TKey : IComparable<TKey>
        {
            protected override Data Execute(InMemoryState<TKey> state)
            {
                if (!state.Jobs.TryGetValue(key, out var entry))
                {
                    return null;
                }

                return new Data
                {
                    InvocationData = entry.InvocationData,
                    State = entry.State?.Name,
                    CreatedAt = entry.CreatedAt,
                    Parameters = entry.GetParameters(),
                    StringComparer = state.Options.StringComparer
                };
            }

            public sealed class Data
            {
                public InvocationData InvocationData { get; set; }
                public string State { get; set; }
                public MonotonicTime CreatedAt { get; set; }
                public KeyValuePair<string, string>[] Parameters { get; set; }
                public StringComparer StringComparer { get; set; }
            }
        }

        public sealed class JobGetState<TKey>(TKey key) : InMemoryCommand<TKey, JobGetState<TKey>.Data>
            where TKey : IComparable<TKey>
        {
            protected override Data Execute(InMemoryState<TKey> state)
            {
                if (!state.Jobs.TryGetValue(key, out var jobEntry) || jobEntry.State == null)
                {
                    return null;
                }

                return new Data
                {
                    Name = jobEntry.State.Name,
                    Reason = jobEntry.State.Reason,
                    StateData = jobEntry.State.Data,
                    StringComparer = state.Options.StringComparer
                };
            }
            
            public sealed class Data
            {
                public string Name { get; set; }
                public string Reason { get; set; }
                public KeyValuePair<string, string>[] StateData { get; set; }
                public StringComparer StringComparer { get; set; }
            }
        }

        public sealed class JobGetParameter<TKey>(TKey key, string name) : InMemoryCommand<TKey, string>
            where TKey : IComparable<TKey>
        {
            protected override string Execute(InMemoryState<TKey> state)
            {
                return state.Jobs.TryGetValue(key, out var entry)
                    ? entry.GetParameter(name, state.Options.StringComparer)
                    : null;
            }
        }

        public sealed class SortedSetGetAll<TKey>(string key) : InMemoryCommand<TKey, HashSet<string>>
            where TKey : IComparable<TKey>
        {
            protected override HashSet<string> Execute(InMemoryState<TKey> state)
            {
                var result = new HashSet<string>(state.Options.StringComparer);

                if (state.Sets.TryGetValue(key, out var set))
                {
                    foreach (var entry in set)
                    {
                        result.Add(entry.Value);
                    }
                }

                return result;
            }
        }

        public sealed class SortedSetFirstByLowestScore<TKey>(string key, double fromScore, double toScore) 
            : InMemoryCommand<TKey, string>
            where TKey : IComparable<TKey>
        {
            protected override string Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    return set.GetFirstBetween(fromScore, toScore);
                }

                return null;
            }
        }

        public sealed class SortedSetFirstByLowestScoreMultiple<TKey>(string key, double fromScore, double toScore, int count) 
            : InMemoryCommand<TKey, List<string>>
            where TKey : IComparable<TKey>
        {
            protected override List<string> Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set))
                {
                    return set.GetViewBetween(fromScore, toScore, count);
                }

                return new List<string>();
            }
        }

        public sealed class SortedSetRange<TKey>(string key, int startingFrom, int endingAt) : IInMemoryCommand<TKey, List<string>>
            where TKey : IComparable<TKey>
        {
            public List<string> Execute(InMemoryState<TKey> state)
            {
                var result = new List<string>();

                if (state.Sets.TryGetValue(key, out var set))
                {
                    var counter = 0;

                    foreach (var entry in set)
                    {
                        if (counter < startingFrom) { counter++; continue; }
                        if (counter > endingAt) break;

                        result.Add(entry.Value);

                        counter++;
                    }
                }

                return result;
            }
        }

        public sealed class SortedSetContains<TKey>(string key, string value) : InMemoryValueCommand<TKey, bool>
            where TKey : IComparable<TKey>
        {
            protected override bool Execute(InMemoryState<TKey> state)
            {
                return state.Sets.TryGetValue(key, out var set) && set.Contains(value);
            }
        }

        public sealed class SortedSetCount<TKey>(string key) : InMemoryValueCommand<TKey, int>
            where TKey : IComparable<TKey>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                return state.Sets.TryGetValue(key, out var set) ? set.Count : 0;
            }
        }

        public sealed class SortedSetCountMultiple<TKey>(IEnumerable<string> keys, int limit) : InMemoryValueCommand<TKey, int>
            where TKey : IComparable<TKey>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                var count = 0;

                foreach (var key in keys)
                {
                    if (count >= limit) break;
                    count += state.Sets.TryGetValue(key, out var set) ? set.Count : 0;
                }

                return Math.Min(count, limit);
            }
        }

        public sealed class SortedSetTimeToLive<TKey>(string key) : InMemoryValueCommand<TKey, MonotonicTime?>
            where TKey : IComparable<TKey>
        {
            protected override MonotonicTime? Execute(InMemoryState<TKey> state)
            {
                if (state.Sets.TryGetValue(key, out var set) && set.ExpireAt.HasValue)
                {
                    return set.ExpireAt;
                }

                return null;
            }
        }

        public sealed class HashGetAll<TKey>(string key) : IInMemoryCommand<TKey, Dictionary<string, string>>
            where TKey : IComparable<TKey>
        {
            public Dictionary<string, string> Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash))
                {
                    return hash.Value.ToDictionary(x => x.Key, x => x.Value, state.Options.StringComparer);
                }

                return null;
            }
        }

        public sealed class HashGet<TKey>(string key, string name) : IInMemoryCommand<TKey, string>
            where TKey : IComparable<TKey>
        {
            public string Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.Value.TryGetValue(name, out var result))
                {
                    return result;
                }

                return null;
            }
        }

        public sealed class HashFieldCount<TKey>(string key) : InMemoryValueCommand<TKey, int>
            where TKey : IComparable<TKey>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                return state.Hashes.TryGetValue(key, out var hash) ? hash.Value.Count : 0;
            }
        }

        public sealed class HashTimeToLive<TKey>(string key) : InMemoryValueCommand<TKey, MonotonicTime?>
            where TKey : IComparable<TKey>
        {
            protected override MonotonicTime? Execute(InMemoryState<TKey> state)
            {
                if (state.Hashes.TryGetValue(key, out var hash) && hash.ExpireAt.HasValue)
                {
                    return hash.ExpireAt;
                }

                return null;
            }
        }

        public sealed class ListGetAll<TKey>(string key) : IInMemoryCommand<TKey, List<string>>
            where TKey : IComparable<TKey>
        {
            public List<string> Execute(InMemoryState<TKey> state)
            {
                if (state.Lists.TryGetValue(key, out var list))
                {
                    return new List<string>(list);
                }

                return new List<string>();
            }
        }

        public sealed class ListRange<TKey>(string key, int startingFrom, int endingAt) : IInMemoryCommand<TKey, List<string>>
            where TKey : IComparable<TKey>
        {
            public List<string> Execute(InMemoryState<TKey> state)
            {
                var result = new List<string>();

                if (state.Lists.TryGetValue(key, out var list))
                {
                    var count = endingAt - startingFrom + 1;
                    foreach (var item in list)
                    {
                        if (startingFrom-- > 0) continue;
                        if (count-- == 0) break;

                        result.Add(item);
                    }
                }

                return result;
            }
        }

        public sealed class ListCount<TKey>(string key) : InMemoryValueCommand<TKey, int>
            where TKey : IComparable<TKey>
        {
            protected override int Execute(InMemoryState<TKey> state)
            {
                return state.Lists.TryGetValue(key, out var list) ? list.Count : 0;
            }
        }

        public sealed class ListTimeToLive<TKey>(string key) : InMemoryValueCommand<TKey, MonotonicTime?>
            where TKey : IComparable<TKey>
        {
            protected override MonotonicTime? Execute(InMemoryState<TKey> state)
            {
                if (state.Lists.TryGetValue(key, out var list) && list.ExpireAt.HasValue)
                {
                    return list.ExpireAt;
                }

                return null;
            }
        }

        public sealed class CounterGet<TKey>(string key) : InMemoryValueCommand<TKey, long>
            where TKey : IComparable<TKey>
        {
            protected override long Execute(InMemoryState<TKey> state)
            {
                return state.Counters.TryGetValue(key, out var counter) ? counter.Value : 0;
            }
        }
    }
}