using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal interface IExpirableEntry
    {
        string Key { get; }
        DateTime? ExpireAt { get; set; }
    }

    internal sealed class ListEntry : IExpirableEntry
    {
        private readonly StringComparer _stringComparer;
        private List<string> _value = new List<string>();

        public ListEntry(string id, StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
            Key = id;
        }

        public string Key { get; }
        public DateTime? ExpireAt { get; set; }

        public int Count => _value.Count;

        public string this[int index] => _value[Count - index - 1];

        public void Add(string value)
        {
            _value.Add(value);
        }

        public void RemoveAll(string value)
        {
            _value.RemoveAll(other => _stringComparer.Equals(value, other));
        }

        // TODO: Try everything to remove this strange operation
        internal void Update(List<string> value)
        {
            _value = value;
        }
    }

    internal sealed class HashEntry : IExpirableEntry
    {
        public HashEntry(string id, StringComparer comparer)
        {
            Key = id;
            Value = new Dictionary<string, string>(comparer);
        }

        public string Key { get; }
        public IDictionary<string, string> Value { get; }
        public DateTime? ExpireAt { get; set; }
    }

    internal sealed class SetEntry : IExpirableEntry, IEnumerable<SortedSetEntry>
    {
        private readonly IDictionary<string, SortedSetEntry> _hash;
        private readonly SortedSet<SortedSetEntry> _value;

        public SetEntry(string id, StringComparer stringComparer)
        {
            _hash = new Dictionary<string, SortedSetEntry>(stringComparer);
            _value = new SortedSet<SortedSetEntry>(new SortedSetEntryComparer(stringComparer));
            Key = id;
        }

        public string Key { get; }
        public DateTime? ExpireAt { get; set; }

        public int Count => _value.Count;

        public void Add(string value, double score)
        {
            if (!_hash.TryGetValue(value, out var entry))
            {
                entry = new SortedSetEntry(value) { Score = score };
                _value.Add(entry);
                _hash.Add(value, entry);
            }
            else
            {
                // Element already exists, just need to add a score value – re-create it.
                _value.Remove(entry);
                entry.Score = score;
                _value.Add(entry);
            }
        }

        public List<string> GetViewBetween(double from, double to, int count)
        {
            var view = _value.GetViewBetween(
                new SortedSetEntry(null) { Score = from },
                new SortedSetEntry(null) { Score = to });

            var result = new List<string>(view.Count);
            foreach (var entry in view)
            {
                if (count-- == 0) break;
                result.Add(entry.Value);
            }

            return result;
        }
        
        public string GetFirstBetween(double from, double to)
        {
            var view = _value.GetViewBetween(
                new SortedSetEntry(null) { Score = from },
                new SortedSetEntry(null) { Score = to });

            return view.Count > 0 ? view.Min.Value : null;
        }

        public void Remove(string value)
        {
            if (_hash.TryGetValue(value, out var entry))
            {
                _value.Remove(entry);
                _hash.Remove(value);
            }
        }

        public bool Contains(string value)
        {
            return _hash.ContainsKey(value);
        }

        public IEnumerator<SortedSetEntry> GetEnumerator()
        {
            return _value.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    internal sealed class CounterEntry : IExpirableEntry
    {
        public CounterEntry(string id)
        {
            Key = id;
        }

        public string Key { get; }
        public long Value { get; set; }
        public DateTime? ExpireAt { get; set; }
    }

    internal sealed class BackgroundJobEntry : IExpirableEntry
    {
        private const int StateCountForRegularJob = 4; // (Scheduled) -> Enqueued -> Processing -> Succeeded

        internal BackgroundJobEntry()
        {
        }

        public string Key { get; private set; }
        public InvocationData InvocationData { get; internal set; }
        public Job Job { get; private set; }

        public ConcurrentDictionary<string, string> Parameters { get; private set; }

        public StateEntry State { get; set; }
        public ICollection<StateEntry> History { get; } = new List<StateEntry>(StateCountForRegularJob);
        public DateTime CreatedAt { get; private set; }
        public DateTime? ExpireAt { get; set; }

        public static BackgroundJobEntry Create(
            string key,
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            DateTime? expireAt,
            bool disableSerialization,
            StringComparer comparer)
        {
            return new BackgroundJobEntry
            {
                Key = key,
                InvocationData = disableSerialization == false ? InvocationData.SerializeJob(job) : null,
                Job = disableSerialization ? new Job(job.Type, job.Method, job.Args.ToArray(), job.Queue) : null,
                Parameters = new ConcurrentDictionary<string, string>(parameters, comparer),
                CreatedAt = createdAt,
                ExpireAt = expireAt
            };
        }

        public Job TryGetJob(out JobLoadException exception)
        {
            exception = null;

            if (Job != null)
            {
                return new Job(Job.Type, Job.Method, Job.Args.ToArray(), Job.Queue);
            }

            try
            {
                return InvocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                exception = ex;
                return null;
            }
        }
    }

    internal sealed class ServerEntry
    {
        public ServerContext Context { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime HeartbeatAt { get; set; }
    }

    internal sealed class LockEntry
    {
        public InMemoryConnection Owner { get; set; }
        public int ReferenceCount { get; set; }
        public int Level { get; set; }
    }

    internal sealed class QueueEntry
    {
        public ConcurrentQueue<string> Queue = new ConcurrentQueue<string>();
        public InMemoryQueueWaitNode WaitHead = new InMemoryQueueWaitNode(null);
    }

    internal struct SortedSetEntry
    {
        public SortedSetEntry(string value)
        {
            Value = value;
            Score = 0D;
        }

        public string Value { get; }
        public double Score { get; set; }
    }

    internal sealed class StateEntry
    {
        public string Name { get; set; }
        public string Reason { get; set; }
        // TODO: Encapsulate modification to ensure comparisons performed correctly
        public IDictionary<string, string> Data { get; }
        public DateTime CreatedAt { get; set; }
    }

    internal sealed class SortedSetEntryComparer : IComparer<SortedSetEntry>
    {
        private readonly StringComparer _stringComparer;

        public SortedSetEntryComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(SortedSetEntry x, SortedSetEntry y)
        {
            var scoreComparison = x.Score.CompareTo(y.Score);
            if (scoreComparison != 0 ||
                ReferenceEquals(null, y.Value) ||
                ReferenceEquals(null, x.Value))
            {
                return scoreComparison;
            }

            return _stringComparer.Compare(x.Value, y.Value);
        }
    }

    internal sealed class BackgroundJobStateCreatedAtComparer : IComparer<BackgroundJobEntry>
    {
        private readonly StringComparer _stringComparer;

        public BackgroundJobStateCreatedAtComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(BackgroundJobEntry x, BackgroundJobEntry y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (y?.State == null) return 1;
            if (x?.State == null) return -1;

            var stateCreatedAtComparison = x.State.CreatedAt.CompareTo(y.State.CreatedAt);
            if (stateCreatedAtComparison != 0) return stateCreatedAtComparison;

            var createdAtComparison = x.CreatedAt.CompareTo(y.CreatedAt);
            if (createdAtComparison != 0) return createdAtComparison;

            return _stringComparer.Compare(x.Key, y.Key);
        }
    }

    internal sealed class ExpirableEntryComparer : IComparer<IExpirableEntry>
    {
        private readonly StringComparer _stringComparer;

        public ExpirableEntryComparer(StringComparer stringComparer)
        {
            _stringComparer = stringComparer;
        }

        public int Compare(IExpirableEntry x, IExpirableEntry y)
        {
            if (ReferenceEquals(x, y)) return 0;

            // TODO: Nulls last, our indexes shouldn't contain nulls anyway
            // TODO: Check this
            if (x == null) return -1;
            if (y == null) return +1;

            if (!x.ExpireAt.HasValue)
            {
                throw new InvalidOperationException("Left side does not contain ExpireAt value");
            }

            if (!y.ExpireAt.HasValue)
            {
                throw new InvalidOperationException("Right side does not contain ExpireAt value");
            }

            var expirationCompare = x.ExpireAt.Value.CompareTo(y.ExpireAt.Value);
            if (expirationCompare != 0) return expirationCompare;

            return _stringComparer.Compare(x.Key, y.Key);
        }
    }
}
