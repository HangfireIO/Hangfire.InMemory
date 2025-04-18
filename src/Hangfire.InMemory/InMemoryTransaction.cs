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
using System.Linq;
using Hangfire.Common;
using Hangfire.InMemory.State;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal sealed class InMemoryTransaction<TKey> : JobStorageTransaction
        where TKey : IComparable<TKey>
    {
        private readonly InMemoryConnection<TKey> _connection;

        private const int MaxCommandsInList = 4096;
        private readonly List<ICommand<TKey>> _commands = new List<ICommand<TKey>>();
        private LinkedList<ICommand<TKey>>? _additionalCommands;

        private List<IDisposable>? _acquiredLocks;
        private HashSet<string>? _enqueued;

        public InMemoryTransaction(InMemoryConnection<TKey> connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public override void Dispose()
        {
            if (_acquiredLocks != null)
            {
                foreach (var acquiredLock in _acquiredLocks)
                {
                    acquiredLock.Dispose();
                }
            }

            base.Dispose();
        }

        public override void Commit()
        {
            _connection.Dispatcher.QueryWriteAndWait(this, static (t, s) => t.CommitCore(s));
        }

#if !HANGFIRE_170
        public override void AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            var disposableLock = _connection.AcquireDistributedLock(resource, timeout);

            _acquiredLocks ??= new List<IDisposable>();
            _acquiredLocks.Add(disposableLock);
        }
#endif

#if !HANGFIRE_170
        public override string CreateJob(Job job, IDictionary<string, string?> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var key = _connection.KeyProvider.GetUniqueKey();
            var data = InvocationData.SerializeJob(job);
            var now = _connection.Dispatcher.GetMonotonicTime();

            AddCommand(new Commands<TKey>.JobCreate(key, data, parameters.ToArray(), now, expireIn));
            return _connection.KeyProvider.ToString(key);
        }
#endif

#if !HANGFIRE_170
        public override void SetJobParameter(
            string jobId,
            string name,
            string? value)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (name == null) throw new ArgumentNullException(nameof(name));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            AddCommand(new Commands<TKey>.JobSetParameter(key, name, value));
        }
#endif

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new Commands<TKey>.JobExpire(key, now, expireIn, _connection.Options.MaxExpirationTime));
        }

        public override void PersistJob(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            AddCommand(new Commands<TKey>.JobPersist(key));
        }

        public override void SetJobState(string jobId, IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));
            if (state.Name == null) throw new ArgumentException("Name property must not return null.", nameof(state));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var name = state.Name;
            var reason = state.Reason;
            var data = state.SerializeData()?.ToArray() ?? [];
            var now = _connection.Dispatcher.GetMonotonicTime();

            AddCommand(new Commands<TKey>.JobSetState(
                key, name, reason, data, now, _connection.Options.MaxStateHistoryLength));
        }

        public override void AddJobState(string jobId, IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            // IState can be implemented by user, and potentially can throw exceptions.
            // Getting data here, out of the dispatcher thread, to avoid killing it.
            var name = state.Name;
            var reason = state.Reason;
            var data = state.SerializeData()?.ToArray() ?? [];
            var now = _connection.Dispatcher.GetMonotonicTime();

            AddCommand(new Commands<TKey>.JobAddState(
                key, name, reason, data, now, _connection.Options.MaxStateHistoryLength));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            if (!_connection.KeyProvider.TryParse(jobId, out var key))
            {
                return;
            }

            AddCommand(new Commands<TKey>.QueueEnqueue(queue, key));

            _enqueued ??= new HashSet<string>();
            _enqueued.Add(queue);
        }

#if !HANGFIRE_170
        public override void RemoveFromQueue(IFetchedJob fetchedJob)
        {
            // Nothing to do here
        }
#endif

        public override void IncrementCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            AddCommand(new Commands<TKey>.CounterIncrement(key, value: 1));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new Commands<TKey>.CounterIncrementAndExpire(key, value: 1, now, expireIn));
        }

        public override void DecrementCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            AddCommand(new Commands<TKey>.CounterIncrement(key, value: -1));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new Commands<TKey>.CounterIncrementAndExpire(key, value: -1, now, expireIn));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, score: 0.0D);
        }

        public override void AddToSet(string key, string value, double score)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new Commands<TKey>.SortedSetAdd(key, value, score));
        }

        public override void RemoveFromSet(string key, string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new Commands<TKey>.SortedSetRemove(key, value));
        }

        public override void InsertToList(string key, string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new Commands<TKey>.ListInsert(key, value));
        }

        public override void RemoveFromList(string key, string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            AddCommand(new Commands<TKey>.ListRemoveAll(key, value));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new Commands<TKey>.ListTrim(key, keepStartingFrom, keepEndingAt));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            AddCommand(new Commands<TKey>.HashSetRange(key, keyValuePairs));
        }

        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new Commands<TKey>.HashRemove(key));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            foreach (var item in items)
            {
                if (item == null) throw new ArgumentException("The list of items must not contain any `null` entries.", nameof(items));
            }

            if (items.Count == 0) return;

            AddCommand(new Commands<TKey>.SortedSetAddRange(key, items));
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new Commands<TKey>.SortedSetDelete(key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new Commands<TKey>.HashExpire(key, now, expireIn, _connection.Options.MaxExpirationTime));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new Commands<TKey>.ListExpire(key, now, expireIn, _connection.Options.MaxExpirationTime));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var now = _connection.Dispatcher.GetMonotonicTime();
            AddCommand(new Commands<TKey>.SortedSetExpire(key, now, expireIn, _connection.Options.MaxExpirationTime));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new Commands<TKey>.HashPersist(key));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new Commands<TKey>.ListPersist(key));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            AddCommand(new Commands<TKey>.SortedSetPersist(key));
        }

        private void AddCommand(ICommand<TKey> action)
        {
            if (_commands.Count < MaxCommandsInList)
            {
                _commands.Add(action);
            }
            else
            {
                _additionalCommands ??= new LinkedList<ICommand<TKey>>();
                _additionalCommands.AddLast(action);
            }
        }

        private bool CommitCore(MemoryState<TKey> state)
        {
            try
            {
                // TODO: Theoretically it's possible that depending on the actual dispatcher implementation,
                // the call to the QueryWriteAndWait in the Commit method will end a timeout exception,
                // and its connection instance will be able to asynchronously release the locks while
                // transaction itself is running.
                // This is not something that's possible in the current implementation, since transactions
                // are always running in isolation. But it might happen, when queries are running in
                // parallel.
                // In this case, we should freeze the locks before executing a transaction and failing it
                // before running commands if any owner is changed. Any attempt to release such a frozen
                // lock should fail itself, but be recorded to allow cleaning it (and removing from the
                // lock collection) when un-freeze method is called to avoid having abandoned locks.
                foreach (var command in _commands)
                {
                    command.Execute(state);
                }

                if (_additionalCommands != null)
                {
                    foreach (var command in _additionalCommands)
                    {
                        command.Execute(state);
                    }
                }
            }
            finally
            {
                if (_acquiredLocks != null)
                {
                    foreach (var acquiredLock in _acquiredLocks)
                    {
                        acquiredLock.Dispose();
                    }
                }
            }

            if (_enqueued != null)
            {
                foreach (var queue in _enqueued)
                {
                    state.QueueGetOrAdd(queue).SignalOneWaitNode();
                }
            }

            return true;
        }
    }
}