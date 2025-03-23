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
using Hangfire.InMemory.Entities;

namespace Hangfire.InMemory.State
{
    internal interface IMemoryState<TKey> where TKey : IComparable<TKey>
    {
        StringComparer StringComparer { get; }

        IReadOnlyCollection<string> QueueGetIndex();
        bool QueueTryGet(string name, out QueueEntry<TKey> entry);
        QueueEntry<TKey> QueueGetOrAdd(string name);

        bool JobTryGet(TKey key, out JobEntry<TKey> entry);
        bool JobTryGetStateIndex(string name, out ISet<TKey> indexEntry);
        void JobCreate(JobEntry<TKey> entry, TimeSpan? expireIn);
        void JobSetState(JobEntry<TKey> entry, StateRecord state);
        void JobExpire(JobEntry<TKey> entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration);

        bool HashTryGet(string key, out HashEntry entry);
        HashEntry HashGetOrAdd(string key);
        void HashExpire(HashEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration);
        void HashDelete(HashEntry entry);

        bool SetTryGet(string key, out SetEntry entry);
        SetEntry SetGetOrAdd(string key);
        void SetExpire(SetEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration);
        void SetDelete(SetEntry entry);

        bool ListTryGet(string key, out ListEntry entry);
        ListEntry ListGetOrAdd(string key);
        void ListExpire(ListEntry entry, MonotonicTime? now, TimeSpan? expireIn, TimeSpan? maxExpiration);
        void ListDelete(ListEntry entry);

        bool CounterTryGet(string key, out CounterEntry entry);
        CounterEntry CounterGetOrAdd(string key);
        void CounterExpire(CounterEntry entry, MonotonicTime? now, TimeSpan? expireIn);
        void CounterDelete(CounterEntry entry);

        IReadOnlyCollection<string> ServerGetIndex();
        bool ServerTryGet(string serverId, out ServerEntry entry);
        bool ServerTryAdd(string serverId, ServerEntry entry);
        bool ServerRemove(string serverId);

        void EvictExpiredEntries(MonotonicTime now);
    }
}