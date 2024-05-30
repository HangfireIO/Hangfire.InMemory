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

namespace Hangfire.InMemory.State
{
    internal interface ICommand<TKey, out T>
        where TKey : IComparable<TKey>
        where T : class?
    {
        T Execute(MemoryState<TKey> state);
    }

    internal interface ICommand<TKey> : ICommand<TKey, object?>
        where TKey : IComparable<TKey>
    {
    }

    internal abstract class Command<TKey, T> : ICommand<TKey, T>
        where TKey : IComparable<TKey>
        where T : class?
    {
        protected abstract T Execute(MemoryState<TKey> state);
        
        T ICommand<TKey, T>.Execute(MemoryState<TKey> state)
        {
            return Execute(state);
        }
    }
    
    internal abstract class ValueCommand<TKey, T> : ICommand<TKey, ValueCommand<TKey, T>>
        where TKey : IComparable<TKey>
    {
        public T? Result { get; private set; }

        protected abstract T Execute(MemoryState<TKey> state);

        ValueCommand<TKey, T> ICommand<TKey, ValueCommand<TKey, T>>.Execute(MemoryState<TKey> state)
        {
            Result = Execute(state);
            return this;
        }
    }
}