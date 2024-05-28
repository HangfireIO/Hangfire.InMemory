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
using System.Runtime.CompilerServices;

namespace Hangfire.InMemory
{
    internal interface IInMemoryCommand<TKey, out T>
        where TKey : IComparable<TKey>
        where T : class
    {
        T Execute(InMemoryState<TKey> state);
    }

    internal interface IInMemoryCommand<TKey> : IInMemoryCommand<TKey, object>
        where TKey : IComparable<TKey>
    {
    }

    internal abstract class InMemoryCommand<TKey, T> : IInMemoryCommand<TKey, T>
        where TKey : IComparable<TKey>
        where T : class
    {
        protected abstract T Execute(InMemoryState<TKey> state);
        
        T IInMemoryCommand<TKey, T>.Execute(InMemoryState<TKey> state)
        {
            return Execute(state);
        }
    }
    
    internal abstract class InMemoryValueCommand<TKey, T> : IInMemoryCommand<TKey, StrongBox<T>>
        where TKey : IComparable<TKey>
    {
        protected abstract T Execute(InMemoryState<TKey> state);

        StrongBox<T> IInMemoryCommand<TKey, StrongBox<T>>.Execute(InMemoryState<TKey> state)
        {
            return new StrongBox<T>(Execute(state));
        }
    }
}