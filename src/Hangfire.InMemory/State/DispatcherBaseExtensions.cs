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
    internal static class DispatcherBaseExtensions
    {
        public static void QueryWriteAndWait<TKey, TLockOwner, TCommand>(this DispatcherBase<TKey, TLockOwner> dispatcher, TCommand query)
            where TKey : IComparable<TKey>
            where TLockOwner : class
            where TCommand : ICommand<TKey>
        {
            dispatcher.QueryWriteAndWait(query, static (q, s) =>
            {
                q.Execute(s);
                return true;
            });
        }
    }
}