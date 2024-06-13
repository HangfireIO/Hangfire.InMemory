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
    internal static class DispatcherExtensions
    {
        public static T? QueryWriteAndWait<TKey, T>(this DispatcherBase<TKey> dispatcher, ValueCommand<TKey, T?> query)
            where TKey : IComparable<TKey>
            where T : struct
        {
            return dispatcher.QueryWriteAndWait<ValueCommand<TKey, T?>, ValueCommand<TKey, T?>>(query).Result;
        }

        public static T QueryWriteAndWait<TKey, T>(this DispatcherBase<TKey> dispatcher, ValueCommand<TKey, T> query)
            where TKey : IComparable<TKey>
            where T : struct
        {
            return dispatcher.QueryWriteAndWait<ValueCommand<TKey, T>, ValueCommand<TKey, T>>(query).Result;
        }

        public static T QueryWriteAndWait<TKey, T>(this DispatcherBase<TKey> dispatcher, ICommand<TKey, T> query)
            where TKey : IComparable<TKey>
            where T : class?
        {
            return dispatcher.QueryWriteAndWait<ICommand<TKey, T>, T>(query);
        }

        public static T? QueryReadAndWait<TKey, T>(this DispatcherBase<TKey> dispatcher, ValueCommand<TKey, T?> query)
            where TKey : IComparable<TKey>
            where T : struct
        {
            return dispatcher.QueryReadAndWait<ValueCommand<TKey, T?>, ValueCommand<TKey, T?>>(query).Result;
        }

        public static T QueryReadAndWait<TKey, T>(this DispatcherBase<TKey> dispatcher, ValueCommand<TKey, T> query)
            where TKey : IComparable<TKey>
            where T : struct
        {
            return dispatcher.QueryReadAndWait<ValueCommand<TKey, T>, ValueCommand<TKey, T>>(query).Result;
        }

        public static T QueryReadAndWait<TKey, T>(this DispatcherBase<TKey> dispatcher, ICommand<TKey, T> query)
            where TKey : IComparable<TKey>
            where T : class?
        {
            return dispatcher.QueryReadAndWait<ICommand<TKey, T>, T>(query);
        }
    }
}