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
using System.Collections.Concurrent;
using System.Threading;

namespace Hangfire.InMemory
{
    internal static class ConcurrentDictionaryExtensions
    {
        public static bool TryRemoveWorkaround<TKey, TValue>(
            this ConcurrentDictionary<TKey, TValue> dictionary,
            TKey key,
            out TValue removed)
        {
            if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));

            var hasRemoved = dictionary.TryRemove(key, out removed);

            // Workaround for issue https://github.com/dotnet/runtime/issues/107525, should be
            // removed after fix + some time.
            var spinWait = new SpinWait();
            while (!hasRemoved && dictionary.ContainsKey(key))
            {
                hasRemoved = dictionary.TryRemove(key, out removed);
                if (!hasRemoved) spinWait.SpinOnce();
            }

            return hasRemoved;
        }
    }
}