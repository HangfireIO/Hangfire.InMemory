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
using System.Runtime.InteropServices;
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.InMemory.State
{
    [StructLayout(LayoutKind.Explicit, Size = 2 * CacheLineSize)]
    internal struct PaddedInt64
    {
        private const int CacheLineSize = 128;

        [FieldOffset(CacheLineSize)]
        internal long Value;
    }

    internal static class ExceptionHelper
    {
#if !NETSTANDARD1_3
        private static readonly Type StackOverflowType = typeof(StackOverflowException);
#endif
        private static readonly Type OutOfMemoryType = typeof(OutOfMemoryException);
 
        public static bool IsCatchableExceptionType(Exception ex)
        {
            var type = ex.GetType();
            return
#if !NETSTANDARD1_3
                type != StackOverflowType &&
#endif
                type != OutOfMemoryType;
        }
    }

    internal static class ExtensionMethods
    {
        public static Job? TryGetJob(this InvocationData data, out JobLoadException? exception)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            exception = null;

            try
            {
                return data.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                exception = ex;
                return null;
            }
        }
    }
}