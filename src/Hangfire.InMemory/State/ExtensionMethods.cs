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
using System.ComponentModel;
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
        public static Job? TryGetJob(this InvocationData? data, out JobLoadException? exception)
        {
            exception = null;

            try
            {
                if (data != null)
                {
                    return data.DeserializeJob();
                }
            }
            catch (JobLoadException ex)
            {
                exception = ex;
            }

            return null;
        }
    }
}

namespace System.Runtime.CompilerServices
{
#if !NET5_0_OR_GREATER

    [EditorBrowsable(EditorBrowsableState.Never)]
    internal static class IsExternalInit {}

#endif // !NET5_0_OR_GREATER

#if !NET7_0_OR_GREATER

    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    internal sealed class RequiredMemberAttribute : Attribute {}

    [AttributeUsage(AttributeTargets.All, AllowMultiple = true, Inherited = false)]
    internal sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        public CompilerFeatureRequiredAttribute(string featureName)
        {
            FeatureName = featureName;
        }

        public string FeatureName { get; }
        public bool   IsOptional  { get; init; }

        public const string RefStructs      = nameof(RefStructs);
        public const string RequiredMembers = nameof(RequiredMembers);
    }

#endif // !NET7_0_OR_GREATER
}

namespace System.Diagnostics.CodeAnalysis
{
#if !NET7_0_OR_GREATER
    [AttributeUsage(AttributeTargets.Constructor, AllowMultiple = false, Inherited = false)]
    internal sealed class SetsRequiredMembersAttribute : Attribute {}
#endif
}