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
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal static class ExtensionMethods
    {
        public static Job TryGetJob(this InvocationData data, out JobLoadException exception)
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