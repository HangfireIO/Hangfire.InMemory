// This file is part of Hangfire.InMemory. Copyright © 2023 Hangfire OÜ.
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
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class GlobalConfigurationExtensionsFacts
    {
        private static readonly object SyncRoot = new Object();

        [Fact]
        public void UseInMemoryStorage_SetsTheGlobalJobStorageInstance_WithTheDefaultOptions()
        {
            lock (SyncRoot)
            {
                GlobalConfiguration.Configuration.UseInMemoryStorage();
                Assert.IsType<InMemoryStorage>(JobStorage.Current);
                Assert.NotNull(((InMemoryStorage)JobStorage.Current).Options);
            }
        }

        [Fact]
        public void UseInMemoryStorage_WithOptions_ThrowsAnException_WhenOptionsArgument_IsNull()
        {
            lock (SyncRoot)
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => GlobalConfiguration.Configuration.UseInMemoryStorage(null));

                Assert.Equal("options", exception.ParamName);
            }
        }

        [Fact]
        public void UseInMemoryStorage_WithOptions_SetsTheGlobalJobStorageInstance_WithTheGivenOptions()
        {
            lock (SyncRoot)
            {
                var options = new InMemoryStorageOptions();
                GlobalConfiguration.Configuration.UseInMemoryStorage(options);
                Assert.IsType<InMemoryStorage>(JobStorage.Current);
                Assert.Same(options, ((InMemoryStorage)JobStorage.Current).Options);
            }
        }
    }
}