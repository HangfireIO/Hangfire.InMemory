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