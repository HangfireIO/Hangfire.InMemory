using System;
using Hangfire.InMemory;

namespace Hangfire
{
    public static class GlobalConfigurationExtensions
    {
        public static IGlobalConfiguration<InMemoryStorage> UseInMemoryStorage(this IGlobalConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            return configuration.UseStorage(new InMemoryStorage());
        }
    }
}
