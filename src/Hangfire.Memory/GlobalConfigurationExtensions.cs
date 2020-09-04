using System;
using System.Runtime.CompilerServices;
using Hangfire.Memory;

[assembly: InternalsVisibleTo("Hangfire.Memory.Tests")]

namespace Hangfire
{
    public static class GlobalConfigurationExtensions
    {
        public static IGlobalConfiguration<MemoryStorage> UseMemoryStorage(this IGlobalConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            return configuration.UseStorage(new MemoryStorage());
        }
    }
}
