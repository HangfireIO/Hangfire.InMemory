using System;
using Hangfire.Annotations;
using Hangfire.InMemory;

namespace Hangfire
{
    public static class GlobalConfigurationExtensions
    {
        public static IGlobalConfiguration<InMemoryStorage> UseInMemoryStorage(
            [NotNull] this IGlobalConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            return configuration.UseStorage(new InMemoryStorage());
        }

        public static IGlobalConfiguration<InMemoryStorage> UseInMemoryStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] InMemoryStorageOptions options)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (options == null) throw new ArgumentNullException(nameof(options));

            return configuration.UseStorage(new InMemoryStorage(options));
        }
    }
}
