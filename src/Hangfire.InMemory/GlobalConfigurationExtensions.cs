// This file is part of Hangfire.InMemory. Copyright © 2020 Hangfire OÜ.
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
using System.Diagnostics.CodeAnalysis;
using Hangfire.Annotations;
using Hangfire.InMemory;

// ReSharper disable once CheckNamespace
namespace Hangfire
{
    /// <summary>
    /// Provides extension methods for global configuration to use <see cref="InMemoryStorage"/>.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class GlobalConfigurationExtensions
    {
        /// <summary>
        /// Configures Hangfire to use the <see cref="InMemoryStorage"/> with default options.
        /// </summary>
        /// <param name="configuration">The global configuration on which to set the in-memory storage.</param>
        /// <returns>An instance of <see cref="IGlobalConfiguration{InMemoryStorage}"/> for chaining further configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when the <paramref name="configuration"/> argument is null.</exception>
        [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Created in a static scope")]
        public static IGlobalConfiguration<InMemoryStorage> UseInMemoryStorage(
            [NotNull] this IGlobalConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            return configuration.UseStorage(new InMemoryStorage());
        }

        /// <summary>
        /// Configures Hangfire to use the <see cref="InMemoryStorage"/> with the specified options.
        /// </summary>
        /// <param name="configuration">The global configuration on which to set the in-memory storage.</param>
        /// <param name="options">Options for the in-memory storage.</param>
        /// <returns>An instance of <see cref="IGlobalConfiguration{InMemoryStorage}"/> for chaining further configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when the <paramref name="configuration"/> or <paramref name="options"/> argument is null.</exception>
        [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Created in a static scope")]
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
