# Hangfire.InMemory

[![Latest version](https://img.shields.io/nuget/v/Hangfire.InMemory.svg)](https://www.nuget.org/packages/Hangfire.InMemory/) [![Build status](https://ci.appveyor.com/api/projects/status/yq82w8ji419c61vy?svg=true)](https://ci.appveyor.com/project/HangfireIO/hangfire-inmemory) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=HangfireIO_Hangfire.InMemory&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=HangfireIO_Hangfire.InMemory) [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=HangfireIO_Hangfire.InMemory&metric=bugs)](https://sonarcloud.io/summary/new_code?id=HangfireIO_Hangfire.InMemory) [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=HangfireIO_Hangfire.InMemory&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=HangfireIO_Hangfire.InMemory)

This in-memory job storage aims to provide developers a quick way to start using Hangfire without additional infrastructure like SQL Server or Redis. It offers the flexibility to swap out the in-memory implementation in a production environment. This is not intended to compete with TPL or other in-memory processing libraries, as serialization itself incurs a significant overhead.

The implementation includes proper synchronization mechanisms, utilizing blocking operations for locks, queues, and queries. This avoids active polling. Read and write operations are handled by a dedicated background thread to minimize synchronization between threads, keeping the system simple and ready for future async-based enhancements. Internal states use `SortedDictionary`, `SortedSet`, and `LinkedList` to avoid large allocations on the Large Object Heap, thus reducing potential `OutOfMemoryException` issues caused by memory fragmentation.

Additionally, this storage uses a monotonic clock where possible, using the `Stopwatch.GetTimestamp` method. This ensures that expiration rules remain effective even if the system clock changes abruptly.

## Requirements

* **Hangfire 1.8.0** and above: any latest version of Hangfire.InMemory
* **Hangfire 1.7.0**: Hangfire.InMemory 0.11.0 and above
* **Hangfire 1.6.0**: Hangfire.InMemory 0.3.7

## Installation

[Hangfire.InMemory](https://www.nuget.org/packages/Hangfire.InMemory/) is available on NuGet. You can install it using your preferred package manager:

```powershell
> dotnet add package Hangfire.InMemory
```

## Configuration

After installing the package, configure it using the `UseInMemoryStorage` method on the `IGlobalConfiguration` interface:

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage();
```

### Maximum Expiration Time

Starting from version 0.7.0, the package controls the maximum expiration time for storage entries and sets it to *3 hours* by default when a higher expiration time is passed. For example, the default expiration time for background jobs is *24 hours*, and for batch jobs and their contents, the default time is *7 days*, which can be too big for in-memory storage that runs side-by-side with the application.

You can control this behavior or even turn it off with the `MaxExpirationTime` option available in the `InMemoryStorageOptions` class in the following way:

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage(new InMemoryStorageOptions
{
    MaxExpirationTime = TimeSpan.FromHours(3) // Default value, we can also set it to `null` to disable.
});
```

It is also possible to use `TimeSpan.Zero` as a value for this option. In this case, entries will be removed immediately instead of relying on the time-based eviction implementation. Please note that some unwanted side effects may appear when using low value – for example, an antecedent background job may be created, processed, and expired before its continuation is created, resulting in exceptions.

### Comparing Keys

Different storages use different rules for comparing keys. Some of them, like Redis, use case-sensitive comparisons, while others, like SQL Server, may use case-insensitive comparison implementation. It is possible to set this behavior explicitly and simplify moving to another storage implementation in a production environment by configuring the `StringComparer` option in the `InMemoryStorageOptions` class in the following way:

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage(new InMemoryStorageOptions
{
    StringComparer = StringComparer.Ordinal // Default value, case-sensitive.
});
```

### Setting Key Type Jobs

Starting from version 1.0, Hangfire.InMemory uses `long`-based keys for background jobs, similar to the Hangfire.SqlServer package. However, you can change this to use `Guid`-based keys to match the Hangfire.Pro.Redis experience. To do so, simply configure the `InMemoryStorageOptions.IdType` property as follows:

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage(new InMemoryStorageOptions
{
    IdType = InMemoryStorageIdType.Guid
});
```

### Setting the Maximum State History Length

The `MaxStateHistoryLength` option in the `InMemoryStorageOptions` class sets the maximum number of state history entries to be retained for each background job. This is useful for controlling memory usage by limiting the number of state transitions stored in memory. 

By default, Hangfire.InMemory retains `10` state history entries, but you can adjust this setting based on your application's requirements.

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage(new InMemoryStorageOptions
{
    MaxStateHistoryLength = 10 // default value
});
```