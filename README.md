# Hangfire.InMemory

[![Latest version](https://img.shields.io/nuget/v/Hangfire.InMemory.svg)](https://www.nuget.org/packages/Hangfire.InMemory/) [![Build status](https://ci.appveyor.com/api/projects/status/yq82w8ji419c61vy?svg=true)](https://ci.appveyor.com/project/HangfireIO/hangfire-inmemory) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=HangfireIO_Hangfire.InMemory&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=HangfireIO_Hangfire.InMemory) [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=HangfireIO_Hangfire.InMemory&metric=bugs)](https://sonarcloud.io/summary/new_code?id=HangfireIO_Hangfire.InMemory) [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=HangfireIO_Hangfire.InMemory&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=HangfireIO_Hangfire.InMemory)

This is an efficient implementation of in-memory job storage for Hangfire with data structures close to their optimal representation.

The goal of this storage is to provide developers a fast path to start using Hangfire without setting up any additional infrastructure like SQL Server or Redis, with the possibility of swapping implementation in the production environment. The non-goal of this implementation is to compete with TPL or other in-memory processing libraries – serialization alone adds a significant overhead.

This implementation uses proper synchronization, and everything, including locks, queues, or queries, is implemented as blocking operations, so there's no active polling in these cases. Read and write queries are processed by a dedicated background thread to avoid additional synchronization between threads, trying to keep everything as simple as possible with the future async-based implementation in mind. The internal state uses `SortedDictionary`, `SortedSet`, and `LinkedList` data structures to avoid getting even huge collections to the Large Object Heap, reducing the potential `OutOfMemoryException`-related issues due to memory fragmentation.

This storage also uses a monotonic clock whenever possible by leveraging the `Stopwatch.GetTimestamp` method. So, expiration rules don't break when the clock suddenly jumps to the future or the past due to synchronization issues or manual updates.

## Requirements

Minimal supported version of Hangfire is 1.8.0. Latest version that supports previous versions of Hangfire is Hangfire.InMemory 0.3.7.

## Installation

[Hangfire.InMemory](https://www.nuget.org/packages/Hangfire.InMemory/) is available on NuGet, so we can install it, as usual, using our favorite package manager.

```powershell
> dotnet add package Hangfire.InMemory
```

## Configuration

After the package is installed, we can use the new `UseInMemoryStorage` method for the `IGlobalConfiguration` interface to register the storage.

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage();
```

### Maximum Expiration Time

Starting from version 0.7.0, the package controls the maximum expiration time for storage entries and sets it to *3 hours* by default when a higher expiration time is passed. For example, the default expiration time for background jobs is *24 hours*, and for batch jobs and their contents, the default time is *7 days*, which can be too big for in-memory storage that runs side-by-side with the application.

We can control this behavior or even turn it off with the `MaxExpirationTime` option available in the `InMemoryStorageOptions` class in the following way:

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