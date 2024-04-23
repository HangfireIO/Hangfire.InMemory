# Hangfire.InMemory

[![Build status](https://ci.appveyor.com/api/projects/status/yq82w8ji419c61vy?svg=true)](https://ci.appveyor.com/project/HangfireIO/hangfire-inmemory)

This is an attempt to create an efficient transactional in-memory storage for Hangfire with data structures close to their optimal representation. The result of this attempt should enable production-ready usage of this storage implementation and handle particular properties of in-memory processing like avoiding `OutOfMemoryException` at any cost and moderate load on garbage collection. Of course we can't avoid them entirely, but at least can control them somehow.

Read and write queries are processed by a single thread to avoid additional synchronization between threads and keep everything as simple as possible (keeping future async-based implementation in mind). Monitoring API also uses that dedicated thread, but its future implementation can be changed by using concurrent data structures and immutability, but I expect this will increase load on garbage collection.

Distributed locks (heh, in an in-process storage), queue fetch logic (even from multiple queues) and transactional queries are implemented as blocking operations, so there is no active polling in these cases. Every data returned by storage can be safely changed without causing underlying storage state to be changed with bypassing required transactional processing logic, so everything is safe (but increase load on GC). Every data structure, including indexes and their records, is removed when empty to avoid memory leaks.

## Installation

[Hangfire.InMemory](https://www.nuget.org/packages/Hangfire.InMemory/) is available on NuGet so we can install it as usual using your favorite package manager.

```powershell
> dotnet add package Hangfire.InMemory
```

## Configuration

After the package is installed we can use the new `UseInMemoryStorage` method for the `IGlobalConfiguration` interface to register the storage.

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage();
```

### Maximum Expiration Time

Starting from version 0.7.0, the package controls the maximum expiration time for storage entries and sets it to *2 hours* when a higher expiration time is passed. By default, the expiration time for background jobs is *24 hours*, and for batch jobs and their contents is *7 days* which can be too big for in-memory storage that runs side-by-side with the application.

We can control this behavior or even disable it with the `MaxExpirationTime` option available in the `InMemoryStorageOptions` class in the following way.

```csharp
GlobalConfiguration.Configuration.UseInMemoryStorage(new InMemoryStorageOptions
{
    MaxExpirationTime = TimeSpan.FromHours(6) // Or set to `null` to disable
});
```
