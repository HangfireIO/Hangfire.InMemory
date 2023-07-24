using System;
using Hangfire.Server;

namespace Hangfire.InMemory.Entities
{
    internal sealed class ServerEntry
    {
        public ServerEntry(ServerContext context, DateTime startedAt)
        {
            Context = context;
            StartedAt = startedAt;
            HeartbeatAt = startedAt;
        }

        public ServerContext Context { get; }
        public DateTime StartedAt { get; }
        public DateTime HeartbeatAt { get; set; }
    }
}