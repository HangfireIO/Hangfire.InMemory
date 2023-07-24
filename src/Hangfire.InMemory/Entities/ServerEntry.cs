using System;
using Hangfire.Server;

namespace Hangfire.InMemory.Entities
{
    internal sealed class ServerEntry
    {
        public ServerContext Context { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime HeartbeatAt { get; set; }
    }
}