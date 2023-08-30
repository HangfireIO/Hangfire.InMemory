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
        // TODO: Consider replacing DateTime with monotonic clocks almost everywhere
        public DateTime StartedAt { get; }
        public DateTime HeartbeatAt { get; set; }
    }
}