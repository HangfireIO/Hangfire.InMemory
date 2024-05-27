// This file is part of Hangfire.InMemory. Copyright © 2023 Hangfire OÜ.
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
using System.Linq;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryFetchedJobFacts
    {
        private readonly InMemoryState<string> _state;
        private readonly InMemoryDispatcherBase<string> _dispatcher;
        private readonly StringKeyProvider _keyProvider;
        private readonly InMemoryConnection<string> _connection;

        public InMemoryFetchedJobFacts()
        {
            var now = MonotonicTime.GetCurrent();
            var options = new InMemoryStorageOptions();
            _state = new InMemoryState<string>(options, options.StringComparer);
            _dispatcher = new TestInMemoryDispatcher<string>(() => now, _state);
            _keyProvider = new StringKeyProvider();
            _connection = new InMemoryConnection<string>(_dispatcher, _keyProvider);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryFetchedJob<string>(null, "default", "123"));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryFetchedJob<string>(_connection, null, "123"));

            Assert.Equal("queueName", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryFetchedJob<string>(_connection, "default", null));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllTheProperties()
        {
            var fetched = new InMemoryFetchedJob<string>(_connection, "critical", "12345");

            Assert.Equal("critical", fetched.QueueName);
            Assert.Equal("12345", fetched.JobId);
        }

        [Fact]
        public void Requeue_EnqueuesTheGivenJobId_ToTheGivenQueue()
        {
            var fetched = new InMemoryFetchedJob<string>(_connection, "critical", "12345");

            fetched.Requeue();

            Assert.Equal("12345", _state.Queues["critical"].Queue.Single());
        }

        [Fact]
        public void RemoveFromQueue_DoesNotDoAnything()
        {
            IFetchedJob fetched = new InMemoryFetchedJob<string>(_connection, "critical", "12345");
            fetched.RemoveFromQueue();
        }

        [Fact]
        public void Dispose_DoesNotDoAnything()
        {
            IFetchedJob fetched = new InMemoryFetchedJob<string>(_connection, "critical", "12345");
            fetched.Dispose();
        }
    }
}