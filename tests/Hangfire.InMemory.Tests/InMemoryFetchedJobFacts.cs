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
        private readonly InMemoryState _state;
        private readonly InMemoryDispatcherBase _dispatcher;

        public InMemoryFetchedJobFacts()
        {
            var now = MonotonicTime.GetCurrent();
            _state = new InMemoryState(() => now, new InMemoryStorageOptions());
            _dispatcher = new TestInMemoryDispatcher(_state);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDispatcherIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryFetchedJob(null, "default", "123"));

            Assert.Equal("dispatcher", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryFetchedJob(_dispatcher, null, "123"));

            Assert.Equal("queueName", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new InMemoryFetchedJob(_dispatcher, "default", null));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllTheProperties()
        {
            var fetched = new InMemoryFetchedJob(_dispatcher, "critical", "12345");

            Assert.Equal("critical", fetched.QueueName);
            Assert.Equal("12345", fetched.JobId);
        }

        [Fact]
        public void Requeue_EnqueuesTheGivenJobId_ToTheGivenQueue()
        {
            var fetched = new InMemoryFetchedJob(_dispatcher, "critical", "12345");

            fetched.Requeue();

            Assert.Equal("12345", _state.Queues["critical"].Queue.Single());
        }

        [Fact]
        public void RemoveFromQueue_DoesNotDoAnything()
        {
            IFetchedJob fetched = new InMemoryFetchedJob(_dispatcher, "critical", "12345");
            fetched.RemoveFromQueue();
        }

        [Fact]
        public void Dispose_DoesNotDoAnything()
        {
            IFetchedJob fetched = new InMemoryFetchedJob(_dispatcher, "critical", "12345");
            fetched.Dispose();
        }
    }
}