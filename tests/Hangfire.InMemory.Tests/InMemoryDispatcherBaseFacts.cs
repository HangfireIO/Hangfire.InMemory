// This file is part of Hangfire.InMemory. Copyright © 2024 Hangfire OÜ.
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
using System.Collections.Generic;
using System.Drawing.Drawing2D;
using System.Linq;
using Hangfire.Common;
using Hangfire.InMemory.Entities;
using Hangfire.InMemory.State;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryDispatcherBaseFacts
    {
        private readonly MonotonicTime _now;
        private readonly InMemoryStorageOptions _options;
        private readonly MemoryState<string> _state;
        private readonly TestInMemoryDispatcher<string> _dispatcher;
        private readonly Dictionary<string,string> _parameters;
        private readonly Job _job;

        public InMemoryDispatcherBaseFacts()
        {
            _now = MonotonicTime.GetCurrent();
            _options = new InMemoryStorageOptions();
            _state = new MemoryState<string>(_options.StringComparer, _options.StringComparer);
            _dispatcher = new TestInMemoryDispatcher<string>(() => _now, _state);
            _parameters = new Dictionary<string, string>();
            _job = Job.FromExpression<ITestServices>(x => x.Empty());
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStateIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new TestInMemoryDispatcher<string>(() => _now, null));
            
            Assert.Equal("state", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTimeResolverIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new TestInMemoryDispatcher<string>(null, _state));
            
            Assert.Equal("timeResolver", exception.ParamName);
        }

        [Fact]
        public void EvictEntries_EvictsExpiredJobs()
        {
            // Arrange
            _state.JobCreate(CreateJobEntry("job-1"), expireIn: TimeSpan.FromSeconds(-1));
            _state.JobCreate(CreateJobEntry("job-2"), expireIn: TimeSpan.FromMinutes(-1));
            _state.JobCreate(CreateJobEntry("job-3"), expireIn: TimeSpan.FromHours(-1));

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Empty(_state.Jobs);
        }

        [Fact]
        public void EvictEntries_EvictsExpiredHashes()
        {
            // Arrange
            _state.HashExpire(_state.HashGetOrAdd("hash-1"), _now, expireIn: TimeSpan.FromSeconds(-1), _options.MaxExpirationTime);
            _state.HashExpire(_state.HashGetOrAdd("hash-2"), _now, expireIn: TimeSpan.FromMinutes(-1), _options.MaxExpirationTime);
            _state.HashExpire(_state.HashGetOrAdd("hash-3"), _now, expireIn: TimeSpan.FromHours(-1), _options.MaxExpirationTime);

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Empty(_state.Hashes);
        }

        [Fact]
        public void EvictEntries_EvictsExpiredSets()
        {
            // Arrange
            _state.SetExpire(_state.SetGetOrAdd("set-1"), _now, expireIn: TimeSpan.FromSeconds(-1), _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("set-2"), _now, expireIn: TimeSpan.FromMinutes(-1), _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("set-3"), _now, expireIn: TimeSpan.FromHours(-1), _options.MaxExpirationTime);

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Empty(_state.Sets);
        }

        [Fact]
        public void EvictEntries_EvictsExpiredLists()
        {
            // Arrange
            _state.ListExpire(_state.ListGetOrAdd("list-1"), _now, expireIn: TimeSpan.FromSeconds(-1), _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("list-2"), _now, expireIn: TimeSpan.FromMinutes(-1), _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("list-3"), _now, expireIn: TimeSpan.FromHours(-1), _options.MaxExpirationTime);

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Empty(_state.Lists);
        }

        [Fact]
        public void EvictEntries_EvictsExpiredCounters()
        {
            // Arrange
            _state.CounterExpire(_state.CounterGetOrAdd("counter-1"), _now, expireIn: TimeSpan.FromSeconds(-1));
            _state.CounterExpire(_state.CounterGetOrAdd("counter-2"), _now, expireIn: TimeSpan.FromMinutes(-1));
            _state.CounterExpire(_state.CounterGetOrAdd("counter-3"), _now, expireIn: TimeSpan.FromHours(-1));

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Empty(_state.Counters);
        }

        [Fact]
        public void EvictEntries_DoesNotEvict_NonExpiringEntries()
        {
            // Arrange
            _state.JobCreate(CreateJobEntry("my-job"), expireIn: null);
            _state.HashExpire(_state.HashGetOrAdd("my-hash"), _now, expireIn: null, _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("my-set"), _now, expireIn: null, _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("my-list"), _now, expireIn: null, _options.MaxExpirationTime);
            _state.CounterExpire(_state.CounterGetOrAdd("my-counter"), _now, expireIn: null);

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Contains("my-job", _state.Jobs.Keys);
            Assert.Contains("my-hash", _state.Hashes.Keys);
            Assert.Contains("my-set", _state.Sets.Keys);
            Assert.Contains("my-list", _state.Lists.Keys);
            Assert.Contains("my-counter", _state.Counters.Keys);
        }

        [Fact]
        public void EvictEntries_DoesNotEvict_StillExpiringEntries()
        {
            // Arrange
            _state.JobCreate(CreateJobEntry("my-job"), expireIn: TimeSpan.FromMinutes(30));
            _state.HashExpire(_state.HashGetOrAdd("my-hash"), _now, expireIn: TimeSpan.FromMinutes(30), _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("my-set"), _now, expireIn: TimeSpan.FromMinutes(30), _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("my-list"), _now, expireIn: TimeSpan.FromMinutes(30), _options.MaxExpirationTime);
            _state.CounterExpire(_state.CounterGetOrAdd("my-counter"), _now, expireIn: TimeSpan.FromMinutes(30));

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Contains("my-job", _state.Jobs.Keys);
            Assert.Contains("my-hash", _state.Hashes.Keys);
            Assert.Contains("my-set", _state.Sets.Keys);
            Assert.Contains("my-list", _state.Lists.Keys);
            Assert.Contains("my-counter", _state.Counters.Keys);
        }

        [Fact]
        public void EvictEntries_DoesStumbleUpon_NonExpiredEntries()
        {
            // Arrange
            _state.JobCreate(CreateJobEntry("job-0"), expireIn: TimeSpan.Zero);
            _state.JobCreate(CreateJobEntry("job-1"), expireIn: null);
            _state.JobCreate(CreateJobEntry("job-2"), expireIn: TimeSpan.FromMinutes(30));
            _state.JobCreate(CreateJobEntry("job-3"), expireIn: TimeSpan.FromMinutes(-30));
            _state.HashExpire(_state.HashGetOrAdd("hash-0"), _now, expireIn: TimeSpan.Zero, _options.MaxExpirationTime);
            _state.HashExpire(_state.HashGetOrAdd("hash-1"), _now, expireIn: null, _options.MaxExpirationTime);
            _state.HashExpire(_state.HashGetOrAdd("hash-2"), _now, expireIn: TimeSpan.FromMinutes(30), _options.MaxExpirationTime);
            _state.HashExpire(_state.HashGetOrAdd("hash-3"), _now, expireIn: TimeSpan.FromMinutes(-30), _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("set-0"), _now, expireIn: TimeSpan.Zero, _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("set-1"), _now, expireIn: null, _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("set-2"), _now, expireIn: TimeSpan.FromMinutes(30), _options.MaxExpirationTime);
            _state.SetExpire(_state.SetGetOrAdd("set-3"), _now, expireIn: TimeSpan.FromMinutes(-30), _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("list-0"), _now, expireIn: TimeSpan.Zero, _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("list-1"), _now, expireIn: null, _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("list-2"), _now, expireIn: TimeSpan.FromMinutes(30), _options.MaxExpirationTime);
            _state.ListExpire(_state.ListGetOrAdd("list-3"), _now, expireIn: TimeSpan.FromMinutes(-30), _options.MaxExpirationTime);
            _state.CounterExpire(_state.CounterGetOrAdd("counter-0"), _now, expireIn: TimeSpan.Zero);
            _state.CounterExpire(_state.CounterGetOrAdd("counter-1"), _now, expireIn: null);
            _state.CounterExpire(_state.CounterGetOrAdd("counter-2"), _now, expireIn: TimeSpan.FromMinutes(30));
            _state.CounterExpire(_state.CounterGetOrAdd("counter-3"), _now, expireIn: TimeSpan.FromMinutes(-30));

            // Act
            _dispatcher.EvictExpiredEntries();

            // Assert
            Assert.Equal(new [] { "job-1", "job-2" }, _state.Jobs.Keys.OrderBy(x => x));
            Assert.Equal(new [] { "hash-1", "hash-2" }, _state.Hashes.Keys.OrderBy(x => x));
            Assert.Equal(new [] { "set-1", "set-2" }, _state.Sets.Keys.OrderBy(x => x));
            Assert.Equal(new [] { "list-1", "list-2" }, _state.Lists.Keys.OrderBy(x => x));
            Assert.Equal(new [] { "counter-1", "counter-2" }, _state.Counters.Keys.OrderBy(x => x));
        }

        private JobEntry<string> CreateJobEntry(string jobId)
        {
            return new JobEntry<string>(jobId, InvocationData.SerializeJob(_job), _parameters.ToArray(), _now);
        }
    }
}