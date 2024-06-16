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
using System.Threading;
using Hangfire.InMemory.Entities;
using Xunit;

namespace Hangfire.InMemory.Tests.Entities
{
    public class LockEntryFacts
    {
        private readonly object _owner = new Object();

        [Fact]
        public void TryAcquire_ThrowsAnException_WhenOwnerIsNull()
        {
            var entry = CreateLock();

            var exception = Assert.Throws<ArgumentNullException>(
                () => entry.TryAcquire(null, TimeSpan.Zero, out _, out _));

            Assert.Equal("owner", exception.ParamName);
        }

        [Fact]
        public void TryAcquire_AcquiresAnEmptyLock()
        {
            var entry = CreateLock();

            var acquired = entry.TryAcquire(_owner, TimeSpan.Zero, out var retry, out var cleanUp);

            Assert.True(acquired);
            Assert.False(retry);
            Assert.False(cleanUp);
        }

        [Fact]
        public void TryAcquire_AcquiresALock_AlreadyAcquiredByTheSameOwner()
        {
            var entry = CreateLock();

            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            var acquired = entry.TryAcquire(_owner, TimeSpan.Zero, out var retry, out var cleanUp);

            Assert.True(acquired);
            Assert.False(retry);
            Assert.False(cleanUp);
        }

        [Fact]
        public void TryAcquire_DoesNotAcquire_AlreadyOwnedLock()
        {
            var entry = CreateLock();

            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            var acquired = entry.TryAcquire(new object(), TimeSpan.Zero, out var retry, out var cleanUp);

            Assert.False(acquired);
            Assert.False(retry);
            Assert.False(cleanUp);
        }

        [Fact]
        public void TryAcquire_CanAcquire_AnAlreadyReleasedLock_ByAnotherOwner()
        {
            var entry = CreateLock();
            var ready = new ManualResetEventSlim(initialState: false);
            var another = new object();

            entry.TryAcquire(another, TimeSpan.Zero, out _, out _);

            ThreadPool.QueueUserWorkItem(delegate
            {
                ready.Set();
                var acquired = entry.TryAcquire(_owner, TimeSpan.FromSeconds(5), out _, out _);

                entry.Release(_owner, out var cleanUp);

                Assert.True(acquired);
                Assert.True(cleanUp);
            });

            Assert.True(ready.Wait(TimeSpan.FromSeconds(1)));
            Thread.Sleep(2000);
            entry.Release(another, out var anotherCleanUp);

            Assert.False(anotherCleanUp);
        }

        [Fact]
        public void TryAcquire_OnAFinalizedLock_RequiresRetry()
        {
            var entry = CreateLock();
            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            entry.Release(_owner, out _);

            var acquired = entry.TryAcquire(_owner, TimeSpan.Zero, out var retry, out var cleanUp);

            Assert.False(acquired);
            Assert.True(retry);
            Assert.False(cleanUp);
        }

        [Fact]
        public void Release_ThrowsAnException_WhenOwnerIsNull()
        {
            var entry = CreateLock();

            var exception = Assert.Throws<ArgumentNullException>(
                () => entry.Release(null, out _));

            Assert.Equal("owner", exception.ParamName);
        }

        [Fact]
        public void Release_ThrowsAnException_WhenAttemptedToBeReleasedByAnotherOwner()
        {
            var entry = CreateLock();
            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);

            var exception = Assert.Throws<ArgumentException>(
                () => entry.Release(new object(), out _));

            Assert.Equal("owner", exception.ParamName);
        }

        [Fact]
        public void Release_FinalizesLock_AcquiredByTheSameOwner_WithNoOtherReferences()
        {
            var entry = CreateLock();

            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            entry.Release(_owner, out var cleanUp);

            Assert.True(cleanUp);
        }

        [Fact]
        public void Release_DoesNotFinalizeLock_AcquiredMultipleTimes_AndNotFullyReleased()
        {
            var entry = CreateLock();

            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            entry.Release(_owner, out var cleanUp);

            Assert.False(cleanUp);
        }

        [Fact]
        public void Release_FinalizesLock_AcquiredMultipleTimes_AndFullyReleased()
        {
            var entry = CreateLock();

            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            entry.TryAcquire(_owner, TimeSpan.Zero, out _, out _);
            entry.Release(_owner, out _);
            entry.Release(_owner, out var cleanUp);

            Assert.True(cleanUp);
        }

        private static LockEntry<object> CreateLock() => new LockEntry<object>();
    }
}