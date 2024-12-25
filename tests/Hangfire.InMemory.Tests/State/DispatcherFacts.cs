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
using System.Runtime.CompilerServices;
using Hangfire.InMemory.State;
using Xunit;

namespace Hangfire.InMemory.Tests.State
{
    public class DispatcherFacts
    {
        private readonly MemoryState<string> _state;
        private readonly MonotonicTime _now;
        private readonly Func<MonotonicTime> _timeResolver;

        public DispatcherFacts()
        {
            var options = new InMemoryStorageOptions();
            _now = MonotonicTime.GetCurrent();
            _state = new MemoryState<string>(options.StringComparer, options.StringComparer);
            _timeResolver = () => _now;
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenThreadNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new Dispatcher<string, InMemoryConnection<string>>(null!, _timeResolver, _state));

            Assert.Equal("threadName", exception.ParamName);
        }

        [Fact]
        public void Dispose_DisposesTheDispatcherInstance_WithoutAnyException()
        {
            using (CreateDispatcher())
            {
            }
        }

        [Fact]
        public void QueryWriteAndWait_IsBeingEventuallyExecuted()
        {
            using var dispatcher = CreateDispatcher();
            var box = new StrongBox<bool>();

            var result = dispatcher.QueryWriteAndWait(box, static (b, _) => b.Value = true);

            Assert.True(box.Value);
            Assert.True(result);
        }

        [Fact]
        public void QueryReadAndWait_IsBeingEventuallyExecuted()
        {
            using var dispatcher = CreateDispatcher();

            var result = dispatcher.QueryReadAndWait(false, static (_, _) => true);

            Assert.True(result);
        }

        private Dispatcher<string, InMemoryConnection<string>> CreateDispatcher()
        {
            return new Dispatcher<string, InMemoryConnection<string>>("DispatcherThread", _timeResolver, _state);
        }
    }
}