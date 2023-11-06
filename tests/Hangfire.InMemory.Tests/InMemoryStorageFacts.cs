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
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryStorageFacts
    {
        private readonly InMemoryStorage _storage = new InMemoryStorage();

        [Fact]
        public void LinearizableRead_Property_ReturnsTrue()
        {
            Assert.True(_storage.LinearizableReads);
        }

        [Fact]
        public void HasFeature_ThrowsArgumentNullException_WhenFeatureIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _storage.HasFeature(null));

            Assert.Equal("featureId", exception.ParamName);
        }

        [Fact]
        public void HasFeature_ReturnsTrue_ForTheFollowingFeatures()
        {
            Assert.True(_storage.HasFeature("Storage.ExtendedApi"));
            Assert.True(_storage.HasFeature("Job.Queue"));
            Assert.True(_storage.HasFeature("Connection.BatchedGetFirstByLowestScoreFromSet"));
            Assert.True(_storage.HasFeature("Connection.GetUtcDateTime"));
            Assert.True(_storage.HasFeature("Connection.GetSetContains"));
            Assert.True(_storage.HasFeature("Connection.GetSetCount.Limited"));
            Assert.True(_storage.HasFeature("Transaction.AcquireDistributedLock"));
            Assert.True(_storage.HasFeature("Transaction.CreateJob"));
            Assert.True(_storage.HasFeature("Transaction.SetJobParameter"));
            Assert.True(_storage.HasFeature("TransactionalAcknowledge:InMemoryFetchedJob"));
            Assert.True(_storage.HasFeature("Monitoring.DeletedStateGraphs"));
            Assert.True(_storage.HasFeature("Monitoring.AwaitingJobs"));
            Assert.False(_storage.HasFeature("SomeNonExistingFeature"));
        }

        [Fact]
        public void GetConnection_ReturnsUsableInstance()
        {
            using (var connection = _storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact]
        public void GetMonitoringApi_ReturnsUsableInstance()
        {
            var monitoringApi = _storage.GetMonitoringApi();
            Assert.NotNull(monitoringApi);
        }

        [Fact]
        public void ToString_ReturnsUsefulString()
        {
            var result = _storage.ToString();
            Assert.Equal("In-Memory Storage", result);
        }
    }
}