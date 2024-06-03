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

// ReSharper disable AssignNullToNotNullAttribute
// ReSharper disable PossibleNullReferenceException

namespace Hangfire.InMemory.Tests
{
    public class InMemoryStorageFacts
    {
        [Fact]
        public void LinearizableRead_Property_ReturnsTrue()
        {
            // Arrange
            using var storage = CreateStorage();

            // Act & Assert
            Assert.True(storage.LinearizableReads);
        }

        [Fact]
        public void HasFeature_ThrowsArgumentNullException_WhenFeatureIdIsNull()
        {
            // Arrange
            using var storage = CreateStorage();

            // Act
            var exception = Assert.Throws<ArgumentNullException>(
                () => storage.HasFeature(null));

            // Assert
            Assert.Equal("featureId", exception.ParamName);
        }

        [Fact]
        public void HasFeature_ReturnsTrue_ForTheFollowingFeatures()
        {
            // Arrange
            using var storage = CreateStorage();

            // Act & Assert
            Assert.True(storage.HasFeature("Storage.ExtendedApi"));
            Assert.True(storage.HasFeature("Job.Queue"));
            Assert.True(storage.HasFeature("Connection.BatchedGetFirstByLowestScoreFromSet"));
            Assert.True(storage.HasFeature("Connection.GetUtcDateTime"));
            Assert.True(storage.HasFeature("Connection.GetSetContains"));
            Assert.True(storage.HasFeature("Connection.GetSetCount.Limited"));
            Assert.True(storage.HasFeature("Transaction.AcquireDistributedLock"));
            Assert.True(storage.HasFeature("Transaction.CreateJob"));
            Assert.True(storage.HasFeature("Transaction.SetJobParameter"));
            Assert.True(storage.HasFeature("TransactionalAcknowledge:InMemoryFetchedJob"));
            Assert.True(storage.HasFeature("Monitoring.DeletedStateGraphs"));
            Assert.True(storage.HasFeature("Monitoring.AwaitingJobs"));
            Assert.False(storage.HasFeature("SomeNonExistingFeature"));
        }

        [Fact]
        public void GetConnection_ReturnsUsableInstance()
        {
            // Arrange
            using var storage = CreateStorage();

            // Act
            using var connection = storage.GetConnection();

            // Assert
            Assert.NotNull(connection);
        }

        [Fact]
        public void GetMonitoringApi_ReturnsUsableInstance()
        {
            // Arrange
            using var storage = CreateStorage();

            // Act
            var monitoringApi = storage.GetMonitoringApi();

            // Assert
            Assert.NotNull(monitoringApi);
        }

        [Fact]
        public void ToString_ReturnsUsefulString()
        {
            // Arrange
            using var storage = CreateStorage();

            // Act
            var result = storage.ToString();

            // Assert
            Assert.Equal("In-Memory Storage", result);
        }

        private static InMemoryStorage CreateStorage()
        {
            return new InMemoryStorage();
        }
    }
}