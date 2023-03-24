using System;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class InMemoryStorageFacts
    {
        private readonly InMemoryStorage _storage;

        public InMemoryStorageFacts()
        {
            _storage = new InMemoryStorage();
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
            Assert.True(_storage.HasFeature("Monitoring.DeletedStateGraphs"));
            Assert.True(_storage.HasFeature("Monitoring.AwaitingJobs"));
            Assert.False(_storage.HasFeature("SomeNonExistingFeature"));
        }
    }
}