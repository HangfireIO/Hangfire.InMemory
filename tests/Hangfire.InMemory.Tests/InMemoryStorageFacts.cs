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
            Assert.False(_storage.HasFeature("SomeNonExistingFeature"));
        }
    }
}