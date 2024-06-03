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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Hangfire.InMemory.State;
using Xunit;

namespace Hangfire.InMemory.Tests
{
    public class MonotonicTimeFacts
    {
        [Fact]
        public void GetCurrent_ReturnsBiggerValues_ForSubsequentCalls()
        {
            var time1 = MonotonicTime.GetCurrent();
            Thread.Sleep(10);
            var time2 = MonotonicTime.GetCurrent();
            Thread.Sleep(10);
            var time3 = MonotonicTime.GetCurrent();

            Assert.True(time1 < time2 && time2 < time3);
        }

        [Fact]
        public void ToUtcDateTime_ReturnsCorrectValue()
        {
            var utcTime = MonotonicTime.GetCurrent().ToUtcDateTime();
            var utcNow = DateTime.UtcNow;

            AssertWithinSecond(utcTime, utcNow);
        }

        [Fact]
        public void Add_CorrectlyAdds_TheGivenPositiveTimeSpan()
        {
            var utcTime = MonotonicTime.GetCurrent().ToUtcDateTime().Add(TimeSpan.FromDays(1));
            var utcNow = DateTime.UtcNow.AddDays(1);

            AssertWithinSecond(utcTime, utcNow);
        }
        
        [Fact]
        public void Add_CorrectlyAdds_TheGivenNegativeTimeSpan()
        {
            var utcTime = MonotonicTime.GetCurrent().ToUtcDateTime().Add(TimeSpan.FromMinutes(-1));
            var utcNow = DateTime.UtcNow.AddMinutes(-1);

            AssertWithinSecond(utcTime, utcNow);
        }

        [Fact]
        public void Add_CanHandle_BigPositiveValues()
        {
            var utcTime = MonotonicTime.GetCurrent().ToUtcDateTime().Add(TimeSpan.FromDays(10000));
            var utcNow = DateTime.UtcNow.AddDays(10000);

            AssertWithinSecond(utcTime, utcNow);
        }

        [Fact]
        public void Add_CanHandle_BigNegativeValues()
        {
            var utcTime = MonotonicTime.GetCurrent().ToUtcDateTime().Add(TimeSpan.FromDays(-10000));
            var utcNow = DateTime.UtcNow.AddDays(-10000);

            AssertWithinSecond(utcTime, utcNow);
        }

        [Fact]
        public void Equals_ReturnsTrue_OnEqualValues()
        {
            var time1 = MonotonicTime.GetCurrent();
            var time2 = time1;

            Assert.Equal(time1, time2);
            Assert.True(time1.Equals(time2));
            Assert.True(time1.Equals((object)time2));
            Assert.True(time1 == time2);
            Assert.False(time1 != time2);
        }

        [Fact]
        public void Equals_ReturnsTrue_OnDifferentValues()
        {
            var time1 = MonotonicTime.GetCurrent();
            var time2 = time1.Add(TimeSpan.FromSeconds(1));

            Assert.NotEqual(time1, time2);
            Assert.False(time1.Equals(time2));
            Assert.False(time1.Equals((object)time2));
            Assert.False(time1 == time2);
            Assert.True(time1 != time2);
        }

        [Fact]
        public void Equals_Object_ReturnsFalse_WhenNullValueIsGiven()
        {
            var time = MonotonicTime.GetCurrent();
            Assert.False(time.Equals(null));
        }

        [Fact]
        public void Equals_Object_ReturnsFalse_ForOtherTypes()
        {
            var time = MonotonicTime.GetCurrent();
            // ReSharper disable SuspiciousTypeConversion.Global
            Assert.False(time.Equals(12345));
            // ReSharper restore SuspiciousTypeConversion.Global
        }

        [Fact]
        public void GetHashCode_ReturnsEqualValues_ForEqualInstances()
        {
            var time = MonotonicTime.GetCurrent();
            Assert.Equal(time.GetHashCode(), time.GetHashCode());
        }

        [Fact]
        public void GetHashCode_ReturnsDifferentValues_ForDifferentInstances()
        {
            var time1 = MonotonicTime.GetCurrent();
            var time2 = time1.Add(TimeSpan.FromSeconds(1));

            Assert.NotEqual(time1.GetHashCode(), time2.GetHashCode());
        }

        [Fact]
        public void CompareTo_ReturnsZero_OnEqualComparisons()
        {
            var time1 = MonotonicTime.GetCurrent();
            var time2 = time1;

            Assert.Equal(0, time1.CompareTo(time2));
            Assert.Equal(0, time1.CompareTo((object)time2));
            Assert.True(time1 >= time2);
            Assert.True(time1 <= time2);
            Assert.False(time1 < time2);
            Assert.False(time1 > time2);
        }

        [Fact]
        public void CompareTo_ReturnsOne_WhenValueIsBigger_ThanTheGivenOne()
        {
            var time1 = MonotonicTime.GetCurrent();
            var time2 = time1.Add(TimeSpan.FromSeconds(-1));

            Assert.Equal(1, time1.CompareTo(time2));
            Assert.Equal(1, time1.CompareTo((object)time2));
            Assert.True(time1 >= time2);
            Assert.True(time1 > time2);
            Assert.False(time1 <= time2);
            Assert.False(time1 < time2);
        }

        [Fact]
        public void CompareTo_ReturnsMinusOne_WhenValueIsSmaller_ThanTheGivenOne()
        {
            var time1 = MonotonicTime.GetCurrent();
            var time2 = time1.Add(TimeSpan.FromSeconds(1));

            Assert.Equal(-1, time1.CompareTo(time2));
            Assert.Equal(-1, time1.CompareTo((object)time2));
            Assert.False(time1 >= time2);
            Assert.False(time1 > time2);
            Assert.True(time1 <= time2);
            Assert.True(time1 < time2);
        }

        [Fact]
        public void CompareTo_Object_ReturnsOne_WhenNullValueIsGiven()
        {
            var time = MonotonicTime.GetCurrent();
            Assert.Equal(1, time.CompareTo(null));
        }

        [Fact]
        public void CompareTo_Object_ThrowsAnException_ForOtherTypes()
        {
            var time = MonotonicTime.GetCurrent();
            var exception = Assert.Throws<ArgumentException>(
                () => time.CompareTo(1234));
            
            Assert.Equal("other", exception.ParamName);
        }

        [Fact]
        public void ToString_ReturnsSomeValue()
        {
            var result = MonotonicTime.GetCurrent().ToString();
            Assert.NotNull(result);
        }

        [Fact]
        [SuppressMessage("SonarLint", "S1764:IdenticalExpressionsShouldNotBeUsedOnBothSidesOfOperators", Justification = "Checking the correct behavior.")]
        public void op_Subtraction_ReturnsZeroTimeSpan_ForEqualValues()
        {
            var time = MonotonicTime.GetCurrent();
            Assert.Equal(TimeSpan.Zero, time - time);
        }

        [Fact]
        public void op_Subtraction_ReturnsCorrectPositiveTimeSpan_WhenSubtractingFromBiggerValue()
        {
            var span = TimeSpan.FromDays(1);
            var time = MonotonicTime.GetCurrent();
            var nextDay = time.Add(span);

            Assert.Equal(span, nextDay - time);
        }

        [Fact]
        public void op_Subtraction_ReturnsCorrectNegativeTimeSpan_WhenSubtractingFromLowerValue()
        {
            var span = TimeSpan.FromHours(1);
            var time = MonotonicTime.GetCurrent();
            var nextHour = time.Add(span);

            Assert.Equal(span.Negate(), time - nextHour);
        }

        private static void AssertWithinSecond(DateTime date1, DateTime date2)
        {
            Assert.Equal(0, (date1 - date2).TotalSeconds, 1);
        }
    }
}