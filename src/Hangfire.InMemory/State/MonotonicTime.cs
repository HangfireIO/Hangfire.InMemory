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
using System.Diagnostics;
using System.Globalization;

namespace Hangfire.InMemory.State
{
    [DebuggerDisplay("{DebuggerToString()}")]
    internal readonly struct MonotonicTime : IEquatable<MonotonicTime>, IComparable, IComparable<MonotonicTime>
    {
        private const long TicksPerMillisecond = 10000;
        private const long TicksPerSecond = TicksPerMillisecond * 1000;

        private static readonly double TickFrequency = (double)TicksPerSecond / Stopwatch.Frequency;

        private readonly long _timestamp;

        private MonotonicTime(long timestamp)
        {
            _timestamp = timestamp;
        }

        public static MonotonicTime GetCurrent()
        {
            return new MonotonicTime(Stopwatch.GetTimestamp());
        }

        public MonotonicTime Add(TimeSpan value)
        {
            return new MonotonicTime(_timestamp + unchecked((long)(value.Ticks / TickFrequency)));
        }

        public DateTime ToUtcDateTime()
        {
            return DateTime.UtcNow.Add(this - GetCurrent());
        }

        public override bool Equals(object? obj)
        {
            return obj is MonotonicTime other && Equals(other);
        }

        public bool Equals(MonotonicTime other)
        {
            return _timestamp == other._timestamp;
        }

        public override int GetHashCode()
        {
            return _timestamp.GetHashCode();
        }

        public int CompareTo(object? obj)
        {
            if (obj == null) return 1;
            if (obj is not MonotonicTime time)
            {
                throw new ArgumentException("Value must be of type " + nameof(MonotonicTime), nameof(obj));
            }
 
            return CompareTo(time);
        }

        public int CompareTo(MonotonicTime other)
        {
            return _timestamp.CompareTo(other._timestamp);
        }

        public override string ToString()
        {
            return _timestamp.ToString(CultureInfo.InvariantCulture);
        }

        public static TimeSpan operator -(MonotonicTime left, MonotonicTime right)
        {
            var elapsed = unchecked((long)((left._timestamp - right._timestamp) * TickFrequency));
            return new TimeSpan(elapsed);
        }

        public static bool operator ==(MonotonicTime left, MonotonicTime right) => left.Equals(right);
        public static bool operator !=(MonotonicTime left, MonotonicTime right) => !(left == right);
        public static bool operator <(MonotonicTime left, MonotonicTime right) => left.CompareTo(right) < 0;
        public static bool operator <=(MonotonicTime left, MonotonicTime right) => left.CompareTo(right) <= 0;
        public static bool operator >(MonotonicTime left, MonotonicTime right) => left.CompareTo(right) > 0;
        public static bool operator >=(MonotonicTime left, MonotonicTime right) => left.CompareTo(right) >= 0;

        private string DebuggerToString()
        {
            return $"DateTime: {ToUtcDateTime()}, Raw: {ToString()}";
        }
    }
}