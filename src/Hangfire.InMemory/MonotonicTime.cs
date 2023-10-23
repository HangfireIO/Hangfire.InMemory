using System;
using System.Diagnostics;

namespace Hangfire.InMemory
{
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

        public override bool Equals(object obj)
        {
            return obj is MonotonicTime other && Equals(other);
        }

        public override int GetHashCode()
        {
            return _timestamp.GetHashCode();
        }

        public int CompareTo(object other)
        {
            if (other == null) return 1;
            if (other is not MonotonicTime time)
            {
                throw new ArgumentException("Value must be of type " + nameof(MonotonicTime), nameof(other));
            }
 
            return CompareTo(time);
        }

        public bool Equals(MonotonicTime other)
        {
            return _timestamp == other._timestamp;
        }

        public int CompareTo(MonotonicTime other)
        {
            return _timestamp.CompareTo(other._timestamp);
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
    }
}