using System;

namespace Hangfire.InMemory.Entities
{
    internal readonly struct SortedSetEntry : IEquatable<SortedSetEntry>
    {
        public SortedSetEntry(string value, double score)
        {
            Value = value;
            Score = score;
        }

        public string Value { get; }
        public double Score { get; }

        public bool Equals(SortedSetEntry other)
        {
            return Value == other.Value && Score.Equals(other.Score);
        }

        public override bool Equals(object obj)
        {
            return obj is SortedSetEntry other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Value != null ? Value.GetHashCode() : 0) * 397) ^ Score.GetHashCode();
            }
        }
    }
}