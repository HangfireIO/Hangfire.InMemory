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

namespace Hangfire.InMemory.Entities
{
    internal readonly struct SortedSetItem : IEquatable<SortedSetItem>
    {
        public SortedSetItem(string value, double score)
        {
            Value = value;
            Score = score;
        }

        public string Value { get; }
        public double Score { get; }

        public bool Equals(SortedSetItem other)
        {
            return Value == other.Value && Score.Equals(other.Score);
        }

        public override bool Equals(object? obj)
        {
            return obj is SortedSetItem other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Value != null ? Value.GetHashCode() : 0) * 397) ^ Score.GetHashCode();
            }
        }

        public override string ToString()
        {
            return $"Value: {Value}, Score: {Score}]";
        }
    }
}