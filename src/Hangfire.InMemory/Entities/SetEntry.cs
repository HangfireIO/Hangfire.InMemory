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
using System.Collections;
using System.Collections.Generic;
using Hangfire.InMemory.State;

namespace Hangfire.InMemory.Entities
{
    internal sealed class SetEntry : IExpirableEntry<string>, IEnumerable<SortedSetItem>
    {
        private readonly SortedDictionary<string, SortedSetItem> _hash;
        private readonly SortedSet<SortedSetItem> _value;

        public SetEntry(string id, StringComparer stringComparer)
        {
            _hash = new SortedDictionary<string, SortedSetItem>(stringComparer);
            _value = new SortedSet<SortedSetItem>(new SortedSetItemComparer(stringComparer));
            Key = id;
        }

        public string Key { get; }
        public MonotonicTime? ExpireAt { get; set; }

        public int Count => _value.Count;

        public void Add(string value, double score)
        {
            if (!_hash.TryGetValue(value, out var entry))
            {
                entry = new SortedSetItem(value, score);
                _value.Add(entry);
                _hash.Add(value, entry);
            }
            else
            {
                // Element already exists, just need to add a score value – re-create it.
                _value.Remove(entry);

                entry = new SortedSetItem(value, score);
                _value.Add(entry);
                _hash[value] = entry;
            }
        }

        public List<string> GetViewBetween(double from, double to, int count)
        {
            if (_value.Count == 0) return new List<string>();

            var result = new List<string>(count);

            if (_value.Min.Score >= from)
            {
                // Fast path - item is found, no need to traverse the tree, just iterating
                foreach (var item in _value)
                {
                    if (item.Score > to || count-- == 0) break;
                    result.Add(item.Value);
                }
            }
            else
            {
                // Slow path - find the item first
                var view = _value.GetViewBetween(
                    new SortedSetItem(null!, from),
                    new SortedSetItem(null!, to));

                // Don't query view.Count here as it leads to VersionCheck(updateCount: true) call,
                // which is very expensive when there are a huge number of entries.
                foreach (var item in view)
                {
                    if (count-- == 0) break;
                    result.Add(item.Value);
                }
            }

            return result;
        }

        public string? GetFirstBetween(double from, double to)
        {
            if (_value.Count == 0) return null;

            var minItem = _value.Min;
            if (minItem.Score >= from)
            {
                // Fast path - item is found, no need to traverse
                return minItem.Score <= to ? minItem.Value : null;
            }

            // Slow path - find the item first
            var view = _value.GetViewBetween(
                new SortedSetItem(null!, from),
                new SortedSetItem(null!, to));

            return view.Min.Value;
        }

        public void Remove(string value)
        {
            if (_hash.TryGetValue(value, out var entry))
            {
                _value.Remove(entry);
                _hash.Remove(value);
            }
        }

        public bool Contains(string value)
        {
            return _hash.ContainsKey(value);
        }

        public IEnumerator<SortedSetItem> GetEnumerator()
        {
            return _value.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}