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

namespace Hangfire.InMemory.Entities
{
    internal sealed class ListEntry : IExpirableEntry<string>, IEnumerable<string>
    {
        private readonly LinkedList<string> _list = new LinkedList<string>();

        public ListEntry(string id)
        {
            Key = id;
        }

        public string Key { get; }
        public MonotonicTime? ExpireAt { get; set; }

        public int Count => _list.Count;

        public void Add(string value)
        {
            _list.AddFirst(value);
        }

        public int RemoveAll(string value, StringComparer comparer)
        {
            var node = _list.First;
            while (node != null)
            {
                var current = node;
                node = node.Next;

                if (comparer.Compare(current.Value, value) == 0)
                {
                    _list.Remove(current);
                }
            }

            return _list.Count;
        }

        public int Trim(int keepStartingFrom, int keepEndingAt)
        {
            var count = keepEndingAt - keepStartingFrom + 1;

            var node = _list.First;

            // Removing first items
            while (node != null && keepStartingFrom-- > 0)
            {
                var current = node;
                node = node.Next;

                _list.Remove(current);
            }

            if (node != null)
            {
                // Skipping required entries
                while (node != null && count-- > 0)
                {
                    node = node.Next;
                }

                // Removing rest items
                while (node != null)
                {
                    var current = node;
                    node = node.Next;

                    _list.Remove(current);
                }
            }

            return _list.Count;
        }

        public IEnumerator<string> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}