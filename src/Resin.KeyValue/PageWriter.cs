using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class PageWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly ArrayWriter<TKey> _writer;
        private TKey[] _allKeys;

        public PageWriter(ArrayWriter<TKey> writer)
        {
            _writer = writer;
            _allKeys = ReadAllKeysInColumn();
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (KeyExists(key))
            {
                return false;
            }

            var put = _writer.TryPut(key, value);
            if (put && _writer.IsPageFull)
            {
                Serialize();
            }
            return put;
        }

        private bool KeyExists(TKey key)
        {
            var ix = new Span<TKey>(_allKeys);
            ix.Sort();
            int index = ix.BinarySearch(key);
            return index > -1;
        }

        private TKey[] ReadAllKeysInColumn()
        {
            var originalPos = _writer.KeyStream.Position;
            _writer.KeyStream.Position = 0;
            var kbuf = new byte[_writer.KeyStream.Length];
            _writer.KeyStream.ReadExactly(kbuf);
            _writer.KeyStream.Position = originalPos;
            var keys = MemoryMarshal.Cast<byte, TKey>(kbuf);
            keys.Sort();
            return keys.ToArray();
        }

        public void Serialize()
        {
            _writer.Serialize();
            _allKeys = ReadAllKeysInColumn();
        }

        public void Dispose()
        {
            if (_writer != null)
            {
                Serialize();
            }
        }
    }
}