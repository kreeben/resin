using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class PageWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly ByteArrayWriter<TKey> _writer;
        private TKey[] _allKeys;

        public PageWriter(ByteArrayWriter<TKey> writer, Stream keyStream, Stream addressStream)
        {
            _writer = writer;

            if (keyStream.Position != 0)
            {
                keyStream.Position = 0;
            }
            var kbuf = new byte[keyStream.Length];
            keyStream.ReadExactly(kbuf);
            var keys = MemoryMarshal.Cast<byte, TKey>(kbuf);
            keys.Sort();
            _allKeys = keys.ToArray();
        }

        public void Dispose()
        {
            if (_writer != null)
            {
                _writer.Serialize();
            }
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (KeyExists(key))
            {
                return false;
            }
            var put = _writer.TryPut(key, value);
            if (_writer.IsPageFull)
            {
                Serialize();
            }
            return put;
        }

        private bool KeyExists(TKey key)
        {
            int index = new Span<TKey>(_allKeys.ToArray()).BinarySearch(key);
            return index > -1;
        }

        public void Serialize()
        {
            var newKeys = _writer.Serialize();

            if (_allKeys.Length > 0)
            {
                var enlargedArray = new TKey[_allKeys.Length + newKeys.Length];
                _allKeys.CopyTo(enlargedArray, 0);
                newKeys.CopyTo(enlargedArray, _allKeys.Length);
                _allKeys = enlargedArray;
            }
            else
            {
                _allKeys = newKeys;
            }

            new Span<TKey>(_allKeys).Sort();
        }
    }
}