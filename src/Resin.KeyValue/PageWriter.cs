using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class PageWriter : IDisposable
    {
        private readonly ByteArrayWriter _writer;
        private readonly Stream _keyStream;
        private readonly Stream _addressStream;
        private long[] _allKeys;

        public PageWriter(ByteArrayWriter writer, Stream keyStream, Stream addressStream)
        {
            _writer = writer;
            _keyStream = keyStream;
            _addressStream = addressStream;

            if (keyStream.Position != 0)
            {
                keyStream.Position = 0;
            }
            var kbuf = new byte[keyStream.Length];
            keyStream.ReadExactly(kbuf);
            var keys = MemoryMarshal.Cast<byte, long>(kbuf);
            keys.Sort();
            _allKeys = keys.ToArray();
        }

        public void Dispose()
        {
            if (_writer != null)
            {
                _writer.Serialize(_keyStream, _addressStream);
            }
        }

        public bool TryPut(long key, ReadOnlySpan<byte> value)
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

        private bool KeyExists(long key)
        {
            int index = new Span<long>(_allKeys.ToArray()).BinarySearch(key);
            return index > -1;
        }

        public void Serialize()
        {
            var newKeys = _writer.Serialize(_keyStream, _addressStream);

            var enlargedArray = new long[_allKeys.Length + newKeys.Length];
            _allKeys.CopyTo(enlargedArray, 0);
            newKeys.CopyTo(enlargedArray, _allKeys.Length);
            _allKeys = enlargedArray;
            new Span<long>(_allKeys).Sort();
        }
    }

    public interface ITokenizer
    {
        IEnumerable<ReadOnlySpan<byte>> GetTokens();
    }
}