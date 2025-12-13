using System.Runtime.InteropServices;
namespace Resin.KeyValue
{
    public class PageReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly Memory<TKey> _keyBuf;
        private readonly Stream _valueStream;
        private readonly Stream _addressStream;
        private readonly long _addressOffset;

        public PageReader(ReadSession readSession, int sizeOfT)
        {
            if (readSession is null)
            {
                throw new ArgumentNullException(nameof(readSession));
            }

            Span<byte> keyBuf = new byte[readSession.PageSize];
            readSession.KeyStream.ReadExactly(keyBuf);
            var keys = MemoryMarshal.Cast<byte, TKey>(keyBuf);
            _keyBuf = keys.ToArray();

            _valueStream = readSession.ValueStream;
            _addressStream = readSession.AddressStream;
            _addressOffset = readSession.AddressStream.Position;
        }

        public ReadOnlySpan<byte> Get(TKey key)
        {
            int index = _keyBuf.Span.BinarySearch(key);
            if (index > -1)
            {
                var address = ReadOperations.GetAddress(_addressStream, index, _addressOffset);
                return ReadOperations.ReadValue(_valueStream, address);
            }
            return ReadOnlySpan<byte>.Empty;
        }

        public int IndexOf(TKey key)
        {
            return _keyBuf.Span.BinarySearch(key);
        }
    }
}