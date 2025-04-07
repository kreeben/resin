using System.Runtime.InteropServices;
namespace Resin.KeyValue
{
    public class PageReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly Memory<TKey> _keyBuf;
        private readonly Stream _valueStream;
        private readonly Stream _addressStream;
        private readonly long _addressOffset;

        public PageReader(ReadSession readSession, int sizeOfT, int pageSize)
        {
            if (readSession is null)
            {
                throw new ArgumentNullException(nameof(readSession));
            }

            Span<byte> keyBuf = new byte[pageSize];
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
                var address = GetAddress(index);
                _valueStream.Position = address.Offset;
                var valueBuf = new byte[address.Length].AsSpan();
                _valueStream.ReadExactly(valueBuf);
                return valueBuf;
            }
            return ReadOnlySpan<byte>.Empty;
        }

        public int IndexOf(TKey key)
        {
            return _keyBuf.Span.BinarySearch(key);
        }

        public Address GetAddress(int index)
        {
            var relPos = index * Address.Size;
            var absPos = relPos + _addressOffset;
            _addressStream.Position = absPos;
            var buf = new byte[Address.Size];
            _addressStream.ReadExactly(buf);
            return Address.Deserialize(buf);
        }
    }
}