using System.Runtime.InteropServices;
namespace Resin.KeyValue
{
    public class PageReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly TKey[] _keyBuf;
        private readonly ReadOnlyMemory<Address> _addresses;
        private readonly Stream _valueStream;

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

            int addressBufSize = (pageSize / sizeOfT) * Address.Size;
            Span<byte> addressBuf = new byte[addressBufSize];
            readSession.AddressStream.ReadExactly(addressBuf);
            var addresses = MemoryMarshal.Cast<byte, Address>(addressBuf);
            _addresses = addresses.ToArray().AsMemory();

            _valueStream = readSession.ValueStream;
        }

        public ReadOnlySpan<byte> Get(TKey key)
        {
            int index = new Span<TKey>(_keyBuf).BinarySearch(key);
            if (index > -1)
            {
                var address = _addresses.Span[index];
                _valueStream.Position = address.Offset;
                var valueBuf = new byte[address.Length].AsSpan();
                _valueStream.ReadExactly(valueBuf);
                return valueBuf;
            }
            return ReadOnlySpan<byte>.Empty;
        }

        public int IndexOf(TKey key)
        {
            return new Span<TKey>(_keyBuf).BinarySearch(key);
        }
    }
}