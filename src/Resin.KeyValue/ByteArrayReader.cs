using System.Runtime.InteropServices;
namespace Resin.KeyValue
{
    public class ByteArrayReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly TKey[] _keyBuf;
        private readonly ReadOnlyMemory<Address> _addresses;
        private readonly Stream _valueStream;

        public ByteArrayReader(Stream keyStream, Stream valueStream, Stream addressStream, int sizeOfTInBytes, int pageSize)
        {
            if (keyStream is null)
            {
                throw new ArgumentNullException(nameof(keyStream));
            }

            if (addressStream is null)
            {
                throw new ArgumentNullException(nameof(addressStream));
            }

            if (valueStream is null)
            {
                throw new ArgumentNullException(nameof(valueStream));
            }

            Span<byte> keyBuf = new byte[pageSize];
            keyStream.ReadExactly(keyBuf);
            var keys = MemoryMarshal.Cast<byte, TKey>(keyBuf);
            _keyBuf = keys.ToArray();

            int addressBufSize = (pageSize / sizeOfTInBytes) * Address.Size;
            Span<byte> addressBuf = new byte[addressBufSize];
            addressStream.ReadExactly(addressBuf);
            var addresses = MemoryMarshal.Cast<byte, Address>(addressBuf);
            _addresses = addresses.ToArray().AsMemory();

            _valueStream = valueStream;
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