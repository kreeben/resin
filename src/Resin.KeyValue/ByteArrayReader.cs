using System.Runtime.InteropServices;
namespace Resin.KeyValue
{
    public class ByteArrayReader
    {
        private readonly long[] _keyBuf;
        private readonly ReadOnlyMemory<Address> _addresses;
        private readonly Stream _valueStream;

        public ByteArrayReader(Stream keyStream, Stream valueStream, Stream addressStream, long offset = 0, int pageSize = 4096)
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

            if (keyStream.Position != offset)
                keyStream.Position = offset;

            int addressBufSize = (pageSize / sizeof(long)) * Address.Size;

            if (addressStream.Position != addressBufSize)
                addressStream.Position = offset;

            Span<byte> keyBuf = new byte[pageSize];

            keyStream.ReadExactly(keyBuf);

            var keys = MemoryMarshal.Cast<byte, long>(keyBuf);
            _keyBuf = keys.ToArray();

            Span<byte> addressBuf = new byte[addressBufSize];
            addressStream.ReadExactly(addressBuf);
            var addresses = MemoryMarshal.Cast<byte, Address>(addressBuf);
            _addresses = addresses.ToArray().AsMemory();

            _valueStream = valueStream;
        }

        public ReadOnlySpan<byte> Get(long key)
        {
            int index = new Span<long>(_keyBuf).BinarySearch(key);
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
    }
}
