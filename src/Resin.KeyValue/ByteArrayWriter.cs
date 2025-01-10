using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class ByteArrayWriter
    {
        private readonly long[] _keyBuf;
        private Address[] _addressBuf;
        private readonly int _keyBufSize;
        private int _keyBufCursor;
        private int _keyCount;
        private Stream _valueStream;
        public const long EmptyKey = int.MaxValue;

        public ByteArrayWriter(Stream keyStream, Stream valueStream, Stream addressStream, long offset = 0, int pageSize = 4096)
        {
            if (keyStream is null)
            {
                throw new ArgumentNullException(nameof(keyStream));
            }

            if (valueStream is null)
            {
                throw new ArgumentNullException(nameof(valueStream));
            }

            if (addressStream is null)
            {
                throw new ArgumentNullException(nameof(addressStream));
            }

            if (pageSize % Address.Size > 0)
            {
                throw new ArgumentOutOfRangeException(nameof(pageSize));
            }

            if (pageSize % sizeof(long) > 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size modulu sizeof(long) must be equal to zero.");

            if (keyStream.Position != offset)
                keyStream.Position = offset;

            var kbuf = new byte[pageSize];
            keyStream.ReadExactly(kbuf);
            _keyBuf = MemoryMarshal.Cast<byte, long>(kbuf).ToArray();

            int addressBufSize = (pageSize / sizeof(long)) * Address.Size;
            Span<byte> adrBuf = new byte[addressBufSize];
            addressStream.ReadExactly(adrBuf);
            Span<long> adrSpan = MemoryMarshal.Cast<byte, long>(adrBuf);
            var numOfAddresses = adrBuf.Length / Address.Size;
            var addressList = new Address[numOfAddresses];
            var addressListIndex = 0;
            for (int i = 0; i < numOfAddresses; i += 2)
            {
                long ofs = adrSpan[i];
                long len = adrSpan[i + 1];
                addressList[addressListIndex++] = new Address(ofs, len);
            }
            _addressBuf = addressList;
        }

        public ByteArrayWriter(Stream valueStream, int pageSize = 4096)
        {
            if (pageSize % sizeof(long) > 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size modulu sizeof(long) must be equal to zero.");

            _keyBufSize = pageSize / sizeof(long);
            _keyBuf = new long[_keyBufSize];
            _addressBuf = new Address[_keyBufSize];
            _valueStream = valueStream;

            new Span<long>(_keyBuf).Fill(EmptyKey);
            new Span<Address>(_addressBuf).Fill(Address.Empty());
        }

        public bool TryPut(long key, ReadOnlySpan<byte> value)
        {
            if (_keyCount >= _keyBufSize)
                throw new OutOfPageStorageException();

            int index = new Span<long>(_keyBuf).BinarySearch(key);
            if (index > -1)
            {
                return false;
            }

            var address = Serialize(value);
            _keyBuf[_keyBufCursor] = key;
            _addressBuf[_keyBufCursor] = address;

            _keyCount++;
            _keyBufCursor++;

            new Span<long>(_keyBuf).Sort(new Span<Address>(_addressBuf));

            return true;
        }

        private Address Serialize(ReadOnlySpan<byte> value)
        {
            if (value == Span<byte>.Empty || value.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(value.Length), "Value cannot be null or empty.");

            var pos = _valueStream.Position;
            _valueStream.Write(value);
            return new Address(pos, value.Length);
        }

        public void Serialize(Stream keyStream, Stream addressStream)
        {
            if (_keyCount == 0)
                return;

            _valueStream.Flush();

            foreach (var key in _keyBuf)
            {
                //if (key == EmptyKey)
                //    break;

                keyStream.Write(BitConverter.GetBytes(key));
            }

            foreach (var adr in _addressBuf)
            {
                //if (adr.Equals(Address.Empty()))
                //    break;

                addressStream.Write(BitConverter.GetBytes(adr.Offset));
                addressStream.Write(BitConverter.GetBytes(adr.Length));
            }

            keyStream.Flush();
            addressStream.Flush();

            _keyCount = 0;
            _keyBufCursor = 0;
            new Span<long>(_keyBuf).Fill(EmptyKey);
            new Span<Address>(_addressBuf).Fill(Address.Empty());
        }
    }
}
