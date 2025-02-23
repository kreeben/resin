using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class Int64Writer : ByteArrayWriter<long>
    {
        public Int64Writer(Stream keyStream, Stream valueStream, Stream addressStream, long offset = 0, int pageSize = 4096)
            : base(keyStream, valueStream, addressStream, long.MaxValue, (x) => BitConverter.GetBytes(x), offset, pageSize)
        {
        }

        public Int64Writer(Stream valueStream, int pageSize = 4096)
            : base(valueStream, long.MaxValue, (x) => BitConverter.GetBytes(x), pageSize)
        {
        }
    }

    public class ByteArrayWriter<T> where T : struct, IEquatable<T>, IComparable<T>
    {
        private readonly T[] _keyBuf;
        private Address[] _addressBuf;
        private readonly int _pageSize;
        private readonly int _keyBufSize;
        private int _keyBufCursor;
        private int _keyCount;
        private Stream _valueStream;
        private readonly T _emptyKey;
        public readonly Func<T, byte[]> _getBytes;

        public bool IsPageFull { get { return _keyCount == _pageSize / sizeof(long); } }

        public ByteArrayWriter(Stream keyStream, Stream valueStream, Stream addressStream, T valueOfEmptyKey, Func<T, byte[]> getBytes, long offset = 0, int pageSize = 4096)
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

            _emptyKey = valueOfEmptyKey;
            _getBytes = getBytes;

            if (keyStream.Position != offset)
                keyStream.Position = offset;

            var kbuf = new byte[pageSize];
            keyStream.ReadExactly(kbuf);
            _keyBuf = MemoryMarshal.Cast<byte, T>(kbuf).ToArray();
            int i = 0;
            for (; i < _keyBuf.Length; i++)
            {
                if (_keyBuf[i].Equals(_emptyKey)) { break; }
            }
            _keyCount = i;

            int addressBufSize = (pageSize / sizeof(long)) * Address.Size;
            Span<byte> adrBuf = new byte[addressBufSize];
            addressStream.ReadExactly(adrBuf);
            Span<long> adrSpan = MemoryMarshal.Cast<byte, long>(adrBuf);
            var numOfAddresses = adrBuf.Length / Address.Size;
            var addressList = new Address[numOfAddresses];
            var addressListIndex = 0;
            for (i = 0; i < numOfAddresses; i += 2)
            {
                long ofs = adrSpan[i];
                long len = adrSpan[i + 1];
                addressList[addressListIndex++] = new Address(ofs, len);
            }
            _addressBuf = addressList;
            _pageSize = pageSize;
        }

        public ByteArrayWriter(Stream valueStream, T valueOfEmptyKey, Func<T, byte[]> getBytes, int pageSize = 4096)
        {
            if (pageSize % sizeof(long) > 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size modulu sizeof(long) must be equal to zero.");

            _emptyKey = valueOfEmptyKey;
            _getBytes = getBytes;
            _keyBufSize = pageSize / sizeof(long);
            _keyBuf = new T[_keyBufSize];
            _addressBuf = new Address[_keyBufSize];
            _valueStream = valueStream;
            _pageSize = pageSize;
            new Span<T>(_keyBuf).Fill(_emptyKey);
            new Span<Address>(_addressBuf).Fill(Address.Empty());
        }

        public bool TryPut(T key, ReadOnlySpan<byte> value)
        {
            if (_keyCount >= _keyBufSize)
                throw new OutOfPageStorageException();

            int index = new Span<T>(_keyBuf).BinarySearch(key);
            if (index > -1)
            {
                return false;
            }

            var address = Serialize(value);
            _keyBuf[_keyBufCursor] = key;
            _addressBuf[_keyBufCursor] = address;

            _keyCount++;
            _keyBufCursor++;

            new Span<T>(_keyBuf).Sort(new Span<Address>(_addressBuf));

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

        public T[] Serialize(Stream keyStream, Stream addressStream)
        {
            if (_keyCount == 0)
                return Array.Empty<T>();

            foreach (var key in _keyBuf)
            {
                keyStream.Write(_getBytes(key));
            }

            foreach (var adr in _addressBuf)
            {
                addressStream.Write(BitConverter.GetBytes(adr.Offset));
                addressStream.Write(BitConverter.GetBytes(adr.Length));
            }

            _keyCount = 0;
            _keyBufCursor = 0;

            var keys = new T[_keyBuf.Length];
            _keyBuf.CopyTo(keys, 0);
            new Span<T>(_keyBuf).Fill(_emptyKey);
            new Span<Address>(_addressBuf).Fill(Address.Empty());
            return keys;
        }
    }
}