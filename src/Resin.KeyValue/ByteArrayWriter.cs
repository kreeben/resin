using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class ByteArrayWriter<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly TKey[] _keyBuf;
        private Address[] _addressBuf;
        private readonly int _pageSize;
        private readonly int _keyBufSize;
        private int _keyBufCursor;
        private int _keyCount;
        private Stream _valueStream;
        private readonly TKey _emptyKey;
        public readonly Func<TKey, byte[]> _getBytes;
        private readonly int _sizeOfT;

        public bool IsPageFull { get { return _keyCount == _pageSize / _sizeOfT; } }

        public ByteArrayWriter(Stream keyStream, Stream valueStream, Stream addressStream, TKey maxValueOfKey, Func<TKey, byte[]> getBytes, int sizeOfTInBytes, int pageSize)
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

            if (pageSize % sizeOfTInBytes > 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size modulu size of T must equal zero.");

            _emptyKey = maxValueOfKey;
            _getBytes = getBytes;
            _sizeOfT = sizeOfTInBytes;

            var kbuf = new byte[pageSize];
            keyStream.ReadExactly(kbuf);
            _keyBuf = MemoryMarshal.Cast<byte, TKey>(kbuf).ToArray();
            int i = 0;
            for (; i < _keyBuf.Length; i++)
            {
                if (_keyBuf[i].Equals(_emptyKey)) { break; }
            }
            _keyCount = i;

            int addressBufSize = (pageSize / sizeOfTInBytes) * Address.Size;
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

        public ByteArrayWriter(Stream valueStream, TKey maxValueOfKey, Func<TKey, byte[]> getBytes, int sizeOfT, int pageSize)
        {
            if (pageSize % sizeOfT > 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size modulu size of T must equal zero.");

            _emptyKey = maxValueOfKey;
            _getBytes = getBytes;
            _sizeOfT = sizeOfT;
            _keyBufSize = pageSize / _sizeOfT;
            _keyBuf = new TKey[_keyBufSize];
            _addressBuf = new Address[_keyBufSize];
            _valueStream = valueStream;
            _pageSize = pageSize;
            new Span<TKey>(_keyBuf).Fill(_emptyKey);
            new Span<Address>(_addressBuf).Fill(Address.Empty());
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (_keyCount >= _keyBufSize)
                throw new OutOfPageStorageException();

            int index = new Span<TKey>(_keyBuf).BinarySearch(key);
            if (index > -1)
            {
                return false;
            }

            var address = Serialize(value);
            _keyBuf[_keyBufCursor] = key;
            _addressBuf[_keyBufCursor] = address;

            _keyCount++;
            _keyBufCursor++;

            new Span<TKey>(_keyBuf).Sort(new Span<Address>(_addressBuf));

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

        public TKey[] Serialize(Stream keyStream, Stream addressStream)
        {
            if (_keyCount == 0)
                return Array.Empty<TKey>();

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

            var keys = new TKey[_keyBuf.Length];
            _keyBuf.CopyTo(keys, 0);
            new Span<TKey>(_keyBuf).Fill(_emptyKey);
            new Span<Address>(_addressBuf).Fill(Address.Empty());
            return keys;
        }
    }
}