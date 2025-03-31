using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class ByteArrayWriter<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private TKey[] _keyBuffer;
        private Address[] _addressBuffer;
        private int _keyCount;
        private Stream _keyStream;
        private Stream _valueStream;
        private Stream _addressStream;
        private readonly TKey _emptyKey;
        private readonly int _pageSizeInBytes;
        private readonly int _keyBufSize;
        public readonly Func<TKey, byte[]> _getBytes;
        private readonly int _sizeOfTInBytes;

        public bool IsPageFull { get { return _keyCount == _pageSizeInBytes / _sizeOfTInBytes; } }
        public Stream KeyStream => _keyStream;
        public Stream AddressStream => _addressStream;

        public ByteArrayWriter(Stream keyStream, Stream valueStream, Stream addressStream, TKey maxValueOfKey, Func<TKey, byte[]> getBytes, int sizeOfTInBytes, int pageSizeInBytes)
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

            if (pageSizeInBytes % Address.Size > 0)
            {
                throw new ArgumentOutOfRangeException(nameof(pageSizeInBytes), $"Page size modulu {Address.Size} (Address.Size) must equal zero.");
            }

            if (pageSizeInBytes % sizeOfTInBytes > 0)
                throw new ArgumentOutOfRangeException(nameof(pageSizeInBytes), $"Page size modulu size of T must equal zero.");

            _emptyKey = maxValueOfKey;
            _getBytes = getBytes ?? throw new ArgumentNullException(nameof(getBytes));
            _sizeOfTInBytes = sizeOfTInBytes;
            _keyBufSize = pageSizeInBytes / _sizeOfTInBytes;
            _pageSizeInBytes = pageSizeInBytes;
            _valueStream = valueStream;
            _keyStream = keyStream;
            _addressStream = addressStream;

            if (keyStream.Length > 0)
            {
                var keyInfo = ReadKeyPage(keyStream);
                _keyBuffer = keyInfo.keyBuffer;
                _keyCount = keyInfo.keyCount;

                _addressBuffer = ReadAddressPage(addressStream);
            }
            else
            {
                _keyBuffer = new TKey[_pageSizeInBytes / _sizeOfTInBytes];
                new Span<TKey>(_keyBuffer).Fill(_emptyKey);

                _addressBuffer = new Address[_pageSizeInBytes / _sizeOfTInBytes];
                new Span<Address>(_addressBuffer).Fill(Address.Empty());
            }
        }

        //public void Refresh()
        //{
        //    var keyInfo = ReadKeyPage(_keyStream);
        //    _keyBuffer = keyInfo.keyBuffer;
        //    _keyCount = keyInfo.keyCount;
        //    _addressBuffer = ReadAddressPage(_addressStream);
        //}

        private Address[] ReadAddressPage(Stream addressStream)
        {
            if (addressStream.Length == 0)
            {
                throw new InvalidOperationException("Address stream is empty.");
            }
            int addressBufSize = (_pageSizeInBytes / _sizeOfTInBytes) * Address.Size;
            Span<byte> adrBuf = new byte[addressBufSize];
            addressStream.ReadExactly(adrBuf);
            Span<long> adrSpan = MemoryMarshal.Cast<byte, long>(adrBuf);
            var numOfAddresses = adrBuf.Length / Address.Size;
            var addressList = new Address[numOfAddresses];
            var addressListIndex = 0;
            for (int i = _keyCount; i < numOfAddresses; i += 2)
            {
                long ofs = adrSpan[i];
                long len = adrSpan[i + 1];
                addressList[addressListIndex++] = new Address(ofs, len);
            }
            return addressList;
        }

        private (TKey[] keyBuffer, int keyCount) ReadKeyPage(Stream keyStream)
        {
            if (keyStream.Length == 0)
            {
                throw new InvalidOperationException("Key stream stream is empty.");
            }
            int i = 0;
            var kbuf = new byte[_pageSizeInBytes];
            keyStream.ReadExactly(kbuf);
            var keyBuffer = MemoryMarshal.Cast<byte, TKey>(kbuf).ToArray();
            for (; i < keyBuffer.Length; i++)
            {
                if (keyBuffer[i].Equals(_emptyKey)) { break; }
            }
            var keyCount = i;
            return (keyBuffer, keyCount);
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (_keyCount >= _keyBufSize)
                throw new OutOfPageStorageException();

            int index = new Span<TKey>(_keyBuffer).BinarySearch(key);
            if (index >= 0)
            {
                return false;
            }

            var address = Serialize(value);
            _keyBuffer[_keyCount] = key;
            _addressBuffer[_keyCount] = address;

            _keyCount++;

            new Span<TKey>(_keyBuffer).Sort(new Span<Address>(_addressBuffer));

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

        public void Serialize()
        {
            if (_keyCount == 0)
                return;

            if (_keyStream.Position != _keyStream.Length)
                _keyStream.Position = _keyStream.Length;

            foreach (var key in _keyBuffer)
            {
                _keyStream.Write(_getBytes(key));
            }

            if (_addressStream.Position != _addressStream.Length)
                _addressStream.Position = _addressStream.Length;

            foreach (var adr in _addressBuffer)
            {
                _addressStream.Write(BitConverter.GetBytes(adr.Offset));
                _addressStream.Write(BitConverter.GetBytes(adr.Length));
            }

            _keyCount = 0;

            new Span<TKey>(_keyBuffer).Fill(_emptyKey);
            new Span<Address>(_addressBuffer).Fill(Address.Empty());
        }
    }
}