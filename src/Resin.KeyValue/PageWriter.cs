using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class PageWriter<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
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
        private readonly int _sizeOfT;

        public bool IsPageFull { get { return _keyCount == _pageSizeInBytes / _sizeOfT; } }
        public Stream KeyStream => _keyStream;
        public Stream AddressStream => _addressStream;

        public PageWriter(WriteTransaction writeTransaction, TKey maxValueOfKey, Func<TKey, byte[]> getBytes, int sizeOfT, int pageSize)
        {
            if (writeTransaction is null)
            {
                throw new ArgumentNullException(nameof(writeTransaction));
            }

            if (pageSize % Address.Size > 0)
            {
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size modulu {Address.Size} (Address.Size) must equal zero.");
            }

            if (pageSize % sizeOfT > 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size modulu size of T must equal zero.");

            _emptyKey = maxValueOfKey;
            _getBytes = getBytes ?? throw new ArgumentNullException(nameof(getBytes));
            _sizeOfT = sizeOfT;
            _keyBufSize = pageSize / _sizeOfT;
            _pageSizeInBytes = pageSize;
            _valueStream = writeTransaction.ValueStream;
            _keyStream = writeTransaction.KeyStream;
            _addressStream = writeTransaction.AddressStream;

            if (_keyStream.Length > 0)
            {
                var keyInfo = ReadKeyPage(_keyStream);
                _keyBuffer = keyInfo.keyBuffer;
                _keyCount = keyInfo.keyCount;

                _addressBuffer = ReadAddressPage(_addressStream);
            }
            else
            {
                _keyBuffer = new TKey[_pageSizeInBytes / _sizeOfT];
                new Span<TKey>(_keyBuffer).Fill(_emptyKey);

                _addressBuffer = new Address[_pageSizeInBytes / _sizeOfT];
                new Span<Address>(_addressBuffer).Fill(Address.Empty());
            }
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (_keyCount >= _keyBufSize)
                throw new OutOfPageStorageException();

            int index = new Span<TKey>(_keyBuffer).BinarySearch(key);
            if (index >= 0)
            {
                // there is already such a key
                return false;
            }

            // put value
            var address = Serialize(value);

            // save key and address in memory and increment key count
            _keyBuffer[_keyCount] = key;
            _addressBuffer[_keyCount] = address;
            _keyCount++;

            // sort in memory keys and addresses
            new Span<TKey>(_keyBuffer).Sort(new Span<Address>(_addressBuffer));

            return true;
        }

        public void PutOrAppend(TKey key, ReadOnlySpan<byte> value)
        {
            if (_keyCount >= _keyBufSize)
                throw new OutOfPageStorageException();

            Address address;
            int index = new Span<TKey>(_keyBuffer).BinarySearch(key);

            if (index >= 0)
            {
                // there is already such a key

                // get address of existing key
                address = _addressBuffer[index];

                // copy existing value into buffer
                var buf = new byte[address.Length + value.Length];
                _valueStream.Position = address.Offset;
                var read = _valueStream.Read(buf, 0, (int)address.Length);
                if (read != address.Length)
                    throw new InvalidDataException();

                // copy new value into buffer
                Buffer.BlockCopy(value.ToArray(), 0, buf, (int)address.Length, value.Length);

                // put buffer
                address = Serialize(buf);
            }
            else
            {
                // this is a new key

                // put value
                address = Serialize(value);

                // save key and address in memory and increment key count
                _keyBuffer[_keyCount] = key;
                _addressBuffer[_keyCount] = address;
                _keyCount++;

                // sort in memory keys and addresses
                new Span<TKey>(_keyBuffer).Sort(new Span<Address>(_addressBuffer));
            }
        }

        private Address[] ReadAddressPage(Stream addressStream)
        {
            if (addressStream.Length == 0)
            {
                throw new InvalidOperationException("Address stream is empty.");
            }
            int addressBufSize = (_pageSizeInBytes / _sizeOfT) * Address.Size;
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

        private Address Serialize(ReadOnlySpan<byte> value)
        {
            if (value == Span<byte>.Empty || value.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(value.Length), "Value cannot be null or empty.");

            GoToEndOfStream(_valueStream);

            var pos = _valueStream.Position;
            _valueStream.Write(value);
            return new Address(pos, value.Length);
        }

        public void Serialize()
        {
            if (_keyCount == 0)
                return;

            GoToEndOfStream(_keyStream);

            foreach (var key in _keyBuffer)
            {
                _keyStream.Write(_getBytes(key));
            }

            GoToEndOfStream(_addressStream);

            Address.SerializeMany(_addressStream, _addressBuffer);

            _keyCount = 0;

            new Span<TKey>(_keyBuffer).Fill(_emptyKey);
            new Span<Address>(_addressBuffer).Fill(Address.Empty());
        }

        private static void GoToEndOfStream(Stream stream)
        {
            if (stream.Position != stream.Length)
                stream.Position = stream.Length;
        }
    }
}