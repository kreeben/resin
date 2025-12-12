using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class ByteArrayWriter
    {
        private readonly Stream _keyStream;
        private readonly Stream _valueStream;
        private readonly Stream _addressStream;
        private readonly WriteSession _session;
        private long[] _keyBuf;
        private Address[] _addressBuf;

        // Number of keys currently stored in the page/index of next key to insert.
        private int _keyCount;

        private int _noOfKeysPerPage;

        internal Stream KeyStream => _keyStream;

        // Indicates whether current page reached capacity
        public bool IsPageFull => _keyCount >= _noOfKeysPerPage;

        public ByteArrayWriter(WriteSession writeSession)
        {
            if (writeSession is null)
                throw new ArgumentNullException(nameof(writeSession));

            _valueStream = writeSession.ValueStream;
            _keyStream = writeSession.KeyStream;
            _addressStream = writeSession.AddressStream;
            _session = writeSession;
            _noOfKeysPerPage = _session.PageSize / sizeof(long);

            if (_keyStream.Length > 0)
            {
                _keyBuf = ReadKeys(_keyStream, _session.PageSize);
                _keyCount = _keyBuf.Length;
                _addressBuf = ReadAddresses(_addressStream);
            }
            else
            {
                _keyBuf = new long[_noOfKeysPerPage];
                _keyCount = 0;
                _addressBuf = new Address[_noOfKeysPerPage];
            }
        }

        public bool TryPut(long key, ReadOnlySpan<byte> value)
        {
            if (IsPageFull)
                throw new OutOfPageStorageException();

            int index = new Span<long>(_keyBuf, 0, _keyCount).BinarySearch(key);
            if (index >= 0)
            {
                return false;
            }

            var address = SerializeValue(value);

            _keyBuf[_keyCount] = key;
            _addressBuf[_keyCount] = address;
            _keyCount++;

            new Span<long>(_keyBuf, 0, _keyCount).Sort(new Span<Address>(_addressBuf, 0, _keyCount));

            return true;
        }

        public void PutOrAppend(long key, ReadOnlySpan<byte> value)
        {
            if (IsPageFull)
                throw new OutOfPageStorageException();

            int index = new Span<long>(_keyBuf, 0, _keyCount).BinarySearch(key);

            if (index >= 0)
            {
                var address = _addressBuf[index];
                var buf = new byte[address.Length + value.Length];
                _valueStream.Position = address.Offset;
                var read = _valueStream.Read(buf, 0, (int)address.Length);
                if (read != address.Length)
                    throw new InvalidDataException();

                value.CopyTo(buf.AsSpan((int)address.Length));

                _addressBuf[index] = SerializeValue(buf);
            }
            else
            {
                var address = SerializeValue(value);

                _keyBuf[_keyCount] = key;
                _addressBuf[_keyCount] = address;
                _keyCount++;

                new Span<long>(_keyBuf, 0, _keyCount).Sort(new Span<Address>(_addressBuf, 0, _keyCount));
            }
        }

        private Address SerializeValue(ReadOnlySpan<byte> value)
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

            // Cast Span<long> to Span<byte> before writing
            _keyStream.Write(MemoryMarshal.AsBytes(new Span<long>(_keyBuf)));

            GoToEndOfStream(_addressStream);

            // Cast Span<Address> to Span<byte> before writing
            _addressStream.Write(MemoryMarshal.AsBytes(new Span<Address>(_addressBuf)));

            _keyCount = 0;

            _keyBuf = new long[_noOfKeysPerPage];
            _addressBuf = new Address[_noOfKeysPerPage];
        }

        public bool TryPut(double key, ReadOnlySpan<byte> value)
        {
            long bits = BitConverter.DoubleToInt64Bits(key);
            return TryPut(bits, value);
        }

        public bool TryPut(int key, ReadOnlySpan<byte> value) => TryPut((long)key, value);

        public bool TryPut(float key, ReadOnlySpan<byte> value)
        {
            long bitsAsLong = BitConverter.SingleToInt32Bits(key);
            return TryPut(bitsAsLong, value);
        }

        public bool TryPut(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (key == Span<byte>.Empty || key.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(key), "Key cannot be null or empty.");

            long hashedKey = HashKey64(key);
            return TryPut(hashedKey, value);
        }

        public void PutOrAppend(int key, ReadOnlySpan<byte> value) => PutOrAppend((long)key, value);

        public void PutOrAppend(float key, ReadOnlySpan<byte> value)
        {
            long bitsAsLong = BitConverter.SingleToInt32Bits(key);
            PutOrAppend(bitsAsLong, value);
        }

        public void PutOrAppend(double key, ReadOnlySpan<byte> value)
        {
            long bits = BitConverter.DoubleToInt64Bits(key);
            PutOrAppend(bits, value);
        }

        private static long HashKey64(ReadOnlySpan<byte> key)
        {
            // FNV-1a 64-bit for wider key space
            const ulong fnvOffset = 14695981039346656037UL;
            const ulong fnvPrime = 1099511628211UL;

            ulong hash = fnvOffset;
            foreach (byte b in key)
            {
                hash ^= b;
                hash *= fnvPrime;
            }

            return unchecked((long)hash);
        }

        private static void GoToEndOfStream(Stream stream)
        {
            if (stream.Position != stream.Length)
                stream.Position = stream.Length;
        }

        private long[] ReadKeys(Stream keyStream, int pageSize)
        {
            if (keyStream.Length == 0)
                throw new InvalidOperationException("Key stream stream is empty.");

            var kbuf = new byte[pageSize];
            keyStream.ReadExactly(kbuf);
            return MemoryMarshal.Cast<byte, long>(kbuf).ToArray();
        }

        private Address[] ReadAddresses(Stream addressStream)
        {
            if (addressStream.Length == 0)
                throw new InvalidOperationException("Address stream is empty.");

            int addressBufSize = _noOfKeysPerPage * Address.Size;
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

            return addressList;
        }
    }
}