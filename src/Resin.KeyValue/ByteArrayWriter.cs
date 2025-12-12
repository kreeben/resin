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

            var keySpan = new Span<long>(_keyBuf, 0, _keyCount);
            int index = keySpan.BinarySearch(key);
            if (index >= 0)
            {
                return false;
            }

            var address = SerializeValue(value);

            // insertion point: ~index
            int insertAt = ~index;

            if (insertAt < _keyCount)
            {
                // shift right by one for both arrays
                Array.Copy(_keyBuf, insertAt, _keyBuf, insertAt + 1, _keyCount - insertAt);
                Array.Copy(_addressBuf, insertAt, _addressBuf, insertAt + 1, _keyCount - insertAt);
            }

            _keyBuf[insertAt] = key;
            _addressBuf[insertAt] = address;
            _keyCount++;

            return true;
        }

        public void PutOrAppend(long key, ReadOnlySpan<byte> value)
        {
            if (IsPageFull)
                throw new OutOfPageStorageException();

            var keySpan = new Span<long>(_keyBuf, 0, _keyCount);
            int index = keySpan.BinarySearch(key);

            if (index >= 0)
            {
                var address = _addressBuf[index];
                var combinedAddress = SerializeValueAppendExisting(address, value);
                _addressBuf[index] = combinedAddress;
            }
            else
            {
                var newAddress = SerializeValue(value);

                int insertAt = ~index;
                if (insertAt < _keyCount)
                {
                    Array.Copy(_keyBuf, insertAt, _keyBuf, insertAt + 1, _keyCount - insertAt);
                    Array.Copy(_addressBuf, insertAt, _addressBuf, insertAt + 1, _keyCount - insertAt);
                }

                _keyBuf[insertAt] = key;
                _addressBuf[insertAt] = newAddress;
                _keyCount++;
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

        // Writes the existing value (referenced by 'address') followed by 'append' directly to the end of the value stream,
        // returning the new combined Address, without allocating an intermediate combined buffer.
        private Address SerializeValueAppendExisting(Address address, ReadOnlySpan<byte> append)
        {
            if (append == Span<byte>.Empty || append.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(append.Length), "Append value cannot be null or empty.");

            // Position value stream at end to begin new record
            GoToEndOfStream(_valueStream);
            var start = _valueStream.Position;

            // Copy existing value from its original offset to the end, in small chunks to minimize memory
            const int chunkSize = 8192;
            Span<byte> chunk = stackalloc byte[Math.Min(chunkSize, (int)Math.Min(address.Length, chunkSize))];

            long remaining = address.Length;
            _valueStream.Position = address.Offset;
            while (remaining > 0)
            {
                int toRead = (int)Math.Min(chunk.Length, remaining);
                int read = _valueStream.Read(chunk.Slice(0, toRead));
                if (read != toRead)
                    throw new InvalidDataException();

                _valueStream.Position = start + (address.Length - remaining);
                _valueStream.Write(chunk.Slice(0, read));
                remaining -= read;
            }

            // After copying existing bytes, write the appended span directly
            _valueStream.Position = start + address.Length;
            _valueStream.Write(append);

            return new Address(start, address.Length + append.Length);
        }

        public void Serialize()
        {
            if (_keyCount == 0)
                return;

            GoToEndOfStream(_keyStream);

            // Cast Span<long> to Span<byte> before writing
            _keyStream.Write(MemoryMarshal.AsBytes(new Span<long>(_keyBuf, 0, _keyCount)));

            GoToEndOfStream(_addressStream);

            // Cast Span<Address> to Span<byte> before writing
            _addressStream.Write(MemoryMarshal.AsBytes(new Span<Address>(_addressBuf, 0, _keyCount)));

            // Reset count and reuse existing buffers (avoid allocating new arrays)
            _keyCount = 0;
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

            // Read directly into final long[] without intermediate byte[]
            int count = pageSize / sizeof(long);
            var longs = new long[count];
            keyStream.ReadExactly(MemoryMarshal.AsBytes(new Span<long>(longs)));
            return longs;
        }

        private Address[] ReadAddresses(Stream addressStream)
        {
            if (addressStream.Length == 0)
                throw new InvalidOperationException("Address stream is empty.");

            // Read directly into final Address[] without intermediate byte[]
            var addresses = new Address[_noOfKeysPerPage];
            addressStream.ReadExactly(MemoryMarshal.AsBytes(new Span<Address>(addresses)));
            return addresses;
        }
    }
}