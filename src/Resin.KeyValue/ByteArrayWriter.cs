using System.Buffers;
using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class ByteArrayWriter : IDisposable
    {
        private readonly Stream _keyStream;
        private readonly Stream _valueStream;
        private readonly Stream _addressStream;
        private readonly WriteSession _session;

        // Pooled buffers
        private readonly ArrayPool<long> _longPool = ArrayPool<long>.Shared;
        private readonly ArrayPool<Address> _addressPool = ArrayPool<Address>.Shared;
        private readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;

        private long[] _keyBuf;
        private Address[] _addressBuf;

        // Reusable copy buffer for append path
        private byte[]? _copyBuf;
        private const int DefaultCopyBufferSize = 8192;

        // Number of keys currently stored in the page/index of next key to insert.
        private int _keyCount;

        private int _noOfKeysPerPage;

        internal Stream KeyStream => _keyStream;
        internal Stream AddressStream => _addressStream;
        internal Stream ValueStream => _valueStream;

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

            // Rent pooled buffers sized to the page capacity
            _keyBuf = _longPool.Rent(_noOfKeysPerPage);
            _addressBuf = _addressPool.Rent(_noOfKeysPerPage);
            _keyCount = 0;

            // Rent reusable copy buffer once (will be reused for all appends)
            _copyBuf = _bytePool.Rent(DefaultCopyBufferSize);

            if (_keyStream.Length > 0)
            {
                // Read existing keys/addresses based on actual stream length, not page size
                var loadedKeys = ReadKeys(_keyStream);
                var loadedAddresses = ReadAddresses(_addressStream, loadedKeys.Length);

                loadedKeys.CopyTo(_keyBuf, 0);
                loadedAddresses.CopyTo(_addressBuf, 0);
                _keyCount = loadedKeys.Length;
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

        private Address SerializeValueAppendExisting(Address address, ReadOnlySpan<byte> append)
        {
            if (append == Span<byte>.Empty || append.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(append.Length), "Append value cannot be null or empty.");

            if (_copyBuf is null || _copyBuf.Length < 1)
            {
                _copyBuf = _bytePool.Rent(DefaultCopyBufferSize);
            }

            // Position value stream at end to begin new record
            GoToEndOfStream(_valueStream);
            var start = _valueStream.Position;

            // Copy existing value from its original offset to the end, using reusable heap buffer
            long remaining = address.Length;
            var srcPos = address.Offset;
            while (remaining > 0)
            {
                int toRead = (int)Math.Min(_copyBuf!.Length, remaining);
                _valueStream.Position = srcPos;
                int read = _valueStream.Read(_copyBuf.AsSpan(0, toRead));
                if (read != toRead)
                    throw new InvalidDataException();

                var destPos = start + (address.Length - remaining);
                _valueStream.Position = destPos;
                _valueStream.Write(_copyBuf.AsSpan(0, read));
                remaining -= read;
                srcPos += read;
            }

            _valueStream.Position = start + address.Length;
            _valueStream.Write(append);

            return new Address(start, address.Length + append.Length);
        }

        public void Serialize()
        {
            if (_keyCount == 0)
                return;

            GoToEndOfStream(_keyStream);
            _keyStream.Write(MemoryMarshal.AsBytes(new Span<long>(_keyBuf, 0, _keyCount)));

            GoToEndOfStream(_addressStream);
            _addressStream.Write(MemoryMarshal.AsBytes(new Span<Address>(_addressBuf, 0, _keyCount)));

            // Reset count; keep pooled buffers for reuse.
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

        // Read keys based on actual stream length; supports partially filled pages.
        private long[] ReadKeys(Stream keyStream)
        {
            var totalBytes = keyStream.Length;
            if (totalBytes == 0)
                throw new InvalidOperationException("Key stream is empty.");

            var count = (int)(totalBytes / sizeof(long));
            var longs = new long[count];
            keyStream.Position = 0;
            keyStream.ReadExactly(MemoryMarshal.AsBytes(new Span<long>(longs)));
            return longs;
        }

        // Read addresses based on actual stream length or provided key count to keep arrays aligned.
        private Address[] ReadAddresses(Stream addressStream, int expectedCount)
        {
            var totalBytes = addressStream.Length;
            if (totalBytes == 0)
                throw new InvalidOperationException("Address stream is empty.");

            var count = (int)(totalBytes / Address.Size);
            if (count < expectedCount)
                throw new InvalidDataException("Address stream contains fewer addresses than keys.");

            // Read exactly expectedCount to align with loaded keys
            var addresses = new Address[expectedCount];
            addressStream.Position = 0;
            addressStream.ReadExactly(MemoryMarshal.AsBytes(new Span<Address>(addresses)));
            return addresses;
        }

        public void Dispose()
        {
            if (_keyBuf != null)
            {
                _longPool.Return(_keyBuf, clearArray: false);
                _keyBuf = Array.Empty<long>();
            }

            if (_addressBuf != null)
            {
                _addressPool.Return(_addressBuf, clearArray: false);
                _addressBuf = Array.Empty<Address>();
            }

            if (_copyBuf != null)
            {
                _bytePool.Return(_copyBuf, clearArray: false);
                _copyBuf = null;
            }
        }
    }
}