namespace Resin.KeyValue
{
    public class ColumnReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly Stream _addressStream;
        private readonly Stream _valueStream;
        private readonly IDictionary<TKey, Address> _addressCache;
        private TKey[] _allKeys;

        public ColumnReader(ReadSession readSession)
        {
            _addressStream = readSession.AddressStream;
            _valueStream = readSession.ValueStream;
            _allKeys = ReadOperations.ReadSortedSetOfAllKeysInColumn<TKey>(readSession.KeyStream);
            _addressCache = new Dictionary<TKey, Address>();
        }

        public ReadOnlySpan<byte> Get(TKey key)
        {
            var index = new Span<TKey>(_allKeys).BinarySearch(key);
            if (index >= 0)
            {
                Address adr;
                if (!_addressCache.TryGetValue(key, out adr))
                {
                    adr = ReadOperations.GetAddress(_addressStream, index, 0);
                    _addressCache.Add(key, adr);
                }

                if (IsNodeHead(adr))
                {
                    return ConcatenateMany(adr, out _);
                }

                return ReadOperations.ReadValue(_valueStream, adr);
            }
            return ReadOnlySpan<byte>.Empty;
        }

        public ReadOnlySpan<byte> GetMany(TKey key, out int count)
        {
            count = 0;
            var index = new Span<TKey>(_allKeys).BinarySearch(key);
            if (index < 0)
                return ReadOnlySpan<byte>.Empty;

            Address adr;
            if (!_addressCache.TryGetValue(key, out adr))
            {
                adr = ReadOperations.GetAddress(_addressStream, index, 0);
                _addressCache.Add(key, adr);
            }

            if (IsNodeHead(adr))
            {
                return ConcatenateMany(adr, out count);
            }

            var single = ReadOperations.ReadValue(_valueStream, adr);
            count = single.IsEmpty ? 0 : 1;
            return single;
        }

        public int IndexOf(TKey key)
        {
            return new Span<TKey>(_allKeys).BinarySearch(key);
        }

        private bool IsNodeHead(Address adr)
        {
            if (adr.Length != LinkedAddressNode.Size)
                return false;

            var buf = new byte[LinkedAddressNode.Size];
            _valueStream.Position = adr.Offset;
            _valueStream.ReadExactly(buf);

            var node = LinkedAddressNode.Deserialize(buf);
            return node.Header == LinkedAddressNode.Magic;
        }

        private LinkedAddressNode ReadNode(Address adr)
        {
            var buf = new byte[LinkedAddressNode.Size];
            _valueStream.Position = adr.Offset;
            _valueStream.ReadExactly(buf);
            var node = LinkedAddressNode.Deserialize(buf);
            if (node.Header != LinkedAddressNode.Magic)
                throw new InvalidDataException("Invalid LinkedAddressNode header.");
            return node;
        }

        private ReadOnlySpan<byte> ConcatenateMany(Address headAdr, out int count)
        {
            long totalLen = 0;
            count = 0;

            // First pass: compute total length and count
            var curAdr = headAdr;
            while (true)
            {
                var node = ReadNode(curAdr);
                totalLen += node.Target.Length;
                count++;
                if (node.NextOffset == 0)
                    break;
                curAdr = new Address(node.NextOffset, LinkedAddressNode.Size);
            }

            var buf = new byte[totalLen];
            var dest = buf.AsSpan();
            var writePos = 0;

            // Second pass: copy values into buffer
            curAdr = headAdr;
            while (true)
            {
                var node = ReadNode(curAdr);
                var val = ReadOperations.ReadValue(_valueStream, node.Target);
                val.CopyTo(dest.Slice(writePos, val.Length));
                writePos += val.Length;

                if (node.NextOffset == 0)
                    break;
                curAdr = new Address(node.NextOffset, LinkedAddressNode.Size);
            }

            return buf;
        }
    }
}
