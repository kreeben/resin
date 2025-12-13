namespace Resin.KeyValue
{
    public class ColumnWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly PageWriter<TKey> _writer;
        private TKey[] _allKeys;

        public ColumnWriter(PageWriter<TKey> writer)
        {
            _writer = writer;
            _allKeys = ReadOperations.ReadSortedSetOfAllKeysInColumn<TKey>(writer.KeyStream);
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (key.KeyExists(_allKeys))
            {
                return false;
            }

            var put = _writer.TryPut(key, value);
            if (put && _writer.IsPageFull)
            {
                Serialize();
            }
            return put;
        }

        public void PutOrAppend(TKey key, ReadOnlySpan<byte> value)
        {
            var index = new Span<TKey>(_allKeys).BinarySearch(key);
            if (index >= 0)
            {
                var newValueAdr = WriteValue(_writer.ValueStream, value);

                var currentAdr = ReadOperations.GetAddress(_writer.AddressStream, index, 0);

                if (IsNodeHead(currentAdr, out var headNode))
                {
                    var tailOffset = currentAdr.Offset;
                    var curAdr = currentAdr;
                    var curNode = headNode;

                    while (curNode.NextOffset != 0)
                    {
                        tailOffset = curNode.NextOffset;
                        curAdr = new Address(tailOffset, LinkedAddressNode.Size);
                        curNode = ReadNode(_writer.ValueStream, curAdr);
                    }

                    var newTailAdr = WriteNode(_writer.ValueStream, new LinkedAddressNode(newValueAdr, nextOffset: 0));
                    LinkedAddressNode.OverwriteNextOffset(_writer.ValueStream, tailOffset, newTailAdr.Offset);
                }
                else
                {
                    var headAdr = WriteNode(_writer.ValueStream, new LinkedAddressNode(currentAdr, nextOffset: 0));
                    var newTailAdr = WriteNode(_writer.ValueStream, new LinkedAddressNode(newValueAdr, nextOffset: 0));
                    LinkedAddressNode.OverwriteNextOffset(_writer.ValueStream, headAdr.Offset, newTailAdr.Offset);
                    OverwriteAddressAtIndex(_writer.AddressStream, index, headAdr);
                }

                return;
            }

            _writer.PutOrAppend(key, value);
            if (_writer.IsPageFull)
            {
                Serialize();
            }
        }

        public void Serialize()
        {
            _writer.Serialize();
        }

        public void Dispose()
        {
            if (_writer != null)
            {
                Serialize();
            }
        }

        private static Address WriteValue(Stream valueStream, ReadOnlySpan<byte> value)
        {
            if (value == Span<byte>.Empty || value.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(value.Length), "Value cannot be null or empty.");

            if (valueStream.Position != valueStream.Length)
                valueStream.Position = valueStream.Length;

            var pos = valueStream.Position;
            valueStream.Write(value);
            return new Address(pos, value.Length);
        }

        private static Address WriteNode(Stream valueStream, in LinkedAddressNode node)
        {
            if (valueStream.Position != valueStream.Length)
                valueStream.Position = valueStream.Length;

            var pos = valueStream.Position;
            LinkedAddressNode.Serialize(valueStream, node);
            return new Address(pos, LinkedAddressNode.Size);
        }

        // FIX: Implement node header reading using the writer's ValueStream.
        private bool IsNodeHead(Address adr, out LinkedAddressNode node)
        {
            node = default;
            if (adr.Length != LinkedAddressNode.Size)
                return false;

            return TryReadNodeHeader(_writer.ValueStream, adr, out node) && node.Header == LinkedAddressNode.Magic;
        }

        private static bool TryReadNodeHeader(Stream valueStream, Address adr, out LinkedAddressNode node)
        {
            node = default;

            if (adr.Length != LinkedAddressNode.Size)
                return false;

            var buf = new byte[LinkedAddressNode.Size];
            valueStream.Position = adr.Offset;
            valueStream.ReadExactly(buf);

            var parsed = LinkedAddressNode.Deserialize(buf);
            if (parsed.Header != LinkedAddressNode.Magic)
                return false;

            node = parsed;
            return true;
        }

        private static LinkedAddressNode ReadNode(Stream valueStream, Address adr)
        {
            var buf = new byte[LinkedAddressNode.Size];
            valueStream.Position = adr.Offset;
            valueStream.ReadExactly(buf);
            var node = LinkedAddressNode.Deserialize(buf);
            if (node.Header != LinkedAddressNode.Magic)
                throw new InvalidDataException("Invalid LinkedAddressNode header.");
            return node;
        }

        private static void OverwriteAddressAtIndex(Stream addressStream, int index, Address newAdr)
        {
            var pos = index * Address.Size;
            addressStream.Position = pos;
            Address.Serialize(addressStream, new[] { newAdr });
        }
    }
}