namespace Resin.KeyValue
{
    public class ColumnReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly Stream _addressStream;
        private readonly Stream _valueStream;
        private readonly IDictionary<TKey, Address> _addressCache;
        private TKey[] _allKeys;

        public ColumnReader(ReadSession readSession, int sizeOfT, int pageSize)
        {
            _addressStream = readSession.AddressStream;
            _valueStream = readSession.ValueStream;
            _allKeys = ReadUtil.ReadSortedSetOfAllKeysInColumn<TKey>(readSession.KeyStream);
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
                    adr = ReadUtil.GetAddress(_addressStream, index, 0);
                    _addressCache.Add(key, adr);
                }
                return ReadUtil.ReadValue(_valueStream, adr);

            }
            return ReadOnlySpan<byte>.Empty;
        }

        /// <summary>
        /// Returns a key's order in the column.
        /// </summary>
        public int IndexOf(TKey key)
        {
            return new Span<TKey>(_allKeys).BinarySearch(key);
        }
    }
}
