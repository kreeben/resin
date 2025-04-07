namespace Resin.KeyValue
{
    public class ColumnReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly int _pageSize;
        private readonly int _sizeOfT;
        private readonly ReadSession _readSession;
        private readonly int _addressPageSize;
        private readonly Stream _keyStream;
        private readonly Stream _addressStream;

        public ColumnReader(ReadSession readSession, int sizeOfT, int pageSize)
        {
            _keyStream = readSession.KeyStream;
            _addressStream = readSession.AddressStream;
            _pageSize = pageSize;
            _sizeOfT = sizeOfT;
            _readSession = readSession;
            _addressPageSize = (_pageSize / _sizeOfT) * Address.Size;
        }

        /// <summary>
        /// Returns a key's order in the column.
        /// </summary>
        public int IndexOf(TKey key)
        {
            if (_keyStream.Length > 0)
            {
                int index = 0;
                var numOfPages = 0;
                var numOfItemsPerPage = _pageSize / _sizeOfT;

                while (true)
                {
                    var keyOffset = index * _pageSize;
                    var addressOffset = index * _addressPageSize;
                    _keyStream.Position = keyOffset;
                    _addressStream.Position = addressOffset;

                    var reader = new PageReader<TKey>(_readSession, sizeOfT: _sizeOfT, pageSize: _pageSize);
                    numOfPages++;
                    index = reader.IndexOf(key);
                    if (index < 0 && _keyStream.Position + 1 < _keyStream.Length)
                    {
                        continue;
                    }
                    else
                    {
                        index = index + (numOfItemsPerPage * numOfPages);
                        break;
                    }
                }
                return index;
            }
            return -1;
        }

        public ReadOnlySpan<byte> Get(TKey key)
        {
            var keyLen = _keyStream.Length;
            if (keyLen > 0)
            {
                var addressPageSize = (_pageSize / _sizeOfT) * Address.Size;
                int index = 0;
                while (true)
                {
                    var keyOffset = index * _pageSize;
                    var addressOffset = index * addressPageSize;
                    _keyStream.Position = keyOffset;
                    _addressStream.Position = addressOffset;
                    var reader = new PageReader<TKey>(_readSession, sizeOfT: _sizeOfT, pageSize: _pageSize);
                    var value = reader.Get(key);
                    if (value.IsEmpty)
                    {
                        if (_keyStream.Position + 1 < keyLen)
                        {
                            keyOffset += addressPageSize;
                            index++;
                            continue;
                        }
                        else
                        {
                            break;
                        }
                    }
                    else
                    {
                        return value;
                    }
                }
            }

            return ReadOnlySpan<byte>.Empty;
        }
    }
}
