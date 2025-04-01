namespace Resin.KeyValue
{
    public class ColumnReader<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly int _pageSize;
        private readonly int _sizeOfT;
        private readonly ReadSession _readSession;
        private readonly Stream _keyStream;
        private readonly Stream _addressStream;

        public ColumnReader(ReadSession readSession, int sizeOfT, int pageSize)
        {
            _keyStream = readSession.KeyStream;
            _addressStream = readSession.AddressStream;
            _pageSize = pageSize;
            _sizeOfT = sizeOfT;
            _readSession = readSession;
        }

        /// <summary>
        /// Returns a key's order in the column.
        /// </summary>
        public int IndexOf(TKey key)
        {
            if (_keyStream.Length > 0)
            {
                _keyStream.Position = 0;
                _addressStream.Position = 0;
                int index;
                var numOfPages = 0;
                var numOfItemsPerPage = _pageSize / _sizeOfT;

                while (true)
                {
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
            if (_keyStream.Length > 0)
            {
                _keyStream.Position = 0;
                _addressStream.Position = 0;
                while (true)
                {
                    var reader = new PageReader<TKey>(_readSession, sizeOfT: _sizeOfT, pageSize: _pageSize);
                    var value = reader.Get(key);
                    if (value.IsEmpty)
                    {
                        if (_keyStream.Position + 1 < _keyStream.Length)
                        {
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
