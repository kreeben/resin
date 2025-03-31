namespace Resin.KeyValue
{
    public class ColumnReader<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly Stream _valueStream;
        private readonly Stream _addressStream;
        private readonly int _pageSize;
        private readonly int _sizeOfTInBytes;
        private readonly Stream _keyStream;

        public ColumnReader(Stream keyStream, Stream valueStream, Stream addressStream, int sizeOfTInBytes, int pageSize)
        {
            _keyStream = keyStream;
            _valueStream = valueStream;
            _addressStream = addressStream;
            _pageSize = pageSize;
            _sizeOfTInBytes = sizeOfTInBytes;
        }

        public int IndexOf(TKey key)
        {
            if (_keyStream.Length > 0)
            {
                _keyStream.Position = 0;
                _addressStream.Position = 0;
                int index;
                while (true)
                {
                    var reader = new ByteArrayReader<TKey>(_keyStream, _valueStream, _addressStream, sizeOfTInBytes: _sizeOfTInBytes, pageSize: _pageSize);
                    index = reader.IndexOf(key);
                    if (index < 0 && _keyStream.Position + 1 < _keyStream.Length)
                    {
                        continue;
                    }
                    else
                    {
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
                    var reader = new ByteArrayReader<TKey>(_keyStream, _valueStream, _addressStream, sizeOfTInBytes: _sizeOfTInBytes, pageSize: _pageSize);
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

        public void Dispose()
        {
            if (_addressStream != null)
            {
                _addressStream.Dispose();
            }
            if (_keyStream != null)
            {
                _keyStream.Dispose();
            }
            if (_valueStream != null)
            {
                _valueStream.Dispose();
            }
        }
    }

    public class DoublePageReader : ColumnReader<double>
    {
        public DoublePageReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize) : base(keyStream, valueStream, addressStream, sizeof(double), pageSize)
        {
        }
    }
}
