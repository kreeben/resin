namespace Resin.KeyValue
{
    public class PageReader
    {
        private readonly Stream _valueStream;
        private readonly Stream _addressStream;
        private readonly int _pageSize;
        private readonly Stream _keyStream;

        public PageReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize = 4096)
        {
            _keyStream = keyStream;
            _valueStream = valueStream;
            _addressStream = addressStream;
            _pageSize = pageSize;
        }

        public ReadOnlySpan<byte> Get(long key)
        {
            if (_keyStream.Length > 0)
            {
                _keyStream.Position = 0;
                _addressStream.Position = 0;
                while (true)
                {
                    var value = new ByteArrayReader(_keyStream, _valueStream, _addressStream, pageSize: _pageSize).Get(key);
                    if (value == ReadOnlySpan<byte>.Empty)
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
