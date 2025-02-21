namespace Resin.KeyValue
{
    public class PageWriter : IDisposable
    {
        private readonly ByteArrayWriter _writer;
        private readonly Stream _keyStream;
        private readonly Stream _addressStream;

        public PageWriter(ByteArrayWriter writer, Stream keyStream, Stream addressStream)
        {
            _writer = writer;
            _keyStream = keyStream;
            _addressStream = addressStream;
        }

        public void Dispose()
        {
            if (_writer != null)
            {
                _writer.Serialize(_keyStream, _addressStream);
            }
        }

        public bool TryPut(long key, ReadOnlySpan<byte> value)
        {
            if (_writer.IsPageFull)
            {
                _writer.Serialize(_keyStream, _addressStream);

            }
            return _writer.TryPut(key, value);
        }

        public void Serialize()
        {
            _writer.Serialize(_keyStream, _addressStream);
        }
    }

    public interface ITokenizer
    {
        IEnumerable<ReadOnlySpan<byte>> GetTokens();
    }
}