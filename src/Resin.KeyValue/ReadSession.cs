namespace Resin.KeyValue
{
    public class ReadSession : IDisposable
    {
        private readonly Stream _addressStream;
        private readonly Stream _keyStream;
        private readonly Stream _valueStream;

        public Stream KeyStream => _keyStream;
        public Stream ValueStream => _valueStream;
        public Stream AddressStream => _addressStream;

        public ReadSession(Stream keyStream, Stream valueStream, Stream addressStream)
        {
            _keyStream = keyStream;
            _valueStream = valueStream;
            _addressStream = addressStream;
        }

        public ReadSession(DirectoryInfo workingDir, ulong collectionId)
        {
            var streamFactory = new StreamFactory(workingDir);

            _keyStream = streamFactory.CreateReadStream(collectionId, FileExtensions.Key);
            _valueStream = streamFactory.CreateReadStream(collectionId, FileExtensions.Value);
            _addressStream = streamFactory.CreateReadStream(collectionId, FileExtensions.Address);
        }

        public void Dispose()
        {
            if (_keyStream != null)
                _keyStream.Dispose();

            if (_valueStream != null)
                _valueStream.Dispose();

            if (_addressStream != null)
                _addressStream.Dispose();
        }
    }
}
