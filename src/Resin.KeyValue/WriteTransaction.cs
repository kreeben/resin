namespace Resin.KeyValue
{
    public class WriteTransaction : IDisposable
    {
        private readonly Stream _addressStream;
        private readonly Stream _keyStream;
        private readonly Stream _valueStream;

        public Stream KeyStream => _keyStream;
        public Stream ValueStream => _valueStream;
        public Stream AddressStream => _addressStream;

        public WriteTransaction()
        {
            _keyStream = new MemoryStream();
            _valueStream = new MemoryStream();
            _addressStream = new MemoryStream();
        }


        public WriteTransaction(DirectoryInfo workingDir, ulong collectionId)
        {
            var streamFactory = new StreamFactory(workingDir);

            _keyStream = streamFactory.CreateReadWriteStream(collectionId, FileExtensions.Key);
            _valueStream = streamFactory.CreateReadWriteStream(collectionId, FileExtensions.Value);
            _addressStream = streamFactory.CreateReadWriteStream(collectionId, FileExtensions.Address);
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
