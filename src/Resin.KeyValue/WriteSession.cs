namespace Resin.KeyValue
{
    public class WriteSession : IDisposable
    {
        public int PageSize { get; }
        public Stream KeyStream => _keyStream;
        public Stream ValueStream => _valueStream;
        public Stream AddressStream => _addressStream;

        private readonly Stream _addressStream;
        private readonly Stream _keyStream;
        private readonly Stream _valueStream;
        private const int MaxByteArrayLength = 0x7FFFFFC7; // 2,147,483,591
        private bool _disposed;

        public WriteSession(int pageSize = 4096)
        {
            if (pageSize < 0 || pageSize > MaxByteArrayLength)
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"Page size must be between 0 and {MaxByteArrayLength} bytes.");

            _keyStream = new MemoryStream();
            _valueStream = new MemoryStream();
            _addressStream = new MemoryStream();

            PageSize = pageSize;
        }

        public WriteSession(DirectoryInfo workingDir, ulong collectionId, int pageSize = 4096)
        {
            var streamFactory = new StreamFactory(workingDir);

            _keyStream = streamFactory.CreateReadWriteStream(collectionId, FileExtensions.Key);
            _valueStream = streamFactory.CreateReadWriteStream(collectionId, FileExtensions.Value);
            _addressStream = streamFactory.CreateReadWriteStream(collectionId, FileExtensions.Address);

            PageSize = pageSize;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                // Dispose managed resources
                _keyStream?.Dispose();
                _valueStream?.Dispose();
                _addressStream?.Dispose();
            }

            // No unmanaged resources to free

            _disposed = true;
        }
    }
}
