namespace Resin.KeyValue
{
    public class ReadSession : IDisposable
    {
        private readonly Stream _addressStream;
        private readonly Stream _keyStream;
        private readonly Stream _valueStream;

        public int PageSize { get; }
        public Stream KeyStream => _keyStream;
        public Stream ValueStream => _valueStream;
        public Stream AddressStream => _addressStream;

        public ReadSession(WriteSession tx)
        {
            if (tx is null)
                throw new ArgumentNullException(nameof(tx));

            _keyStream = tx.KeyStream;
            _valueStream = tx.ValueStream;
            _addressStream = tx.AddressStream;
            PageSize = tx.PageSize;
        }

        public ReadSession(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize = 4096)
        {
            if (keyStream is null) throw new ArgumentNullException(nameof(keyStream));
            if (valueStream is null) throw new ArgumentNullException(nameof(valueStream));
            if (addressStream is null) throw new ArgumentNullException(nameof(addressStream));
            if (pageSize < 0) throw new ArgumentOutOfRangeException(nameof(pageSize));

            _keyStream = keyStream;
            _valueStream = valueStream;
            _addressStream = addressStream;
            PageSize = pageSize;
        }

        public ReadSession(DirectoryInfo workingDir, ulong columnId, int pageSize = 4096)
        {
            if (workingDir is null)
                throw new ArgumentNullException(nameof(workingDir));

            var streamFactory = new StreamFactory(workingDir);

            _keyStream = streamFactory.CreateReadStream(columnId, FileExtensions.Key);
            _valueStream = streamFactory.CreateReadStream(columnId, FileExtensions.Value);
            _addressStream = streamFactory.CreateReadStream(columnId, FileExtensions.Address);
            PageSize = pageSize;
        }

        public void Dispose()
        {
            _keyStream?.Dispose();
            _valueStream?.Dispose();
            _addressStream?.Dispose();
        }
    }
}
