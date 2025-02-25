namespace Resin.TextAnalysis
{
    public class StreamFactory
    {
        private readonly DirectoryInfo _directory;

        public DirectoryInfo CurrentDirectory { get { return _directory; } }

        public StreamFactory(DirectoryInfo directory)
        {
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));

            if (!directory.Exists)
            {
                directory.Create();
            }
        }

        public void Truncate()
        {
            foreach (var file in Directory.GetFiles(_directory.FullName))
                File.Delete(file);
        }

        public Stream CreateReadStream(ulong collectionId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{collectionId}.{fileExtension}");
            if (!File.Exists(fileName))
            {
                return new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
            }

            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
        }

        public Stream CreateReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
        }

        public Stream CreateAppendStream(ulong collectionId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateOverwriteStream(ulong collectionId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateSeekableWriteStream(ulong collectionId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        }
    }
}
