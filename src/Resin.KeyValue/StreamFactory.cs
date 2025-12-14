namespace Resin.KeyValue
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

        public void Truncate(ulong columnId)
        {
            var label = columnId.ToString();
            foreach (var file in Directory.GetFiles(_directory.FullName))
            {
                if (file.Contains(label))
                    File.Delete(file);
            }
        }

        public Stream CreateReadWriteStream(ulong columnId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{columnId}.{fileExtension}");
            if (!File.Exists(fileName))
            {
                return new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
            }

            return new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
        }

        public Stream CreateReadStream(ulong columnId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{columnId}.{fileExtension}");
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

        public Stream CreateAppendStream(ulong columnId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{columnId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateOverwriteStream(ulong columnId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{columnId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateSeekableWriteStream(ulong columnId, string fileExtension)
        {
            var fileName = Path.Combine(_directory.FullName, $"{columnId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        }
    }
}
