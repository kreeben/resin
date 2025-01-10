using System.IO;

namespace Resin.KeyValue
{
    public static class StreamFactory
    {
        public static Stream CreateReadStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
        }

        public static Stream CreateReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
        }

        public static Stream CreateAppendStream(DirectoryInfo directory, ulong collectionId, string fileExtension)
        {
            if (!directory.Exists)
            {
                System.IO.Directory.CreateDirectory(directory.FullName);
            }

            var fileName = Path.Combine(directory.FullName, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public static Stream CreateOverwriteStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!System.IO.Directory.Exists(directory))
            {
                System.IO.Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        }

        public static Stream CreateAppendStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!System.IO.Directory.Exists(directory))
            {
                System.IO.Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public static Stream CreateSeekableWriteStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!System.IO.Directory.Exists(directory))
            {
                System.IO.Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        }
    }
}
