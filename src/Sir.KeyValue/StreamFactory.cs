using System.IO;

namespace Sir.KeyValue
{
    public static class StreamFactory
    {
        public static Stream CreateAppendStream(string directory, ulong collectionId, string fileExtension)
        {
            if (!System.IO.Directory.Exists(directory))
            {
                System.IO.Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
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
    }
}
