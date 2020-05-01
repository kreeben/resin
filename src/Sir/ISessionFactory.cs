using Microsoft.Extensions.Logging;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Sir
{
    public interface ISessionFactory
    {
        ILoggerFactory LoggerFactory { get; }
        IConfigurationProvider Config { get; }
        string Dir { get; }
        IStringModel Model { get; }

        Stream CreateAppendStream(string fileName, int bufferSize = 4096);
        Stream CreateAsyncAppendStream(string fileName, int bufferSize = 4096);
        Stream CreateAsyncReadStream(string fileName, int bufferSize = 4096);
        Stream CreateReadStream(string fileName, int bufferSize = 4096, FileOptions fileOptions = FileOptions.RandomAccess);
        void RegisterKeyMapping(ulong collectionId, ulong keyHash, long keyId);
        bool TryGetKeyId(ulong collectionId, ulong keyHash, out long keyId);
        void Write(WriteJob job, int reportSize);
        MemoryMappedFile OpenMMF(string fileName);
    }
}