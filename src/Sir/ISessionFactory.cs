using System.IO;

namespace Sir
{
    public interface ISessionFactory
    {
        IConfigurationProvider Config { get; }
        string Dir { get; }
        IStringModel Model { get; }

        bool CollectionExists(ulong collectionId);
        bool CollectionIsIndexOnly(ulong collectionId);
        Stream CreateAppendStream(string fileName, int bufferSize = 4096);
        Stream CreateAsyncAppendStream(string fileName, int bufferSize = 4096);
        Stream CreateAsyncReadStream(string fileName, int bufferSize = 4096);
        Stream CreateReadStream(string fileName, int bufferSize = 4096, FileOptions fileOptions = FileOptions.RandomAccess);
        void Dispose();
        void RegisterKeyMapping(ulong collectionId, ulong keyHash, long keyId);
        void RegisterCollectionAlias(ulong collectionId, ulong originalCollectionId);
        ulong GetCollectionReference(ulong collectionId);
        void Refresh();
        void Truncate(ulong collectionId);
        void TruncateIndex(ulong collectionId);
        bool TryGetKeyId(ulong collectionId, ulong keyHash, out long keyId);
    }
}