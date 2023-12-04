using Sir.IO;
using System.IO;

namespace Sir
{
    public interface IStreamDispatcher
    {
        Stream CreateAppendStream(string directory, ulong collectionId, string fileExtension);
        Stream CreateAppendStream(string directory, ulong collectionId, long keyId, string fileExtension);
        Stream CreateReadStream(string fileName);
        void RegisterKeyMapping(string directory, ulong collectionId, ulong keyHash, long keyId);
        bool TryGetKeyId(string directory, ulong collectionId, ulong keyHash, out long keyId);
        long GetKeyId(string directory, ulong collectionId, ulong keyHash);
    }
}