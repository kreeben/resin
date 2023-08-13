using Sir.IO;
using System.IO;

namespace Sir
{
    public interface ISessionFactory
    {
        Stream CreateAppendStream(string directory, ulong collectionId, string fileExtension);
        Stream CreateAppendStream(string directory, ulong collectionId, long keyId, string fileExtension);
        Stream CreateSeekWriteStream(string directory, ulong collectionId, long keyId, string fileExtension);
        Stream CreateReadStream(string fileName, int bufferSize = 4096);
        ColumnReader CreateColumnReader(string directory, ulong collectionId, long keyId, IModel model);
    }
}

namespace Sir.IO
{
}