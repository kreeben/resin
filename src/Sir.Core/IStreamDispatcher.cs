using MathNet.Numerics.LinearAlgebra;
using Sir.IO;
using System.Collections.Generic;
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
        IEnumerable<long> AllKeyIds(string directory, ulong collectionId);
        ColumnReader CreateColumnReader(string directory, ulong collectionId, long keyId, IModel model);
        IDictionary<long, Vector<float>> DeserializeMeanVectors(IModel model, string directory, ulong collectionId);
    }
}