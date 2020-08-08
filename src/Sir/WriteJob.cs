using System.Collections.Generic;

namespace Sir
{
    public class WriteJob
    {
        public IStringModel Model { get; }
        public ulong CollectionId { get; private set; }
        public IEnumerable<IDictionary<string, object>> Documents { get; private set; }
        public HashSet<string> FieldNamesToStore { get; private set; }
        public HashSet<string> FieldNamesToIndex { get; private set; }

        public WriteJob(
            ulong collectionId, 
            IEnumerable<IDictionary<string, object>> documents, 
            IStringModel model,
            HashSet<string> fieldNamesToStore,
            HashSet<string> fieldNamesToIndex)
        {
            Model = model;
            CollectionId = collectionId;
            Documents = documents;
            FieldNamesToStore = fieldNamesToStore;
            FieldNamesToIndex = fieldNamesToIndex;
        }
    }
}