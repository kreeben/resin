using System.Collections.Generic;

namespace Sir
{
    public class Job
    {
        public IStringModel Model { get; }
        public ulong CollectionId { get; private set; }
        public IEnumerable<IDictionary<string, object>> Documents { get; private set; }
        public HashSet<string> StoredFieldNames { get; private set; }
        public HashSet<string> IndexedFieldNames { get; private set; }

        public Job(
            ulong collectionId, 
            IEnumerable<IDictionary<string, object>> documents, 
            IStringModel model,
            HashSet<string> storedFieldNames,
            HashSet<string> indexedFieldNames)
        {
            Model = model;
            CollectionId = collectionId;
            Documents = documents;
            StoredFieldNames = storedFieldNames;
            IndexedFieldNames = indexedFieldNames;
        }
    }
}