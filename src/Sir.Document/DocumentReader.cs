using System.Collections.Generic;

namespace Sir.Documents
{
    public static class DocumentReader
    {
        public static Document Read(long documentId, DocumentRegistryReader documentReader, HashSet<string> select = null, double? score = null)
        {
            var docInfo = documentReader.GetDocumentAddress(documentId);
            var docMap = documentReader.GetDocumentMap(docInfo.offset, docInfo.length);
            var fields = new List<Field>();

            for (int i = 0; i < docMap.Length; i++)
            {
                var kvp = docMap[i];
                var kInfo = documentReader.GetAddressOfKey(kvp.keyId);
                var key = (string)documentReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);

                if (select == null || select.Contains(key))
                {
                    var vInfo = documentReader.GetAddressOfValue(kvp.valId);
                    var val = documentReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    fields.Add(new Field(key, val, kvp.keyId));
                }
            }

            return new Document(fields, collectionId: documentReader.CollectionId, documentId: documentId, score: (score.HasValue ? score.Value : 0));
        }
    }
}
