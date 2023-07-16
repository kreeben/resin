using System.Collections.Generic;

namespace Sir
{
    [System.Diagnostics.DebuggerDisplay("{Key}:{Label}")]
    public class Term : BooleanStatement
    {
        public ISerializableVector Vector { get; }
        public long KeyId { get; }
        public string Key { get; }
        public string Directory { get; }
        public ulong CollectionId { get; }
        public IList<long> PostingsPageIds { get; set; }
        public double Score { get; set; }
        public IList<(ulong collectionId, long documentId)> DocumentIds { get; set; }
        public object Label => Vector.Label;

        public Term(
            string directory,
            ulong collectionId,
            long keyId,
            string key,
            ISerializableVector vector,
            bool and,
            bool or,
            bool not)
            : base(and, or, not)
        {
            Directory = directory;
            CollectionId = collectionId;
            KeyId = keyId;
            Key = key;
            Vector = vector;
            IsIntersection = and;
            IsUnion = or;
            IsSubtraction = not;
        }
    }
}