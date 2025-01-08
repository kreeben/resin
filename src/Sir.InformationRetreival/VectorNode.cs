using System.Collections.Generic;

namespace Sir
{
    /// <summary>
    /// Binary tree that consists of nodes that carry vectors as their payload. 
    /// Nodes are balanced by taking into account the cosine similarity of their neighbouring vectors.
    /// </summary>
    public class VectorNode
    {
        public const int BlockSize = sizeof(long) + sizeof(long) + sizeof(long) + sizeof(long) + sizeof(long);

        private VectorNode _right;
        private VectorNode _left;
        private long _weight;

        public HashSet<long> Documents { get; set; }
        public VectorNode Ancestor { get; private set; }
        public long ComponentCount { get; set; }
        public long? VectorOffset { get; set; }
        public long? PostingsOffset { get; set; }
        public ISerializableVector Vector { get; set; }

        public long Weight
        {
            get { return _weight; }
        }

        public VectorNode Right
        {
            get => _right;
            set
            {
                _right = value;
                _right.Ancestor = this;
                IncrementWeight();
            }
        }

        public VectorNode Left
        {
            get => _left;
            set
            {
                _left = value;
                _left.Ancestor = this;
                IncrementWeight();
            }
        }

        public long Terminator { get; set; }

        public bool IsRoot => Ancestor == null && Vector == null;

        public long? KeyId { get; set; }

        public VectorNode(ISerializableVector vector = null, long? documentId = null, long? postingsOffset = null, long? keyId = null, HashSet<long> documents = null)
        {
            Vector = vector;
            ComponentCount = vector == null ? 0 : vector.ComponentCount;
            PostingsOffset = postingsOffset;
            Documents = documents;
            KeyId = keyId;

            if (documentId.HasValue)
            {
                if (Documents == null)
                {
                    Documents = new HashSet<long> { documentId.Value };
                }
                else
                {
                    Documents.Add(documentId.Value);
                }
            }
        }

        public VectorNode(ISerializableVector vector, long postingsOffset, long vecOffset, long terminator, long weight)
        {
            PostingsOffset = postingsOffset;
            VectorOffset = vecOffset;
            Terminator = terminator;
            _weight = weight;
            ComponentCount = vector.ComponentCount;
            Vector = vector;
        }

        public void IncrementWeight()
        {
            _weight++;

            var cursor = Ancestor;

            while (cursor != null)
            {
                cursor._weight++;

                cursor = cursor.Ancestor;
            }
        }

        public override string ToString()
        {
            return IsRoot ? "*" : Vector.Label == null ? Vector.ToString() : Vector.Label.ToString();
        }
    }
}
