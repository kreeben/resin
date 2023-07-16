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

        public HashSet<long> DocIds { get; set; }
        public VectorNode Ancestor { get; private set; }
        public long ComponentCount { get; set; }
        public long VectorOffset { get; set; }
        public long PostingsPageId { get; set; }
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

        public VectorNode(ISerializableVector vector = null, long docId = -1, long postingsPageId = -1, long? keyId = null, HashSet<long> docIds = null)
        {
            Vector = vector;
            ComponentCount = vector == null ? 0 : vector.ComponentCount;
            PostingsPageId = postingsPageId;
            VectorOffset = -1;
            DocIds = docIds;
            KeyId = keyId;

            if (docId > -1)
            {
                if (DocIds == null)
                {
                    DocIds = new HashSet<long> { docId };
                }
                else
                {
                    DocIds.Add(docId);
                }
            }
        }

        public VectorNode(ISerializableVector vector, long postingsPageId, long vecOffset, long terminator, long weight)
        {
            PostingsPageId = postingsPageId;
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
