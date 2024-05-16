using System.Collections.Generic;
using System.Diagnostics;

namespace Sir
{
    [DebuggerDisplay("{Score} {Node}")]
    public class Hit
    {
        public double Score { get; set; }
        public VectorNode Node { get; set; }
        public List<long> PostingsOffsets { get; set; }

        public Hit (VectorNode node, double score)
        {
            if (node == null) throw new System.ArgumentNullException(nameof(node));
            if (node.PostingsOffset == null) throw new System.ArgumentException(nameof(node));

            Node = node;
            Score = score;
            PostingsOffsets = new List<long> { node.PostingsOffset.Value };
        }

        public Hit(VectorNode node, double score, long postingsOffset)
        {
            Score = score;
            Node = node;
            PostingsOffsets = new List<long> { postingsOffset };
        }

        public override string ToString()
        {
            return $"{Score} {Node}";
        }
    }
}
