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
            Score = score;
            Node = node;
        }

        public override string ToString()
        {
            return $"{Score} {Node}";
        }
    }
}
