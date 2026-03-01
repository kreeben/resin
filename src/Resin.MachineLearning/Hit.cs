using System.Diagnostics;

namespace Resin.MachineLearning
{
    [DebuggerDisplay("{Score} {Node}")]
    public class Hit
    {
        public double Score { get; set; }
        public VectorNode Node { get; set; }

        public Hit(VectorNode node, double score)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            Node = node;
            Score = score;
        }

        public override string ToString()
        {
            return $"{Score} {Node}";
        }
    }
}
