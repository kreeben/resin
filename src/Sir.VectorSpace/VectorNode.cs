using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Sir.VectorSpace
{
    /// <summary>
    /// Binary tree that consists of nodes that carry vectors as their payload. 
    /// Nodes are balanced according to the cosine similarity of their vectors.
    /// </summary>
    public class VectorNode
    {
        public const int BlockSize = sizeof(long) + sizeof(long) + sizeof(long) + sizeof(long) + sizeof(long);

        private VectorNode _right;
        private VectorNode _left;
        private VectorNode _ancestor;
        private long _weight;
        private object _sync = new object();

        public HashSet<long> DocIds { get; set; }
        public VectorNode Ancestor { get { return _ancestor; } }
        public long ComponentCount { get; set; }
        public long VectorOffset { get; set; }
        public long PostingsOffset { get; set; }
        public IVector Vector { get; set; }
        public object Sync
        {
            get { return _sync; }
        }

        public long Weight
        {
            get { return _weight; }
        }

        public VectorNode Right
        {
            get => _right;
            set
            {
                var old = _right;

                _right = value;

                if (value != null)
                {
                    _right._ancestor = this;
                    IncrementWeight();
                }
                else
                {
                    _weight = _weight - old.Weight - 1;
                }
            }
        }

        public VectorNode Left
        {
            get => _left;
            set
            {
                var old = _left;

                _left = value;

                if (value != null)
                {
                    _left._ancestor = this;
                    IncrementWeight();
                }
                else
                {
                    _weight = _weight - old.Weight - 1;
                }
            }
        }

        public long Terminator { get; set; }

        public IList<long> PostingsOffsets { get; set; }

        public VectorNode()
        {
            PostingsOffset = -1;
            VectorOffset = -1;
        }

        public VectorNode(IVector vector)
        {
            Vector = vector;
            ComponentCount = vector.ComponentCount;
            PostingsOffset = -1;
            VectorOffset = -1;
        }

        public VectorNode(long postingsOffset)
        {
            PostingsOffset = -1;
            PostingsOffsets = new List<long> { postingsOffset }; ;
            VectorOffset = -1;
        }

        public VectorNode(IVector vector, long docId)
        {
            Vector = vector;
            ComponentCount = vector.ComponentCount;
            PostingsOffset = -1;
            VectorOffset = -1;
            DocIds = new HashSet<long>();
            DocIds.Add(docId);
        }

        public VectorNode(long postingsOffset, long vecOffset, long terminator, long weight, IVector vector)
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
            Interlocked.Increment(ref _weight);

            var cursor = _ancestor;
            while (cursor != null)
            {
                Interlocked.Increment(ref cursor._weight);
                cursor = cursor._ancestor;
            }
        }

        public VectorNode Detach()
        {
            _ancestor = null;
            _left = null;
            _right = null;
            _weight = 0;

            return this;
        }

        public VectorNode DetachFromAncestor()
        {
            _ancestor = null;
            return this;
        }

        public string Visualize()
        {
            StringBuilder output = new StringBuilder();
            Visualize(this, output, 0);
            return output.ToString();
        }

        private void Visualize(VectorNode node, StringBuilder output, int depth)
        {
            if (node == null) return;
            output.Append('\t', depth);
            output.AppendFormat($"{node.ToDebugString()} w:{node.Weight}");
            output.AppendLine();

            Visualize(node.Left, output, depth + 1);
            Visualize(node.Right, output, depth);
        }

        public string ToDebugString()
        {
            var vals = Vector.Value.AsArray();

            var w = new StringBuilder();

            w.Append('|');

            for (int i = 0; i < vals.Length; i++)
            {
                w.Append(vals[i]);

                w.Append('|');
            }

            w.Append(Vector.ToString());

            return w.ToString();
        }

        public override string ToString()
        {
            return Vector.ToString();
        }

        public VectorNodeData ToData()
        {
            long terminator;

            if (Left == null && Right == null) // there are no children
            {
                terminator = 3;
            }
            else if (Left == null) // there is a right but no left
            {
                terminator = 2;
            }
            else if (Right == null) // there is a left but no right
            {
                terminator = 1;
            }
            else // there is a left and a right
            {
                terminator = 0;
            }

            return new VectorNodeData(VectorOffset, PostingsOffset, ComponentCount, Weight, terminator);
        }
    }

    public struct VectorNodeData
    {
        public long VectorOffset { get; }
        public long PostingsOffset { get; }
        public long ComponentCount { get; }
        public long Weight { get; }
        public long Terminator { get; }

        public VectorNodeData(long vectorOffset, long postingsOffset, long componentCount, long weight, long terminator)
        {
            VectorOffset = vectorOffset;
            PostingsOffset = postingsOffset;
            ComponentCount = componentCount;
            Weight = weight;
            Terminator = terminator;
        }
    }
}
