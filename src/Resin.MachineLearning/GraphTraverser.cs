using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    public class GraphTraverser
    {
        public IEnumerable<Hit> Traverse(VectorNode node, Vector<float> vector)
        {
            if (node.Token != null && node.Token.Vector != null)
            {
                yield return new Hit(node, vector.CosAngle(node.Token.Vector));
            }
            if (node.Left != null)
            {
                foreach (var hit in Traverse(node.Left, vector))
                {
                    yield return hit;
                }
            }
            if (node.Right != null)
            {
                foreach (var hit in Traverse(node.Right, vector))
                {
                    yield return hit;
                }
            }
        }
    }
}
