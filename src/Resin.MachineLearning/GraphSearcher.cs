using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    public class GraphSearcher
    {
        private readonly GraphTraverser _traverser;
        public GraphSearcher(GraphTraverser traverser)
        {
            _traverser = traverser;
        }
        public Hit? Search(VectorNode root, Vector<float> vector)
        {
            return _traverser.Traverse(root, vector).OrderByDescending(h => h.Score).FirstOrDefault();
        }
    }
}
