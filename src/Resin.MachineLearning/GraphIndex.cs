using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    public class GraphIndex
    {
        public VectorNode Root { get; private set; }

        private readonly GraphOptions _options;

        public GraphIndex(GraphOptions options, VectorNode root)
        {
            Root = root;
            _options = options;
        }
        public Hit Search(Vector<float> vector)
        {
            return Root.ClosestMatch(vector, _options);
        }
    }
}
