using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    public class GraphBuilder
    {
        public Tokenizer Tokenizer { get; private set; }
        private readonly TextWriter _log;
        private readonly GraphOptions _options;

        public GraphBuilder(GraphOptions options, Tokenizer tokenizer, TextWriter log = null)
        {
            Tokenizer = tokenizer;
            _log = log;
            _options = options;
        }

        public VectorNode BuildGraph(IEnumerable<string> sentences)
        {
            var root = new VectorNode { Token = new Token { Label = "root" } };
            var nodeCount = 0;
            var processedCount = 0;
            foreach (var sentence in sentences)
            {
                var tokens = Tokenizer.Tokenize(sentence);
                var sentenceNode = new VectorNode { Token = new Token { Label = sentence, Vector = Vector<float>.Build.Sparse(Tokenizer.LexiconSize) } };
                foreach (var token in tokens)
                {
                    sentenceNode.Token.Vector += token.Vector;
                }
                processedCount++;
                if (root.TryAdd(sentenceNode, _options))
                {
                    _log?.WriteLine($"Added sentence nodes: {++nodeCount} (processed: {processedCount}, compression: {((processedCount - nodeCount) / processedCount) * 100}%)");
                }
            }
            return root;
        }
    }
}
