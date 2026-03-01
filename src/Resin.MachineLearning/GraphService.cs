namespace Resin.MachineLearning
{
    public class GraphService
    {
        private readonly GraphBuilder _builder;
        private readonly GraphSearcher _searcher;
        private readonly GraphOptions _options;

        public GraphService(GraphOptions options, GraphBuilder builder, GraphSearcher searcher)
        {
            _builder = builder;
            _searcher = searcher;
            _options = options;
        }
        public GraphIndex BuildIndex(IEnumerable<string> sentences)
        {
            var root = _builder.BuildGraph(sentences);
            return new GraphIndex(_options, root);
        }
        public Hit Search(GraphIndex index, string query)
        {
            var queryNode = new VectorNode { Token = new Token { Label = query } };
            var tokens = _builder.Tokenizer.Tokenize(query);
            foreach (var token in tokens)
            {
                queryNode.Token.Vector += token.Vector;
            }
            return _searcher.Search(index.Root, queryNode.Token.Vector);
        }
    }
}
