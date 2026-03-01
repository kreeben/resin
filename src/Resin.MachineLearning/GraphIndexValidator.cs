using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    /// <summary>
    /// Validates that an in-memory index returns a <see cref="Hit"/> with a score that equals or at least approximates closely the <see cref="Graph.IdenticalAngle"/> 
    /// when using the same dataset for validation that was used when building the index with <see cref="GraphBuilder"/>.
    /// </summary>
    public class GraphIndexValidator
    {
        private readonly VectorNode _index;
        private readonly TextWriter _log;
        private readonly Tokenizer _tokenizer;
        private readonly GraphOptions _options;

        public GraphIndexValidator(GraphOptions options, VectorNode index, Tokenizer tokenizer, TextWriter log = null)
        {
            _index = index;
            _log = log;
            _tokenizer = tokenizer;
            _options = options;
        }

        public bool ValidateIndex(IEnumerable<string> data)
        {
            var valid = true;
            var count = 0;

            foreach (var sentence in data)
            {
                var tokens = _tokenizer.Tokenize(sentence);
                var queryVector = Vector<float>.Build.Sparse(_tokenizer.LexiconSize);

                foreach (var token in tokens)
                {
                    queryVector += token.Vector;
                }

                var hit = _index.ClosestMatch(queryVector, _options);

                if (!hit.Score.Approximates(_options.IdenticalAngle, _options.Precision) && hit.Score < _options.IdenticalAngle)
                {
                    valid = false;
                    _log?.WriteLine($"Validation failed for {sentence}: score {hit.Score:F4} (expected ~{_options.IdenticalAngle})");
                    break;
                }
                else
                {
                    count++;
                }
            }

            _log?.WriteLine($"Validation complete: {count} queries passed.");
            return valid;
        }
    }
}
