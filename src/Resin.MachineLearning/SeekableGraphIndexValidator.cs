using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    /// <summary>
    /// Validates that a disk based index returns a <see cref="Hit"/> with a score that equals or at least approximates closely the <see cref="Graph.IdenticalAngle"/> 
    /// when using the same dataset for validation that was used when building the index with <see cref="GraphBuilder"/>.
    /// </summary>
    public class SeekableGraphIndexValidator
    {
        private readonly TextWriter _log;
        private readonly Tokenizer _tokenizer;
        private readonly GraphOptions _options;
        private readonly SeekableGraphReader _reader;

        public SeekableGraphIndexValidator(GraphOptions options, SeekableGraphReader reader, Tokenizer tokenizer, TextWriter log = null)
        {
            _reader = reader;
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

                var hit = _reader.ClosestMatch(queryVector, _options);

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
