using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    public class Tokenizer
    {
        private readonly IDictionary<string, int> _lexicon;

        public int LexiconSize => _lexicon.Count;

        public Tokenizer(IDictionary<string, int> lexicon)
        {
            _lexicon = lexicon;
        }

        public IEnumerable<Token> Tokenize(string text, char separator = ';')
        {
            foreach (var word in text.Split(separator, StringSplitOptions.RemoveEmptyEntries))
            {
                if (_lexicon.TryGetValue(word, out int index))
                {
                    var vector = new float[_lexicon.Count];
                    vector[index] = 1f; // One-hot encoding
                    yield return new Token { Label = word, Vector = Vector<float>.Build.SparseOfArray(vector) };
                }
            }
        }
    }
}
