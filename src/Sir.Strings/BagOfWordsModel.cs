using System.Collections.Generic;

namespace Sir.Strings
{
    public class BagOfWordsModel : DistanceCalculator, IModel<string>
    {
        public double IdenticalAngle => 0.998d;
        public double FoldAngle => 0.55d;
        public override int NumOfDimensions => System.Text.Unicode.UnicodeRanges.All.Length;
        private readonly SortedList<int, float> _embedding = new SortedList<int, float>();

        public IEnumerable<ISerializableVector> CreateEmbedding(string data, bool label)
        {
            var source = data.ToCharArray();

            if (source.Length > 0)
            {
                _embedding.Clear();

                var offset = 0;
                int index = 0;

                for (; index < source.Length; index++)
                {
                    char c = char.ToLower(source[index]);

                    if (char.IsPunctuation(c))
                    {
                        yield return new SerializableVector();
                    }
                    else if (char.IsLetterOrDigit(c) || char.GetUnicodeCategory(c) == System.Globalization.UnicodeCategory.MathSymbol)
                    {
                        _embedding.AddOrAppendToComponent(c, 1);
                    }
                    else
                    {
                        if (_embedding.Count > 0)
                        {
                            var len = index - offset;

                            var vector = new SerializableVector(
                                _embedding,
                                NumOfDimensions,
                                label ? new string(source, offset, len) : null);

                            _embedding.Clear();
                            yield return vector;
                        }

                        offset = index + 1;
                    }
                }

                if (_embedding.Count > 0)
                {
                    var len = index - offset;

                    var vector = new SerializableVector(
                                _embedding,
                                NumOfDimensions,
                                label ? new string(source, offset, len) : null);

                    yield return vector;
                }
            }
        }
    }
}