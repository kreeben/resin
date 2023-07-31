using Sir.IO;
using System.Collections.Generic;

namespace Sir.Strings
{
    public class NGramModel : DistanceCalculator, IModel<string>
    {
        public double IdenticalAngle => 0.95d;
        public double FoldAngle => 0.45d;
        public override int NumOfDimensions { get; }

        private readonly BagOfCharsModel _wordTokenizer;

        public NGramModel(BagOfCharsModel wordTokenizer)
        {
            _wordTokenizer = wordTokenizer;
            NumOfDimensions = wordTokenizer.NumOfDimensions * 2;
        }

        public IEnumerable<ISerializableVector> CreateEmbedding(string data, bool label, SortedList<int, float> embedding = null)
        {
            if (embedding == null)
                embedding = new SortedList<int, float>();
            else
                embedding.Clear();

            ISerializableVector vec0 = null;

            var i = 0;

            foreach (var token in _wordTokenizer.CreateEmbedding(data, label, embedding))
            {
                if (vec0 == null)
                {
                    vec0 = token;
                }
                else
                {
                    var first = vec0.Shift(0, NumOfDimensions);
                    var second = token.Shift(NumOfDimensions/2, NumOfDimensions);
                    first.AddInPlace(second);
                    vec0 = token;
                    yield return first;
                }
                i++;
            }

            if(i==1)
            {
                yield return vec0.Shift(0, NumOfDimensions);
            }
        }
    }
}