using System.Collections.Generic;

namespace Sir.Strings
{
    public class BagOfCharsModel : DistanceCalculator, IModel<string>
    {
        public double IdenticalAngle => 0.998d;
        public double FoldAngle => 0.55d;
        public override int NumOfDimensions => System.Text.Unicode.UnicodeRanges.All.Length;

        public IEnumerable<ISerializableVector> CreateEmbedding(string data, bool label, SortedList<int, float> embedding = null)
        {
            var source = data.ToCharArray();

            if (source.Length > 0)
            {
                if (embedding == null)
                    embedding = new SortedList<int, float>();
                else
                    embedding.Clear();

                var offset = 0;
                int index = 0;

                for (; index < source.Length; index++)
                {
                    char c = char.ToLower(source[index]);

                    if (char.IsLetterOrDigit(c) || char.GetUnicodeCategory(c) == System.Globalization.UnicodeCategory.MathSymbol)
                    {
                        embedding.AddOrAppendToComponent(c, 1);
                    }
                    else
                    {
                        if (embedding.Count > 0)
                        {
                            var len = index - offset;

                            var vector = new SerializableVector(
                                embedding,
                                NumOfDimensions,
                                label ? new string(source, offset, len) : null);

                            embedding.Clear();
                            yield return vector;
                        }

                        offset = index + 1;
                    }
                }

                if (embedding.Count > 0)
                {
                    var len = index - offset;

                    var vector = new SerializableVector(
                                embedding,
                                NumOfDimensions,
                                label ? new string(source, offset, len) : null);

                    yield return vector;
                }
            }
        }
    }

    public static class TokenizeOperations
    {
        public static void AddOrAppendToComponent(this SortedList<int, float> vec, int key, float value)
        {
            float v;

            if (vec.TryGetValue(key, out v))
            {
                vec[key] = v + value;
            }
            else
            {
                vec.Add(key, value);
            }
        }
    }
}