using MathNet.Numerics.LinearAlgebra;
using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.Search
{
    public class Boc2Model : IStringModel
    {
        public double IdenticalAngle => 0.9;
        public double FoldAngle => 0.55d;
        public int VectorWidth => 8;

        public IEnumerable<IVector> Tokenize(string text)
        {
            Span<char> source = text.ToLower().ToCharArray();
            int index = 0;
            int stepped = 0;
            var embeddings = new List<IVector>();
            var embedding = new SortedList<int, float>();

            for (; index < source.Length; index++)
            {
                char c = source[index];

                if (!char.IsLetterOrDigit(c) || char.IsWhiteSpace(c) || char.IsPunctuation(c) || char.IsSymbol(c))
                {
                    continue;
                }

                if (++stepped == VectorWidth)
                {
                    var compressed = new SortedList<int, float>();
                    var isWellFormed = false;

                    foreach (var kv in embedding)
                    {
                        if (kv.Value == 1)
                        {
                            compressed.Add(kv.Key, kv.Value);
                            isWellFormed = true;
                        }
                    }

                    embeddings.Add(new IndexedVector(
                        isWellFormed ? compressed : embedding,
                        source.Slice(index, Math.Min(VectorWidth, source.Length - index)).ToArray(),
                        VectorWidth));

                    embedding = new SortedList<int, float>();
                    stepped = 0;
                }

                embedding.AddOrAppendToComponent(c, 1);

            }

            if (embedding.Count > 0)
            {
                var compressed = new SortedList<int, float>();
                var isWellFormed = false;

                foreach (var kv in embedding)
                {
                    if (kv.Value == 1)
                    {
                        compressed.Add(kv.Key, kv.Value);
                        isWellFormed = true;
                    }
                }

                embeddings.Add(new IndexedVector(
                    isWellFormed ? compressed : embedding,
                    source.Slice(index - stepped, Math.Min(VectorWidth, source.Length - (index - stepped))).ToArray(),
                    VectorWidth));
            }

            return embeddings;
        }

        public double CosAngle(IVector vec1, IVector vec2)
        {
            var dotProduct = vec1.Value.DotProduct(vec2.Value);
            var dotSelf1 = vec1.Value.DotProduct(vec1.Value);
            var dotSelf2 = vec2.Value.DotProduct(vec2.Value);
            
            return (dotProduct / (Math.Sqrt(dotSelf1) * Math.Sqrt(dotSelf2)));
        }

        public double CosAngle(IVector vector, long vectorOffset, int componentCount, Stream vectorStream)
        {
            Span<byte> buf = new byte[componentCount * 2 * sizeof(float)];

            vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
            vectorStream.Read(buf);

            var index = MemoryMarshal.Cast<byte, int>(buf.Slice(0, componentCount * sizeof(int)));
            var values = MemoryMarshal.Cast<byte, float>(buf.Slice(componentCount * sizeof(float)));
            var tuples = new Tuple<int, float>[componentCount];

            for (int i = 0; i < componentCount; i++)
            {
                tuples[i] = new Tuple<int, float>(index[i], values[i]);
            }

            var otherVector = CreateVector.SparseOfIndexed(VectorWidth, tuples);

            var dotProduct = vector.Value.DotProduct(otherVector);
            var dotSelf1 = vector.Value.DotProduct(vector.Value);
            var dotSelf2 = otherVector.DotProduct(otherVector);

            return (dotProduct / (Math.Sqrt(dotSelf1) * Math.Sqrt(dotSelf2)));
        }
    }
}
