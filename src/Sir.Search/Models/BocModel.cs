using MathNet.Numerics.LinearAlgebra;
using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace Sir.Search
{
    public class BocModel : IStringModel
    {
        public double IdenticalAngle => 0.88d;
        public double FoldAngle => 0.58d;
        public int VectorWidth => 256;
        public int UnicodeStartingPoint => 32;

        public IEnumerable<IVector> Tokenize(Memory<char> source)
        {
            var tokens = new List<IVector>();
            
            if (source.Length > 0)
            {
                var embedding = new SortedList<int, float>();
                var offset = 0;
                int index = 0;
                var span = source.Span;

                for (; index < source.Length; index++)
                {
                    char c = char.ToLower(span[index]);

                    if (c < UnicodeStartingPoint || c > UnicodeStartingPoint + VectorWidth)
                    {
                        continue;
                    }

                    if (char.IsLetterOrDigit(c))
                    {
                        embedding.AddOrAppendToComponent(c);
                    }
                    else
                    {
                        if (embedding.Count > 0)
                        {
                            var len = index - offset;
                            var slice = source.Slice(offset, len);

                            var vector = new IndexedVector(
                                embedding,
                                VectorWidth,
                                slice);

                            embedding.Clear();
                            tokens.Add(vector);
                        }

                        offset = index + 1;
                    }
                }

                if (embedding.Count > 0)
                {
                    var len = index - offset;

                    var vector = new IndexedVector(
                                embedding,
                                VectorWidth,
                                source.Slice(offset, len));

                    tokens.Add(vector);
                }
            }

            return tokens;
        }

        public double CosAngle(IVector vec1, IVector vec2)
        {
            //var dotProduct = vec1.Value.DotProduct(vec2.Value);
            //var dotSelf1 = vec1.Value.DotProduct(vec1.Value);
            //var dotSelf2 = vec2.Value.DotProduct(vec2.Value);
            //return (dotProduct / (Math.Sqrt(dotSelf1) * Math.Sqrt(dotSelf2)));

            var dotProduct = vec1.Value.DotProduct(vec2.Value);
            var dotSelf1 = vec1.Value.Norm(2);
            var dotSelf2 = vec2.Value.Norm(2);

            return dotProduct / (dotSelf1 * dotSelf2);
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

        public double CosAngle(IVector vector, long vectorOffset, int componentCount, MemoryMappedViewAccessor vectorView)
        {
            var otherVector = DeserializeVector(vectorOffset, componentCount, vectorView);

            var dotProduct = vector.Value.DotProduct(otherVector.Value);
            var dotSelf1 = vector.Value.DotProduct(vector.Value);
            var dotSelf2 = otherVector.Value.DotProduct(otherVector.Value);

            return (dotProduct / (Math.Sqrt(dotSelf1) * Math.Sqrt(dotSelf2)));
        }

        private IndexedVector DeserializeVector(long vectorOffset, int componentCount, MemoryMappedViewAccessor vectorView)
        {
            var index = new int[componentCount];
            var values = new float[componentCount];
            var read = vectorView.ReadArray(vectorOffset, index, 0, index.Length);

            if (read < componentCount)
                throw new Exception("bad data");

            read = vectorView.ReadArray(vectorOffset + componentCount * sizeof(int), values, 0, values.Length);

            if (read < componentCount)
                throw new Exception("bad data");

            return new IndexedVector(index, values, VectorWidth, null);
        }
    }
}
