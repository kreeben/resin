using System.Buffers;
using System.Runtime.InteropServices;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;

namespace Resin.TextAnalysis
{
    public static class VectorOperations
    {
        // SIMD-accelerated L2 norm for double vectors when dense; sparse falls back to scalar.
        private static double L2NormSimd(this Vector<double> v)
        {
            var storage = v.Storage;
            if (storage is DenseVectorStorage<double> dense)
            {
                var data = dense.Data;
                int n = data.Length;
                int i = 0;
                System.Numerics.Vector<double> acc = System.Numerics.Vector<double>.Zero;
                int simdCount = System.Numerics.Vector<double>.Count;
                for (; i <= n - simdCount; i += simdCount)
                {
                    var vec = new System.Numerics.Vector<double>(data, i);
                    acc += vec * vec;
                }
                double sum = 0d;
                for (int k = 0; k < simdCount; k++) sum += acc[k];
                for (; i < n; i++) sum += data[i] * data[i];
                return Math.Sqrt(sum);
            }
            else if (storage is SparseVectorStorage<double> sparse)
            {
                var values = sparse.Values;
                double sum = 0d;
                for (int i = 0; i < values.Length; i++)
                {
                    var x = values[i];
                    sum += x * x;
                }
                return Math.Sqrt(sum);
            }
            else
            {
                // Fallback: use MathNet implementation
                return v.Norm(2);
            }
        }

        // SIMD-accelerated dot product for double vectors when both are dense; otherwise scalar/sparse.
        private static double DotSimd(this Vector<double> a, Vector<double> b)
        {
            var sa = a.Storage;
            var sb = b.Storage;
            if (sa is DenseVectorStorage<double> da && sb is DenseVectorStorage<double> db)
            {
                var xa = da.Data;
                var xb = db.Data;
                int n = Math.Min(xa.Length, xb.Length);
                int i = 0;
                System.Numerics.Vector<double> acc = System.Numerics.Vector<double>.Zero;
                int simdCount = System.Numerics.Vector<double>.Count;
                for (; i <= n - simdCount; i += simdCount)
                {
                    var va = new System.Numerics.Vector<double>(xa, i);
                    var vb = new System.Numerics.Vector<double>(xb, i);
                    acc += va * vb;
                }
                double sum = 0d;
                for (int k = 0; k < simdCount; k++) sum += acc[k];
                for (; i < n; i++) sum += xa[i] * xb[i];
                return sum;
            }
            else if (sa is SparseVectorStorage<double> saSp && sb is SparseVectorStorage<double> sbSp)
            {
                // Iterate over intersection of indices for sparse dot
                var ia = saSp.Indices;
                var va = saSp.Values;
                var ib = sbSp.Indices;
                var vb = sbSp.Values;
                int i = 0, j = 0;
                double sum = 0d;
                while (i < ia.Length && j < ib.Length)
                {
                    int ai = ia[i];
                    int bj = ib[j];
                    if (ai == bj)
                    {
                        sum += va[i] * vb[j];
                        i++; j++;
                    }
                    else if (ai < bj) i++; else j++;
                }
                return sum;
            }
            else if (sa is SparseVectorStorage<double> sSp && sb is DenseVectorStorage<double> dSt)
            {
                var ix = sSp.Indices;
                var val = sSp.Values;
                var data = dSt.Data;
                double sum = 0d;
                for (int k = 0; k < ix.Length; k++)
                {
                    int idx = ix[k];
                    if ((uint)idx < (uint)data.Length)
                    {
                        sum += val[k] * data[idx];
                    }
                }
                return sum;
            }
            else if (sa is DenseVectorStorage<double> dSt2 && sb is SparseVectorStorage<double> sSp2)
            {
                var ix = sSp2.Indices;
                var val = sSp2.Values;
                var data = dSt2.Data;
                double sum = 0d;
                for (int k = 0; k < ix.Length; k++)
                {
                    int idx = ix[k];
                    if ((uint)idx < (uint)data.Length)
                    {
                        sum += val[k] * data[idx];
                    }
                }
                return sum;
            }
            else
            {
                // Fallback
                return a.DotProduct(b);
            }
        }

        // Consistent format: [int count][count * int indices][count * float values]
        public static byte[] GetBytes(this Vector<float> vector)
        {
            // Collect non-zero components and their indices
            var pairs = new List<(int Index, float Value)>();
            foreach (var pair in vector.EnumerateIndexed(Zeros.AllowSkip))
            {
                int index = pair.Item1;
                float value = pair.Item2;
                pairs.Add((index, value));
            }

            int count = pairs.Count;
            int headerSize = sizeof(int);
            int indicesSize = count * sizeof(int);
            int valuesSize = count * sizeof(float);
            int totalSize = headerSize + indicesSize + valuesSize;

            byte[] rented = ArrayPool<byte>.Shared.Rent(totalSize);
            try
            {
                var span = rented.AsSpan(0, totalSize);

                // Write header (count)
                MemoryMarshal.Write(span, ref count);
                int offset = headerSize;

                // Write indices
                var indicesSpan = MemoryMarshal.Cast<byte, int>(span.Slice(offset, indicesSize));
                for (int i = 0; i < count; i++)
                {
                    indicesSpan[i] = pairs[i].Index;
                }
                offset += indicesSize;

                // Write values
                var valuesSpan = MemoryMarshal.Cast<byte, float>(span.Slice(offset, valuesSize));
                for (int i = 0; i < count; i++)
                {
                    valuesSpan[i] = pairs[i].Value;
                }

                // Return exact-sized array
                return span.ToArray();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        // Consistent format: [int count][count * int indices][count * double values]
        public static byte[] GetBytes(this Vector<double> vector)
        {
            var pairs = new List<(int Index, double Value)>();
            foreach (var pair in vector.EnumerateIndexed(Zeros.AllowSkip))
            {
                int index = pair.Item1;
                double value = pair.Item2;
                pairs.Add((index, value));
            }

            int count = pairs.Count;
            int headerSize = sizeof(int);
            int indicesSize = count * sizeof(int);
            int valuesSize = count * sizeof(double);
            int totalSize = headerSize + indicesSize + valuesSize;

            byte[] rented = ArrayPool<byte>.Shared.Rent(totalSize);
            try
            {
                var span = rented.AsSpan(0, totalSize);

                // Write header (count)
                MemoryMarshal.Write(span, ref count);
                int offset = headerSize;

                // Write indices
                var indicesSpan = MemoryMarshal.Cast<byte, int>(span.Slice(offset, indicesSize));
                for (int i = 0; i < count; i++)
                {
                    indicesSpan[i] = pairs[i].Index;
                }
                offset += indicesSize;

                // Write values
                var valuesSpan = MemoryMarshal.Cast<byte, double>(span.Slice(offset, valuesSize));
                for (int i = 0; i < count; i++)
                {
                    valuesSpan[i] = pairs[i].Value;
                }

                return span.ToArray();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        // Generic path that enforces fixed-size element serialization.
        // Format: [int count][count * int indices][count * elementSize values]
        public static byte[] GetBytes<T>(this Vector<T> vector, Func<T, ReadOnlySpan<byte>> serialize, int elementSize)
            where T : struct, IEquatable<T>, IFormattable
        {
            var pairs = new List<(int Index, T Value)>();
            foreach (var pair in vector.EnumerateIndexed(Zeros.AllowSkip))
            {
                int index = pair.Item1;
                T value = pair.Item2;
                pairs.Add((index, value));
            }

            int count = pairs.Count;
            int headerSize = sizeof(int);
            int indicesSize = count * sizeof(int);
            int valuesSize = count * elementSize;
            int totalSize = headerSize + indicesSize + valuesSize;

            byte[] rented = ArrayPool<byte>.Shared.Rent(totalSize);
            try
            {
                var span = rented.AsSpan(0, totalSize);

                // Write header (count)
                MemoryMarshal.Write(span, ref count);
                int offset = headerSize;

                // Write indices
                var indicesSpan = MemoryMarshal.Cast<byte, int>(span.Slice(offset, indicesSize));
                for (int i = 0; i < count; i++)
                {
                    indicesSpan[i] = pairs[i].Index;
                }
                offset += indicesSize;

                // Write values (fixed-size per element)
                var valuesBytes = span.Slice(offset, valuesSize);
                for (int i = 0; i < count; i++)
                {
                    var elemBytes = serialize(pairs[i].Value);
                    if (elemBytes.Length != elementSize)
                        throw new ArgumentException("serialize must return fixed-size element bytes", nameof(serialize));
                    elemBytes.CopyTo(valuesBytes.Slice(i * elementSize, elementSize));
                }

                return span.ToArray();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        public static MathNet.Numerics.LinearAlgebra.Vector<double> ToVectorDouble(this ReadOnlySpan<byte> bufferWithHeader, int numOfDimensions)
        {
            var componentCount = BitConverter.ToInt32(bufferWithHeader);
            var ixLen = componentCount * sizeof(int);
            var valLen = componentCount * sizeof(double);

            var indices = MemoryMarshal.Cast<byte, int>(bufferWithHeader.Slice(sizeof(int), ixLen)).ToArray();
            var values = MemoryMarshal.Cast<byte, double>(bufferWithHeader.Slice(sizeof(int) + ixLen, valLen)).ToArray();

            var pairs = new (int, double)[componentCount];
            for (int i = 0; i < componentCount; i++)
            {
                pairs[i] = (indices[i], values[i]);
            }
            return CreateVector.SparseOfIndexed(numOfDimensions, pairs);
        }

        public static double CosAngle(this Vector<double> first, Vector<double> second)
        {
            var dotProduct = first.DotSimd(second);
            var dotSelf1 = first.L2NormSimd();
            var dotSelf2 = second.L2NormSimd();

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        public static MathNet.Numerics.LinearAlgebra.Vector<double> Analyze(this Vector<double> first, Vector<double> second)
        {
            // Core metrics
            var dot = first.DotSimd(second);

            var norm1 = first.L2NormSimd();
            var norm2 = second.L2NormSimd();

            double cos = 0d;
            if (dot != 0d && norm1 != 0d && norm2 != 0d)
            {
                cos = dot / (norm1 * norm2);
                // Clamp to valid domain for acos to be safe against numerical drift
                if (cos > 1d) cos = 1d;
                else if (cos < -1d) cos = -1d;
            }

            var angleRad = Math.Acos(cos);

            // Distance metrics
            var diff = first - second;
            var euclidean = diff.L2NormSimd();
            var manhattan = diff.L1Norm();

            // Projection of first onto second (scalar length along second)
            var projLenOnSecond = norm2 > 0d ? dot / norm2 : 0d;

            // Overlap/Jaccard over non-zero indices to capture sparsity structure
            var firstIndices = new HashSet<int>();
            foreach (var pair in first.EnumerateIndexed(Zeros.AllowSkip))
            {
                firstIndices.Add(pair.Item1);
            }

            var overlapCount = 0;
            var secondIndices = new HashSet<int>();
            foreach (var pair in second.EnumerateIndexed(Zeros.AllowSkip))
            {
                var index = pair.Item1;
                secondIndices.Add(index);
                if (firstIndices.Contains(index)) overlapCount++;
            }

            var unionCount = firstIndices.Count + secondIndices.Count - overlapCount;
            var jaccard = unionCount > 0 ? (double)overlapCount / unionCount : 0d;

            // Assemble dense signature vector
            var components = new double[10];
            components[0] = cos;
            components[1] = angleRad;
            components[2] = dot;
            components[3] = norm1;
            components[4] = norm2;
            components[5] = euclidean;
            components[6] = manhattan;
            components[7] = projLenOnSecond;
            components[8] = (double)overlapCount;
            components[9] = jaccard;

            var pairs = new (int, double)[components.Length];
            for (int i = 0; i < components.Length; i++)
            {
                pairs[i] = (i, components[i]);
            }

            return CreateVector.SparseOfIndexed(first.Count, pairs);
        }

        public static string AsString(this Vector<float> vector)
        {
            var storage = (SparseVectorStorage<float>)vector.Storage;
            var chars = new List<char>();
            for (int i = 0; i < storage.Indices.Length; i++)
            {
                var v = Convert.ToInt32(storage.Values[i]);
                chars.Add((char)v);
            }
            return new string(chars.ToArray());
        }

        public static string AsString(this Vector<double> vector)
        {
            var storage = (SparseVectorStorage<double>)vector.Storage;
            var chars = new List<char>();
            for (int i = 0; i < storage.Indices.Length; i++)
            {
                var v = Convert.ToInt64(storage.Values[i]);
                chars.Add((char)v);
            }
            return new string(chars.ToArray());
        }
    }
}
