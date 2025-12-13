using System.Buffers;
using System.Runtime.InteropServices;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;

namespace Resin.TextAnalysis
{
    public static class VectorOperations
    {
        // Consistent format: [int count][count * int indices][count * float values]
        public static byte[] GetBytes(this Vector<float> vector)
        {
            // Collect non-zero components and their indices
            var pairs = new List<(int Index, float Value)>();
            foreach (var (index, value) in vector.EnumerateIndexed(Zeros.AllowSkip))
            {
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
            foreach (var (index, value) in vector.EnumerateIndexed(Zeros.AllowSkip))
            {
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
            foreach (var (index, value) in vector.EnumerateIndexed(Zeros.AllowSkip))
            {
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

        public static Vector<float> ToVectorFloat(this byte[] bufferWithHeader, int numOfDimensions)
        {
            var componentCount = BitConverter.ToInt32(bufferWithHeader);
            var len = componentCount * sizeof(float);
            var indices = MemoryMarshal.Cast<byte, int>(bufferWithHeader.AsSpan(sizeof(int), componentCount * sizeof(int))).ToArray();
            var values = MemoryMarshal.Cast<byte, float>(bufferWithHeader.AsSpan(sizeof(int) + componentCount * sizeof(int), len)).ToArray();
            int i = 0;
            return CreateVector.SparseOfIndexed(numOfDimensions, indices.Select(index => (index, values[i++])));
        }

        public static Vector<double> ToVectorDouble(this byte[] bufferWithHeader, int numOfDimensions)
        {
            var componentCount = BitConverter.ToInt32(bufferWithHeader);
            var ixLen = componentCount * sizeof(int);
            var valLen = componentCount * sizeof(double);
            var indices = MemoryMarshal.Cast<byte, int>(bufferWithHeader.AsSpan(sizeof(int), ixLen)).ToArray();
            var values = MemoryMarshal.Cast<byte, double>(bufferWithHeader.AsSpan(sizeof(int) + ixLen, valLen)).ToArray();
            int i = 0;
            return CreateVector.SparseOfIndexed(numOfDimensions, indices.Select(index => (index, values[i++])));
        }

        public static Vector<double> ToVectorDouble(this ReadOnlySpan<byte> bufferWithHeader, int numOfDimensions)
        {
            var componentCount = BitConverter.ToInt32(bufferWithHeader);
            var ixLen = componentCount * sizeof(int);
            var valLen = componentCount * sizeof(double);

            var indices = MemoryMarshal.Cast<byte, int>(bufferWithHeader.Slice(sizeof(int), ixLen)).ToArray();
            var values = MemoryMarshal.Cast<byte, double>(bufferWithHeader.Slice(sizeof(int) + ixLen, valLen)).ToArray();

            int i = 0;
            return CreateVector.SparseOfIndexed(numOfDimensions, indices.Select(index => (index, values[i++])));
        }

        public static double CosAngle(this Vector<float> first, Vector<float> second)
        {
            var dotProduct = first.DotProduct(second);
            var dotSelf1 = first.Norm(2);
            var dotSelf2 = second.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        public static double CosAngle(this Vector<double> first, Vector<double> second)
        {
            var dotProduct = first.DotProduct(second);
            var dotSelf1 = first.Norm(2);
            var dotSelf2 = second.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        public static Vector<double> Analyze(this Vector<double> first, Vector<double> second)
        {
            //var numOfDimensions = first.
            // Core metrics
            var dot = first.DotProduct(second);

            var norm1 = first.Norm(2);
            var norm2 = second.Norm(2);

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
            var euclidean = diff.L2Norm();
            var manhattan = diff.L1Norm();

            // Projection of first onto second (scalar length along second)
            var projLenOnSecond = norm2 > 0d ? dot / norm2 : 0d;

            // Overlap/Jaccard over non-zero indices to capture sparsity structure
            // Use EnumerateIndexed to support both sparse and dense storage safely.
            var firstIndices = new HashSet<int>();
            foreach (var (index, value) in first.EnumerateIndexed(Zeros.AllowSkip))
            {
                firstIndices.Add(index);
            }

            var overlapCount = 0;
            var secondIndices = new HashSet<int>();
            foreach (var (index, value) in second.EnumerateIndexed(Zeros.AllowSkip))
            {
                secondIndices.Add(index);
                if (firstIndices.Contains(index)) overlapCount++;
            }

            var unionCount = firstIndices.Count + secondIndices.Count - overlapCount;
            var jaccard = unionCount > 0 ? (double)overlapCount / unionCount : 0d;

            // Assemble a dense signature vector that is stable and informative
            // [cos, angle (rad), dot, ||first||, ||second||, euclidean, manhattan, projLenOnSecond, overlapCount, jaccard]
            var components = new[]
            {
                cos,
                angleRad,
                dot,
                norm1,
                norm2,
                euclidean,
                manhattan,
                projLenOnSecond,
                (double)overlapCount,
                jaccard
            };

            return CreateVector.SparseOfIndexed(first.Count, components.Select((v, i) => (i, v)));
        }

        public static string AsString(this Vector<float> vector)
        {
            var storage = (SparseVectorStorage<float>)vector.Storage;
            var chars = new List<char>();
            for (int i = 0; i < storage.Indices.Length; i++)
            {
                var v = Convert.ToInt32(storage.Values[i]);
                chars.Add((char)v);
                continue;
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
                continue;
            }
            return new string(chars.ToArray());
        }
    }
}
