using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using MessagePack;

namespace Resin.MachineLearning
{
    /// <summary>
    /// Serializes and deserializes a <see cref="VectorNode"/> graph to and from disk using a seekable binary format.
    /// <para>
    /// File layout:
    /// <code>
    /// [Header: 16 bytes]
    ///   Magic "AOSG" (4 bytes) | Version int32 | VectorDimensions int32 | NodeCount int32
    /// [Offset table: NodeCount × 8 bytes]
    ///   Each entry is a little-endian int64 byte offset to that node's MessagePack payload
    /// [Node payloads]
    ///   Each node is an independently serialized MessagePack <see cref="SerializableNode"/>
    /// </code>
    /// </para>
    /// The tree is flattened via BFS so index 0 is always the root.
    /// Individual nodes can be read without deserializing the entire file via <see cref="SeekableGraphReader"/>.
    /// </summary>
    public class GraphSerializer
    {
        private static readonly byte[] Magic = "AOSG"u8.ToArray();
        private const int Version = 1;
        private const int HeaderSize = 16; // 4 magic + 4 version + 4 dims + 4 count

        public void Save(VectorNode root, int vectorDimensions, string filePath)
        {
            var flatNodes = Flatten(root);

            using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = new BinaryWriter(stream);

            // Header
            writer.Write(Magic);
            writer.Write(Version);
            writer.Write(vectorDimensions);
            writer.Write(flatNodes.Length);

            // Reserve space for the offset table
            long offsetTablePosition = stream.Position;
            var offsets = new long[flatNodes.Length];
            for (int i = 0; i < flatNodes.Length; i++)
                writer.Write(0L); // placeholder

            // Write each node payload and record its offset
            for (int i = 0; i < flatNodes.Length; i++)
            {
                offsets[i] = stream.Position;
                var nodeBytes = MessagePackSerializer.Serialize(flatNodes[i]);
                writer.Write(nodeBytes.Length);
                writer.Write(nodeBytes);
            }

            // Seek back and write the actual offsets
            stream.Position = offsetTablePosition;
            for (int i = 0; i < offsets.Length; i++)
                writer.Write(offsets[i]);
        }

        public VectorNode Load(string filePath)
        {
            using var reader = new SeekableGraphReader(filePath);
            return reader.LoadFullTree();
        }

        private static SerializableNode[] Flatten(VectorNode root)
        {
            var nodes = new List<SerializableNode>();
            var nodeToIndex = new Dictionary<VectorNode, int>(ReferenceEqualityComparer.Instance);
            var queue = new Queue<VectorNode>();

            queue.Enqueue(root);
            nodeToIndex[root] = 0;
            nodes.Add(null!); // placeholder for root

            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                int currentIndex = nodeToIndex[current];

                int leftIndex = -1;
                int rightIndex = -1;

                if (current.Left != null)
                {
                    leftIndex = nodes.Count;
                    nodeToIndex[current.Left] = leftIndex;
                    nodes.Add(null!);
                    queue.Enqueue(current.Left);
                }

                if (current.Right != null)
                {
                    rightIndex = nodes.Count;
                    nodeToIndex[current.Right] = rightIndex;
                    nodes.Add(null!);
                    queue.Enqueue(current.Right);
                }

                int[]? sparseIndices = null;
                float[]? sparseValues = null;

                if (current.Token?.Vector != null)
                {
                    var storage = current.Token.Vector.Storage;
                    if (storage is SparseVectorStorage<float> sparse)
                    {
                        sparseIndices = sparse.Indices[..sparse.ValueCount];
                        sparseValues = sparse.Values[..sparse.ValueCount];
                    }
                    else
                    {
                        var dense = current.Token.Vector.ToArray();
                        var nonZero = new List<int>();
                        var values = new List<float>();
                        for (int i = 0; i < dense.Length; i++)
                        {
                            if (dense[i] != 0f)
                            {
                                nonZero.Add(i);
                                values.Add(dense[i]);
                            }
                        }
                        sparseIndices = nonZero.ToArray();
                        sparseValues = values.ToArray();
                    }
                }

                nodes[currentIndex] = new SerializableNode
                {
                    Label = current.Token?.Label,
                    SparseIndices = sparseIndices,
                    SparseValues = sparseValues,
                    LeftIndex = leftIndex,
                    RightIndex = rightIndex
                };
            }

            return nodes.ToArray();
        }

        internal static Vector<float>? DeserializeVector(SerializableNode sn, int vectorDimensions)
        {
            if (sn.SparseIndices == null || sn.SparseValues == null)
                return null;

            var tuples = new Tuple<int, float>[sn.SparseIndices.Length];
            for (int j = 0; j < sn.SparseIndices.Length; j++)
            {
                tuples[j] = Tuple.Create(sn.SparseIndices[j], sn.SparseValues[j]);
            }
            return Vector<float>.Build.SparseOfIndexed(vectorDimensions, tuples);
        }
    }
}
