using MathNet.Numerics.LinearAlgebra;
using MessagePack;

namespace Resin.MachineLearning
{
    /// <summary>
    /// Provides on-demand, seekable access to a persisted <see cref="VectorNode"/> graph file
    /// written by <see cref="GraphSerializer"/>. Only nodes along the search path are read from disk.
    /// </summary>
    public class SeekableGraphReader : IDisposable
    {
        private readonly FileStream _stream;
        private readonly BinaryReader _reader;
        private readonly long[] _offsets;

        public int VectorDimensions { get; }
        public int NodeCount { get; }

        public SeekableGraphReader(string filePath)
        {
            _stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            _reader = new BinaryReader(_stream);

            // Read header
            Span<byte> magic = stackalloc byte[4];
            _stream.ReadExactly(magic);
            if (magic[0] != (byte)'A' || magic[1] != (byte)'O' || magic[2] != (byte)'S' || magic[3] != (byte)'G')
                throw new InvalidDataException("Invalid graph file: bad magic bytes.");

            int version = _reader.ReadInt32();
            if (version != 1)
                throw new InvalidDataException($"Unsupported graph file version: {version}.");

            VectorDimensions = _reader.ReadInt32();
            NodeCount = _reader.ReadInt32();

            // Read offset table
            _offsets = new long[NodeCount];
            for (int i = 0; i < NodeCount; i++)
                _offsets[i] = _reader.ReadInt64();
        }

        public SerializableNode ReadNode(int index)
        {
            if ((uint)index >= (uint)NodeCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            _stream.Position = _offsets[index];
            int payloadLength = _reader.ReadInt32();
            var buffer = _reader.ReadBytes(payloadLength);
            return MessagePackSerializer.Deserialize<SerializableNode>(buffer);
        }

        /// <summary>
        /// Performs a <see cref="Graph.ClosestMatch"/> traversal by reading only the nodes along the search path from disk.
        /// </summary>
        public Hit ClosestMatch(Vector<float> vector, GraphOptions options)
        {
            var rootNode = ReadNode(0);
            var rootVector = GraphSerializer.DeserializeVector(rootNode, VectorDimensions);
            var best = new VectorNode { Token = new Token { Label = rootNode.Label, Vector = rootVector } };
            double highscore = 0;

            int cursorIndex = 0;
            SerializableNode? cursor = rootNode;

            while (cursor != null)
            {
                var cursorVector = GraphSerializer.DeserializeVector(cursor, VectorDimensions);
                var angle = cursorVector == null ? 0 : vector.CosAngle(cursorVector);

                if (angle > options.FoldAngle)
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = new VectorNode { Token = new Token { Label = cursor.Label, Vector = cursorVector } };
                    }

                    if (angle >= options.IdenticalAngle || angle.Approximates(options.IdenticalAngle, options.Precision))
                        break;

                    if (cursor.LeftIndex >= 0)
                        cursor = ReadNode(cursor.LeftIndex);
                    else
                        cursor = null;
                }
                else
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = new VectorNode { Token = new Token { Label = cursor.Label, Vector = cursorVector } };
                    }

                    if (cursor.RightIndex >= 0)
                        cursor = ReadNode(cursor.RightIndex);
                    else
                        cursor = null;
                }
            }

            return new Hit(best, highscore);
        }

        /// <summary>
        /// Reads all nodes and reconstructs the full in-memory <see cref="VectorNode"/> tree.
        /// </summary>
        public VectorNode LoadFullTree()
        {
            var vectorNodes = new VectorNode[NodeCount];

            for (int i = 0; i < NodeCount; i++)
            {
                var sn = ReadNode(i);
                vectorNodes[i] = new VectorNode
                {
                    Token = new Token
                    {
                        Label = sn.Label,
                        Vector = GraphSerializer.DeserializeVector(sn, VectorDimensions)
                    }
                };
            }

            for (int i = 0; i < NodeCount; i++)
            {
                var sn = ReadNode(i);
                if (sn.LeftIndex >= 0)
                    vectorNodes[i].Left = vectorNodes[sn.LeftIndex];
                if (sn.RightIndex >= 0)
                    vectorNodes[i].Right = vectorNodes[sn.RightIndex];
            }

            return vectorNodes[0];
        }

        public void Dispose()
        {
            _reader.Dispose();
            _stream.Dispose();
        }
    }
}
