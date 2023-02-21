using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.IO
{
    public class ColumnWriter : IDisposable
    {
        private readonly Stream _ixStream;
        private readonly bool _keepIndexStreamOpen;

        public ColumnWriter(Stream indexStream, bool keepStreamOpen = false)
        {
            _ixStream = indexStream;
            _keepIndexStreamOpen = keepStreamOpen;
        }

        public (int depth, int width) CreatePage(VectorNode column, Stream anglesStream, Stream vectorStream, PageIndexWriter pageIndexWriter, Stream postingsStream)
        {
            var page = SerializeTree(column, _ixStream, anglesStream, vectorStream, postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        private static (long offset, long length) SerializeTree(VectorNode tree, Stream indexStream, Stream anglesStream, Stream vectorStream, Stream postingsStream)
        {
            var offset = indexStream.Position;
            long length = 0;

            foreach (var rightNode in tree.RightNodes)
            {
                List<Angle> angles = null;

                if (postingsStream != null)
                {
                    angles = SerializePostings(rightNode, postingsStream);
                }

                if (vectorStream != null)
                {
                    rightNode.VectorOffset = SerializeVector(rightNode.Vector, vectorStream);
                }

                if (indexStream != null)
                {
                    length += SerializeRightNode(rightNode, angles, indexStream, anglesStream);
                }
            }

            return (offset, length);
        }

        private static long SerializeVector(ISerializableVector vector, Stream vectorStream)
        {
            var pos = vectorStream.Position;

            vector.Serialize(vectorStream);

            return pos;
        }

        private static long SerializeRightNode(VectorNode node, List<Angle> angles, Stream ixStream, Stream anglesStream)
        {
            var startPosition = ixStream.Position;

            // write node
            ixStream.Write(BitConverter.GetBytes(node.VectorOffset));
            ixStream.Write(BitConverter.GetBytes((long)node.Vector.ComponentCount));
            ixStream.Write(BitConverter.GetBytes((long)angles.Count));
            ixStream.Write(BitConverter.GetBytes(anglesStream.Position));

            // write angles and postings offsets
            foreach (var angle in angles)
            {
                anglesStream.Write(BitConverter.GetBytes(angle.ValueOf));
                anglesStream.Write(BitConverter.GetBytes(angle.PostingsOffset));
            }

            return ixStream.Position - startPosition;
        }

        private static List<Angle> SerializePostings(VectorNode node, Stream postingsStream)
        {
            if (node is null) throw new ArgumentNullException(nameof(node));
            if (node.LeftNodes == null) throw new ArgumentNullException(nameof(node.LeftNodes));
            if (node.LeftNodes.Count == 0) throw new ArgumentException("can't be empty", nameof(node.LeftNodes));

            var angles = new List<Angle>();

            foreach (var angle in node.LeftNodes)
            {
                var postingsOffset = postingsStream.Position;

                // serialize list length
                postingsStream.Write(BitConverter.GetBytes((long)angle.Value.documents.Count));

                // serialize address of next page (unknown at this time)
                postingsStream.Write(BitConverter.GetBytes((long)0));

                foreach (var docId in angle.Value.documents)
                {
                    postingsStream.Write(BitConverter.GetBytes(docId));
                }

                angles.Add(new Angle { ValueOf = angle.Key, PostingsOffset = postingsOffset, Labels = angle.Value.labels });
            }

            return angles;
        }

        [System.Diagnostics.DebuggerDisplay("{Labels}")]
        public class Angle
        {
            public double ValueOf { get; set; }
            public long PostingsOffset { get; set; }
            public List<object> Labels { get; set; }
        }

        public void Dispose()
        {
            if (!_keepIndexStreamOpen)
                _ixStream.Dispose();
        }
    }
}