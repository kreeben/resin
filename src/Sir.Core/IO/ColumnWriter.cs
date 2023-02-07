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

        public (int depth, int width) CreatePage(VectorNode column, Stream vectorStream, PageIndexWriter pageIndexWriter, Stream postingsStream = null)
        {
            var page = SerializeTree(column, _ixStream, vectorStream, postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        private static (long offset, long length) SerializeTree(VectorNode tree, Stream indexStream = null, Stream vectorStream = null, Stream postingsStream = null)
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
                    rightNode.VectorOffset = Serialize(tree.Vector, vectorStream);
                }

                if (indexStream != null)
                {
                    length += SerializeNode(rightNode, angles, indexStream);
                }
            }

            return (offset, length);
        }

        private static long Serialize(ISerializableVector vector, Stream vectorStream)
        {
            var pos = vectorStream.Position;

            vector.Serialize(vectorStream);

            return pos;
        }

        private static long SerializeNode(VectorNode node, List<Angle> angles, Stream stream)
        {
            var startPosition = stream.Position;

            stream.Write(BitConverter.GetBytes(node.VectorOffset), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes((long)node.Vector.ComponentCount), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes((long)angles.Count), 0, sizeof(long));

            if (angles != null)
            {
                foreach (var angle in angles)
                {
                    stream.Write(BitConverter.GetBytes(angle.ValueOf), 0, sizeof(double));
                    stream.Write(BitConverter.GetBytes(angle.PostingsOffset), 0, sizeof(long));
                }
            }

            return stream.Position - startPosition;
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
                postingsStream.Write(BitConverter.GetBytes((long)angle.Value.Count));

                // serialize address of next page (unknown at this time)
                postingsStream.Write(BitConverter.GetBytes((long)0));

                foreach (var docId in angle.Value)
                {
                    postingsStream.Write(BitConverter.GetBytes(docId));
                }

                angles.Add(new Angle { ValueOf = angle.Key, PostingsOffset = postingsOffset });
            }

            return angles;
        }

        public class Angle
        {
            public double ValueOf { get; set; }
            public long PostingsOffset { get; set; }
        }

        public void Dispose()
        {
            if (!_keepIndexStreamOpen)
                _ixStream.Dispose();
        }
    }
}