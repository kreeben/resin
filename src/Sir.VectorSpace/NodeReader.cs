using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Sir.VectorSpace
{
    /// <summary>
    /// Index bitmap reader. Each block is a <see cref="Sir.Search.VectorNode"/>.
    /// </summary>
    public class NodeReader : INodeReader
    {
        private readonly ISessionFactory _sessionFactory;
        private readonly ulong _collectionId;

        public long KeyId { get; }

        public NodeReader(
            ulong collectionId,
            long keyId,
            ISessionFactory sessionFactory)
        {
            KeyId = keyId;
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
        }

        public Hit ClosestTerm(IVector vector, IStringModel model, long keyId)
        {
            var pages = GetAllPages(
                Path.Combine(_sessionFactory.Dir, $"{_collectionId}.{KeyId}.ixtp"));

            var hits = new ConcurrentBag<Hit>();

            Parallel.ForEach(pages, page =>
            //foreach (var page in pages)
            {
                var hit = ClosestTermInPage(vector, model, keyId, page.offset);

                if (hit != null)
                    hits.Add(hit);
            });

            Hit best = null;

            foreach (var hit in hits)
            {
                if (best == null || hit.Score > best.Score)
                {
                    best = hit;
                }
                else if (hit.Score >= model.IdenticalAngle)
                {
                    GraphBuilder.MergePostings(best.Node, hit.Node);
                }
            }

            return best;
        }

        public IList<(long offset, long length)> GetAllPages(string pageFileName)
        {
            using (var ixpStream = _sessionFactory.CreateReadStream(pageFileName))
            {
                return new PageIndexReader(ixpStream).GetAll();
            }
        }

        private Hit ClosestTermInPage(
            IVector vector, IStringModel model, long keyId, long pageOffset)
        {
            var vectorFileName = Path.Combine(_sessionFactory.Dir, $"{_collectionId}.vec");
            var ixFileName = Path.Combine(_sessionFactory.Dir, string.Format("{0}.{1}.ix", _collectionId, keyId));

            using (var vectorFile = _sessionFactory.CreateReadStream(vectorFileName))
            using (var ixFile = _sessionFactory.CreateReadStream(ixFileName))
            {
                ixFile.Seek(pageOffset, SeekOrigin.Begin);

                var hit = ClosestMatchInSegment(
                        vector,
                        ixFile,
                        vectorFile,
                        model);

                if (hit.Score > 0)
                {
                    return hit;
                }

                return null;
            }
        }

        private Hit ClosestMatchInSegment(
            IVector queryVector,
            Stream indexFile,
            Stream vectorFile,
            IModel model)
        {
            Span<byte> block = stackalloc byte[VectorNode.BlockSize];
            var best = new Hit();
            var read = indexFile.Read(block);

            while (read > 0)
            {
                var vecOffset = BitConverter.ToInt64(block.Slice(0));
                var postingsOffset = BitConverter.ToInt64(block.Slice(sizeof(long)));
                var componentCount = BitConverter.ToInt64(block.Slice(sizeof(long) * 2));
                var cursorTerminator = BitConverter.ToInt64(block.Slice(sizeof(long) * 4));
                var angle = model.CosAngle(queryVector, vecOffset, (int)componentCount, vectorFile);

                if (angle > model.FoldAngle)
                {
                    if (best == null || angle > best.Score)
                    {
                        best.Score = angle;
                        best.Node = new VectorNode(postingsOffset);
                    }
                    else if (angle == best.Score)
                    {
                        best.Node.PostingsOffsets.Add(postingsOffset);
                    }

                    // We need to determine if we can traverse further left.
                    bool canGoLeft = cursorTerminator == 0 || cursorTerminator == 1;

                    if (canGoLeft)
                    {
                        // There exists either a left and a right child or just a left child.
                        // Either way, we want to go left and the next node in bitmap is the left child.

                        read = indexFile.Read(block);
                    }
                    else
                    {
                        // There is no left child.

                        break;
                    }
                }
                else
                {
                    if ((best == null && angle > best.Score) || angle > best.Score)
                    {
                        best.Score = angle;
                        best.Node = new VectorNode(postingsOffset);
                    }
                    else if (angle > 0 && angle == best.Score)
                    {
                        best.Node.PostingsOffsets.Add(postingsOffset);
                    }

                    // We need to determine if we can traverse further to the right.

                    if (cursorTerminator == 0)
                    {
                        // There exists a left and a right child.
                        // Next node in bitmap is the left child. 
                        // To find cursor's right child we must skip over the left tree.

                        SkipTree(indexFile);

                        read = indexFile.Read(block);
                    }
                    else if (cursorTerminator == 2)
                    {
                        // Next node in bitmap is the right child,
                        // which is good because we want to go right.

                        read = indexFile.Read(block);
                    }
                    else
                    {
                        // There is no right child.

                        break;
                    }
                }
            }

            return best;
        }

        private void SkipTree(Stream indexStream)
        {
            Span<byte> buf = stackalloc byte[VectorNode.BlockSize];
            var read = indexStream.Read(buf);
            var weight = BitConverter.ToInt64(buf.Slice(sizeof(long) * 3));
            var distance = weight * VectorNode.BlockSize;

            if (distance > 0)
            {
                indexStream.Seek(distance, SeekOrigin.Current);
            }
        }

        public void Dispose()
        {
        }
    }
}
