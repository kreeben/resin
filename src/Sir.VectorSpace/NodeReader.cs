using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.VectorSpace
{
    /// <summary>
    /// Index bitmap reader. Each block is a <see cref="Sir.Search.VectorNode"/>.
    /// </summary>
    public class NodeReader : INodeReader
    {
        private readonly ISessionFactory _sessionFactory;
        private readonly ILogger _logger;
        private readonly ulong _collectionId;
        private readonly Stream _vectorFile;
        private readonly Stream _ixFile;
        private readonly IList<(long offset, long length)> _pages;

        public NodeReader(
            ulong collectionId,
            long keyId,
            ISessionFactory sessionFactory,
            ILogger logger)
        {
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
            _logger = logger;

            var vectorFileName = Path.Combine(_sessionFactory.Dir, $"{_collectionId}.vec");
            var ixFileName = Path.Combine(_sessionFactory.Dir, string.Format("{0}.{1}.ix", _collectionId, keyId));

            _vectorFile = _sessionFactory.CreateReadStream(vectorFileName);
            _ixFile = _sessionFactory.CreateReadStream(ixFileName);

            _pages = GetAllPages(
                Path.Combine(_sessionFactory.Dir, $"{_collectionId}.{keyId}.ixtp"));
        }

        public Hit ClosestTerm(IVector vector, IStringModel model, long keyId)
        {
            var time = Stopwatch.StartNew();
            var hits = new ConcurrentBag<Hit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, keyId, page.offset);

                if (hit != null)
                    hits.Add(hit);
            }

            _logger.LogInformation($"scanning all pages took {time.Elapsed}");

            Hit best = null;

            foreach (var hit in hits)
            {
                if (best == null || hit.Score > best.Score)
                {
                    best = hit;
                }
                else if (hit.Score.Approximates(best.Score))
                {
                    GraphBuilder.MergePostings(best.Node, hit.Node);
                }
            }

            return best;
        }

        private IList<(long offset, long length)> GetAllPages(string pageFileName)
        {
            using (var ixpStream = _sessionFactory.CreateReadStream(pageFileName))
            {
                return new PageIndexReader(ixpStream).GetAll();
            }
        }

        private Hit ClosestMatchInPage(
        IVector vector, IStringModel model, long keyId, long pageOffset)
        {
            _ixFile.Seek(pageOffset, SeekOrigin.Begin);

            var hit = ClosestMatchInSegment(
                    vector,
                    _ixFile,
                    _vectorFile,
                    model);

            if (hit.Score > 0)
            {
                return hit;
            }

            return null;
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

                if (angle >= model.IdenticalAngle)
                {
                    best.Score = angle;
                    var n = new VectorNode(postingsOffset);
                    best.Node = n;

                    break;
                }
                else if (angle > model.FoldAngle)
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
            _vectorFile.Dispose();
            _ixFile.Dispose();
        }
    }
}
