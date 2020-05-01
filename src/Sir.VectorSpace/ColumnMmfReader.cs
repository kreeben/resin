using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading.Tasks;

namespace Sir.VectorSpace
{
    /// <summary>
    /// Index bitmap reader. Each block is a <see cref="Sir.Search.VectorNode"/>.
    /// </summary>
    public class ColumnMmfReader : IColumnReader
    {
        private readonly ISessionFactory _sessionFactory;
        private readonly ILogger _logger;
        private readonly ulong _collectionId;
        private readonly Stream _vectorFile;
        private readonly MemoryMappedFile _ixFile;
        private readonly IList<(long offset, long length)> _pages;

        public ColumnMmfReader(
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
            _ixFile = _sessionFactory.OpenMMF(ixFileName);
            _pages = GetAllPages(Path.Combine(_sessionFactory.Dir, $"{_collectionId}.{keyId}.ixtp"));
        }

        public Hit ClosestMatch(IVector vector, IStringModel model)
        {
            var time = Stopwatch.StartNew();
            var hits = new List<Hit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset, page.length);

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

        private Hit ClosestMatchInPage(
        IVector vector, IStringModel model, long pageOffset, long pageLength)
        {
            var time = Stopwatch.StartNew();

            using (var indexView = _ixFile.CreateViewAccessor(pageOffset, pageLength))
            {
                _logger.LogInformation($"creating index view took {time.Elapsed}");

                var hit = ClosestMatchInSegment(
                                    vector,
                                    indexView,
                                    _vectorFile,
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
            MemoryMappedViewAccessor indexView,
            Stream vectorFile,
            IModel model)
        {
            var best = new Hit();
            long viewPosition = 0;
            var block = new long[VectorNode.BlockSize/sizeof(long)];

            var read = indexView.ReadArray(viewPosition, block, 0, block.Length);
            viewPosition += VectorNode.BlockSize;

            while (read > 0)
            {
                var vecOffset = block[0];
                var postingsOffset = block[1];
                var componentCount = block[2];
                var cursorTerminator = block[4];
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

                        read = indexView.ReadArray(viewPosition, block, 0, block.Length);
                        viewPosition += VectorNode.BlockSize;
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

                        SkipTree(indexView, ref viewPosition);

                        read = indexView.ReadArray(viewPosition, block, 0, block.Length);
                        viewPosition += VectorNode.BlockSize;
                    }
                    else if (cursorTerminator == 2)
                    {
                        // Next node in bitmap is the right child,
                        // which is good because we want to go right.

                        read = indexView.ReadArray(viewPosition, block, 0, block.Length);
                        viewPosition += VectorNode.BlockSize;
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

        private void SkipTree(MemoryMappedViewAccessor indexView, ref long offset)
        {
            var weight = indexView.ReadInt64(offset + sizeof(long) + sizeof(long) + sizeof(long));

            offset += VectorNode.BlockSize;

            var distance = weight * VectorNode.BlockSize;

            offset += distance;
        }

        private IList<(long offset, long length)> GetAllPages(string pageFileName)
        {
            using (var ixpStream = _sessionFactory.CreateReadStream(pageFileName))
            {
                return new PageIndexReader(ixpStream).GetAll();
            }
        }

        public void Dispose()
        {
        }
    }
}
