using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;

namespace Sir.IO
{
    /// <summary>
    /// Index bitmap reader. Each word is a <see cref="Sir.Strings.VectorNode"/>.
    /// </summary>
    public class ColumnReader : IDisposable
    {
        private readonly Stream _vectorFile;
        private readonly Stream _ixFile;
        private readonly IList<(long offset, long length)> _pages;

        public ColumnReader(
            IList<(long offset, long length)> pages,
            Stream indexStream,
            Stream vectorStream)
        {
            _vectorFile = vectorStream;
            _ixFile = indexStream;
            _pages = pages;
        }

        public Hit ClosestMatchOrNullScanningAllPages(ISerializableVector vector, IModel model)
        {
            if (_ixFile == null || _vectorFile == null)
                return null;

            var hits = new List<Hit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset);

                if (hit != null && hit.Score > 0)
                {
                    hits.Add(hit);
                }
            }

            Hit best = null;

            foreach (var hit in hits)
            {
                if (best == null || hit.Score > best.Score)
                {
                    best = hit;
                    best.PostingsOffsets = new List<long> { hit.Node.PostingsOffset };
                }
                else if (hit.Score.Approximates(best.Score))
                {
                    best.PostingsOffsets.Add(hit.Node.PostingsOffset);
                }
            }

            return best;
        }

        public Hit ClosestMatchOrNullStoppingAtFirstIdenticalPage(ISerializableVector vector, IModel model)
        {
            if (_ixFile == null || _vectorFile == null)
                return null;

            Hit best = null;

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset);

                if (hit != null && hit.Score > 0)
                {
                    if (best == null || hit.Score > best.Score)
                    {
                        best = hit;
                        best.PostingsOffsets = new List<long> { hit.Node.PostingsOffset };
                    }
                    else if (hit.Score.Approximates(best.Score))
                    {
                        best.PostingsOffsets.Add(hit.Node.PostingsOffset);
                    }

                    if (hit.Score.Approximates(model.IdenticalAngle))
                        break;
                }
            }

            return best;
        }

        private Hit ClosestMatchInPage(ISerializableVector queryVector, IModel model, long pageOffset)
        {
            _ixFile.Seek(pageOffset, SeekOrigin.Begin);

            var block = ArrayPool<byte>.Shared.Rent(VectorNode.BlockSize);
            VectorNode bestNode = null;
            double bestScore = 0;
            
            _ixFile.Read(block, 0, VectorNode.BlockSize);

            while (true)
            {
                var vecOffset = BitConverter.ToInt64(block, 0);
                var postingsOffset = BitConverter.ToInt64(block, sizeof(long));
                var componentCount = BitConverter.ToInt64(block, sizeof(long) * 2);
                var terminator = BitConverter.ToInt64(block, sizeof(long) * 4);

                var angle = model.CosAngle(queryVector, vecOffset, (int)componentCount, _vectorFile);

                if (angle >= model.IdenticalAngle || angle.Approximates(model.IdenticalAngle))
                {
                    bestScore = angle;
                    bestNode = new VectorNode(postingsOffset: postingsOffset);

                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (bestNode == null || angle > bestScore)
                    {
                        bestScore = angle;
                        bestNode = new VectorNode(postingsOffset: postingsOffset);
                    }
                    else if (angle.Approximates(bestScore))
                    {
                        bestNode.PostingsOffset = postingsOffset;
                    }

                    // We need to determine if we can traverse further left.
                    bool canGoLeft = terminator == 0 || terminator == 1;

                    if (canGoLeft)
                    {
                        // There exists either a left and a right child or just a left child.
                        // Either way, we want to go left and the next node in bitmap is the left child.

                        _ixFile.Read(block, 0, VectorNode.BlockSize);
                    }
                    else
                    {
                        // There is no left child.

                        break;
                    }
                }
                else
                {
                    if ((bestNode == null && angle > bestScore) || angle > bestScore)
                    {
                        bestScore = angle;
                        bestNode = new VectorNode(postingsOffset: postingsOffset);
                    }
                    else if (angle > 0 && angle.Approximates(bestScore))
                    {
                        bestNode.PostingsOffset = postingsOffset;
                    }

                    // We need to determine if we can traverse further to the right.

                    if (terminator == 0)
                    {
                        // There exists a left and a right child.
                        // Next node in bitmap is the left child. 
                        // To find cursor's right child we must skip over the left tree.

                        SkipTree();

                        _ixFile.Read(block, 0, VectorNode.BlockSize);
                    }
                    else if (terminator == 2)
                    {
                        // Next node in bitmap is the right child,
                        // which is good because we want to go right.

                        _ixFile.Read(block, 0, VectorNode.BlockSize);
                    }
                    else
                    {
                        // There is no right child.

                        break;
                    }
                }
            }

            ArrayPool<byte>.Shared.Return(block);

            return bestNode == null ? null : new Hit(bestNode, bestScore);
        }

        private void SkipTree()
        {
            var buf = ArrayPool<byte>.Shared.Rent(VectorNode.BlockSize);
            _ixFile.Read(buf, 0, VectorNode.BlockSize);
            var sizeOfTree = BitConverter.ToInt64(buf, sizeof(long) * 3);
            var distance = sizeOfTree * VectorNode.BlockSize;

            ArrayPool<byte>.Shared.Return(buf);

            if (distance > 0)
            {
                _ixFile.Seek(distance, SeekOrigin.Current);
            }
        }

        public void Dispose()
        {
            if (_vectorFile != null)
                _vectorFile.Dispose();

            if (_ixFile != null)
                _ixFile.Dispose();
        }
    }
}
