using MathNet.Numerics.LinearAlgebra;
using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

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
        private readonly Vector<float> _meanVector;

        public ColumnReader(
            IList<(long offset, long length)> pages,
            Stream indexStream,
            Stream vectorStream,
            Vector<float> meanVector)
        {
            _vectorFile = vectorStream;
            _ixFile = indexStream;
            _pages = pages;
            _meanVector = meanVector;
        }

        public VectorInfoHit ClosestMatchScanningAllPages(ISerializableVector vector, IModel model)
        {
            if (_ixFile == null || _vectorFile == null)
                return null;

            var hits = new List<VectorInfoHit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset, Convert.ToInt32(page.length));

                if (hit.Score > 0)
                {
                    hits.Add(hit);
                }
            }

            VectorInfoHit best = null;

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

        public Hit ClosestMatchOrNullScanningAllPages(ISerializableVector vector, IModel model)
        {
            if (_ixFile == null || _vectorFile == null)
                return null;

            var hits = new List<Hit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset);

                if (hit.Score > 0)
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

        private VectorInfoHit ClosestMatchInPage(ISerializableVector termVector, IModel model, long pageOffset, int pageLength)
        {
            _ixFile.Seek(pageOffset, SeekOrigin.Begin);

            double angle;
            long vectorOffset;
            long postingsOffset;
            int componentCount;
            var listLen = pageLength / VectorRecord.BlockSize;
            VectorRecord[] list = ArrayPool<VectorRecord>.Shared.Rent(listLen);
            byte[] buf = ArrayPool<byte>.Shared.Rent(VectorRecord.BlockSize);

            for (int i = 0; i < listLen; i++)
            {
                _ixFile.Read(buf, 0, VectorRecord.BlockSize);

                angle = BitConverter.ToDouble(buf, 0);
                vectorOffset = BitConverter.ToInt64(buf, sizeof(double));
                postingsOffset = BitConverter.ToInt64(buf, sizeof(double) + sizeof(long));
                componentCount = BitConverter.ToInt32(buf, sizeof(double) + sizeof(long) + sizeof(long));

                list[i] = new VectorRecord(angle, vectorOffset, componentCount, postingsOffset);
            }

            ArrayPool<byte>.Shared.Return(buf);

            var termAngle = _meanVector.CosAngle(termVector.Value);
            var records = new Span<VectorRecord>(list, 0, listLen);
            var index = records.BinarySearch(new VectorRecord(termAngle, 0, 0, 0));
            
            if (index < 0)
            {
                index = ~index;
            }

            var hit = list[index];

            ArrayPool<VectorRecord>.Shared.Return(list);

            double score = 1 - Math.Abs(termAngle - hit.Angle);

            return new VectorInfoHit(new VectorInfo { Angle = hit.Angle, VectorOffset = hit.VectorOffset, PostingsOffset = hit.PostingsOffset, ComponentCount = hit.ComponentCount }, score);
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

                if (angle >= model.IdenticalAngle)
                {
                    bestScore = angle;
                    bestNode = new VectorNode(postingsOffset);

                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (bestNode == null || angle > bestScore)
                    {
                        bestScore = angle;
                        bestNode = new VectorNode(postingsOffset);
                    }
                    else if (angle == bestScore)
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
                        bestNode = new VectorNode(postingsOffset);
                    }
                    else if (angle > 0 && angle == bestScore)
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

            return new Hit(bestNode, bestScore);
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
