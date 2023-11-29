using MathNet.Numerics.LinearAlgebra;
using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.IO
{
    /// <summary>
    /// Index bitmap reader. Each word is a <see cref="Sir.Strings.VectorNode"/>.
    /// </summary>
    public class ColumnReader : IDisposable
    {
        private readonly Stream _vectorStream;
        private readonly Stream _ixFile;
        private readonly IList<(long offset, long length)> _pages;
        private readonly IDictionary<long, ISerializableVector> _vectors;
        private readonly bool _keepOpen;
        private readonly ILogger _logger;
        private readonly IModel _model;

        public ColumnReader(
            IList<(long offset, long length)> pages,
            Stream indexStream,
            Stream vectorStream,
            IModel model,
            ILogger logger = null,
            bool keepOpen = false)
        {
            _vectorStream = vectorStream;
            _ixFile = indexStream;
            _pages = pages;
            _model = model;
            _logger = logger;
            _vectors = LoadVectors();
            _keepOpen = keepOpen;
        }

        private IDictionary<long, ISerializableVector> LoadVectors()
        {
            var time = Stopwatch.StartNew();
            var vectors = new Dictionary<long, ISerializableVector>();
            var block = ArrayPool<byte>.Shared.Rent(VectorNode.BlockSize);
            var read = _ixFile.Read(block, 0, VectorNode.BlockSize);

            while (read > 0)
            {
                var vecOffset = BitConverter.ToInt64(block, 0);
                var componentCount = BitConverter.ToInt64(block, sizeof(long) * 2);
                var vector = DeserializeVector(vecOffset, (int)componentCount, _model.NumOfDimensions);

                vectors.Add(vecOffset, vector);
                read = _ixFile.Read(block, 0, VectorNode.BlockSize);
            }

            ArrayPool<byte>.Shared.Return(block);

            if (_logger != null)
            {
                _logger.LogInformation($"loaded {vectors.Count} vectors into memory in {time.Elapsed}");
            }

            return vectors;
        }

        private ISerializableVector DeserializeVector(long vectorOffset, int componentCount, int numOfDimensions)
        {
            var bufSize = componentCount * 2 * sizeof(int);
            var rent = ArrayPool<byte>.Shared.Rent(bufSize);
            Span<byte> buf = new Span<byte>(rent).Slice(0, bufSize);

            if (_vectorStream.Position != vectorOffset)
            {
                _vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
            }

            _vectorStream.Read(buf);

            var index = MemoryMarshal.Cast<byte, int>(buf.Slice(0, componentCount * sizeof(int)));
            var values = MemoryMarshal.Cast<byte, float>(buf.Slice(componentCount * sizeof(int), componentCount * sizeof(float)));

            ArrayPool<byte>.Shared.Return(rent);

            var tuples = ArrayPool<Tuple<int, float>>.Shared.Rent(componentCount);

            for (int i = 0; i < componentCount; i++)
            {
                tuples[i] = new Tuple<int, float>(index[i], values[i]);
            }

            var vectorOnFile = CreateVector.SparseOfIndexed(numOfDimensions, new ArraySegment<Tuple<int, float>>(tuples, 0, componentCount));

            ArrayPool<Tuple<int, float>>.Shared.Return(tuples);

            return new SerializableVector(vectorOnFile);
        }

        public Hit ClosestMatchOrNullScanningAllPages(ISerializableVector vector)
        {
            if (_ixFile == null || _vectorStream == null)
                return null;

            var hits = new List<Hit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, page.offset);

                if (hit.Score > 0 && hit.Node.PostingsPageId > -1)
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
                    best.PostingsPageIds = new List<long> { hit.Node.PostingsPageId };
                }
                else if (hit.Score.Approximates(best.Score))
                {
                    best.PostingsPageIds.Add(hit.Node.PostingsPageId);
                }
            }

            return best;
        }

        public Hit ClosestMatchOrNullStoppingAtFirstIdenticalPage(ISerializableVector vector)
        {
            if (_ixFile == null || _vectorStream == null)
                return null;

            Hit best = null;

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, page.offset);

                if (hit.Score > 0 && hit.Node.PostingsPageId > -1)
                {
                    if (best == null || hit.Score > best.Score)
                    {
                        best = hit;
                        best.PostingsPageIds = new List<long> { hit.Node.PostingsPageId };
                    }
                    else if (hit.Score.Approximates(best.Score))
                    {
                        best.PostingsPageIds.Add(hit.Node.PostingsPageId);
                    }
                }

                if (hit.Score >= _model.IdenticalAngle)
                    break;
            }

            return best;
        }

        private Hit ClosestMatchInPage(ISerializableVector queryVector, long pageOffset)
        {
            _ixFile.Seek(pageOffset, SeekOrigin.Begin);

            var block = ArrayPool<byte>.Shared.Rent(VectorNode.BlockSize);
            VectorNode bestNode = null;
            double bestScore = 0;
            
            _ixFile.Read(block, 0, VectorNode.BlockSize);

            while (true)
            {
                var vecOffset = BitConverter.ToInt64(block, 0);
                var pageId = BitConverter.ToInt64(block, sizeof(long));
                var terminator = BitConverter.ToInt64(block, sizeof(long) * 4);

                var vector = _vectors[vecOffset];
                var angle = _model.CosAngle(queryVector, vector);

                if (angle >= _model.IdenticalAngle)
                {
                    bestScore = angle;
                    bestNode = new VectorNode(postingsPageId: pageId);

                    break;
                }
                else if (angle > _model.FoldAngle)
                {
                    if (bestNode == null || angle > bestScore)
                    {
                        bestScore = angle;
                        bestNode = new VectorNode(postingsPageId: pageId);
                    }
                    else if (angle == bestScore)
                    {
                        bestNode.PostingsPageId = pageId;
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
                        bestNode = new VectorNode(postingsPageId: pageId);
                    }
                    else if (angle > 0 && angle == bestScore)
                    {
                        bestNode.PostingsPageId = pageId;
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
            if (_keepOpen)
                return;

            if (_vectorStream != null)
                _vectorStream.Dispose();

            if (_ixFile != null)
                _ixFile.Dispose();
        }
    }
}
