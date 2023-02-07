using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Sir.IO
{
    /// <summary>
    /// Index bitmap reader. Each word is a <see cref="Sir.Strings.VectorNode"/>.
    /// </summary>
    public class ColumnReader : IDisposable
    {
        private readonly Stream _vectorFile;
        private readonly MemoryMappedFile _ixFile;
        private readonly IList<(long offset, long length)> _pages;

        public ColumnReader(
            IList<(long offset, long length)> pages,
            MemoryMappedFile ixFile,
            Stream vectorStream)
        {
            _vectorFile = vectorStream;
            _ixFile = ixFile;
            _pages = pages;
        }

        public Hit ClosestMatchOrNullScanningAllPages(ISerializableVector vector, IModel model)
        {
            var hits = new List<Hit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset, page.length);

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

        public Hit ClosestMatchOrNullStoppingAtFirstIdenticalPage(ISerializableVector vector, IModel model)
        {
            Hit best = null;

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset, page.length);

                if (hit.Score > 0)
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

                if (hit.Score >= model.IdenticalAngle)
                    break;
            }

            return best;
        }

        private Hit ClosestMatchInPage(ISerializableVector queryVector, IModel model, long pageOffset, long pageLength)
        {
            using (var view = _ixFile.CreateViewAccessor(pageOffset, pageLength))
            {
                var headerBuf = ArrayPool<long>.Shared.Rent(VectorNode.BlockSize);

                view.ReadArray<long>(0, headerBuf, 0, 3);

                var vectorOffset = headerBuf[0];
                var vectorComponentCount = headerBuf[1];
                var anglesCount = headerBuf[2];

                ArrayPool<long>.Shared.Return(headerBuf);

                var queryAngle = model.CosAngle(queryVector, vectorOffset, (int)vectorComponentCount, _vectorFile);
                double angle = 0;
                long windowOffset = VectorNode.BlockSize;
                long windowLength = anglesCount * sizeof(double);
                long indexOfWinner = anglesCount / 2;

                while (windowLength > 3)
                {
                    windowLength /= 2;
                    var positionOfWinner = windowOffset + windowLength;
                    indexOfWinner = positionOfWinner / sizeof(double);
                    angle = view.ReadDouble(positionOfWinner);

                    if (angle == queryAngle)
                    {
                        break;
                    }
                    else if (angle > queryAngle)
                    {
                        windowOffset -= windowLength / 2;
                    }
                    else
                    {
                        windowOffset += windowLength / 2;
                    }
                }

                var postingsOffset = view.ReadInt64(VectorNode.BlockSize + (anglesCount * sizeof(double)) + (indexOfWinner * sizeof(long)));

                return new Hit(new VectorNode(postingsOffset: postingsOffset), angle);
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
