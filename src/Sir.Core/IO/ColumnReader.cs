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
        private readonly MemoryMappedFile _anglesFile;
        private readonly IList<(long offset, long length)> _pages;

        public ColumnReader(
            IList<(long offset, long length)> pages,
            MemoryMappedFile ixFile,
            MemoryMappedFile anglesFile,
            Stream vectorStream)
        {
            _vectorFile = vectorStream;
            _ixFile = ixFile;
            _anglesFile = anglesFile;
            _pages = pages;
        }

        public Hit ClosestMatchOrNullScanningAllPages(ISerializableVector vector, IModel model)
        {
            var hits = new List<Hit>();

            foreach (var page in _pages)
            {
                var hit = ClosestMatchInPage(vector, model, page.offset, page.length);

                if (hit != null)
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
                else if (hit.Score.Approximates(best.Score, 0.000001))
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

                if (hit != null)
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
            const long angleBlockSize = sizeof(double) + sizeof(long);
            var nodeBuf = ArrayPool<long>.Shared.Rent(4);

            using (var indexView = _ixFile.CreateViewAccessor(pageOffset, pageLength))
            {
                long bytesRead = 0;
                double angleOnRecord = 0;
                long postingsOffset = -1;

                while (bytesRead < pageLength)
                {
                    indexView.ReadArray(bytesRead, nodeBuf, 0, 4);
                    bytesRead += VectorNode.BlockSize;

                    var vectorOffset = nodeBuf[0];
                    var vectorComponentCount = nodeBuf[1];
                    var anglesCount = nodeBuf[2];
                    var anglesOffset = nodeBuf[3];

                    var queryAngle = Math.Round(model.CosAngle(queryVector, vectorOffset, (int)vectorComponentCount, _vectorFile), 15);

                    if (queryAngle > 0)
                    {
                        double low = 0;
                        double high = anglesCount - 1;
                        double mid;

                        using (var anglesView = _anglesFile.CreateViewAccessor(anglesOffset, anglesCount * angleBlockSize))
                        {
                            var first = anglesView.ReadDouble(0);
                            var last = anglesView.ReadDouble((anglesCount - 1) * angleBlockSize);

                            if (queryAngle < first)
                            {
                                angleOnRecord = first;
                                postingsOffset = anglesView.ReadInt64(sizeof(double));
                            }
                            else if (queryAngle > last)
                            {
                                angleOnRecord = last;
                                postingsOffset = anglesView.ReadInt64(((anglesCount - 1) * angleBlockSize) + sizeof(double));
                            }
                            else
                            {
                                while (low <= high)
                                {
                                    mid = Math.Ceiling((high + low) / 2);

                                    var pos = (long)mid * angleBlockSize;
                                    angleOnRecord = Math.Round(anglesView.ReadDouble(pos), 15);
                                    postingsOffset = anglesView.ReadInt64(pos + sizeof(double));

                                    if (angleOnRecord.Approximates(queryAngle, 0.000000001))
                                    {
                                        break;
                                    }
                                    else if (angleOnRecord > queryAngle)
                                    {
                                        high = mid - 1;
                                    }
                                    else
                                    {
                                        low = mid + 1;
                                    }
                                }
                            }
                        }

                        if (angleOnRecord > 0)
                        {
                            if (postingsOffset == -1)
                                throw new Exception("invalid postings offset");

                            var score = 1 - Math.Abs(queryAngle - angleOnRecord);
                            return new Hit(new VectorNode(postingsOffset: postingsOffset), score);
                        }
                    }
                }
            }

            ArrayPool<long>.Shared.Return(nodeBuf);

            return null;
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
