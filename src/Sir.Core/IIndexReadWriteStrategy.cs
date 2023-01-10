using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir
{
    public interface IIndexReadWriteStrategy
    {
        void Put<T>(VectorNode column, VectorNode node);
        Hit GetMatchOrNull(ISerializableVector vector, IModel model, ColumnReader reader);
        VectorInfoHit GetMatch(ISerializableVector vector, IModel model, ColumnReader reader);

        void Commit(string directory, ulong collectionId, long keyId, VectorNode tree, IStreamDispatcher streamDispatcher, ILogger logger = null);
        void Commit(string directory, ulong collectionId, long keyId, SortedList<double, VectorInfo> tree, IStreamDispatcher streamDispatcher, ILogger logger = null);
    }

    [DebuggerDisplay("{Angle}")]
    public class VectorInfo
    {
        public double Angle { get; set; }
        public long VectorOffset { get; set; }
        public int ComponentCount { get; set; }
        public long PostingsOffset { get; set; }

        public HashSet<long> DocIds { get; set; }
    }

    [DebuggerDisplay("{Angle}")]
    public struct VectorRecord : IComparable<VectorRecord>
    {
        public const int BlockSize = sizeof(double) + sizeof(long) + sizeof(int) + sizeof(long);

        public double Angle { get; }
        public long VectorOffset { get; }
        public int ComponentCount { get; }
        public long PostingsOffset { get; }

        public VectorRecord(double angle, long vectorOffset, int componentCount, long postingsOffset)
        {
            Angle = angle;
            VectorOffset = vectorOffset;
            ComponentCount = componentCount;
            PostingsOffset = postingsOffset;
        }

        public int CompareTo(VectorRecord other)
        {
            return Angle.CompareTo(other.Angle);
        }
    }
}
