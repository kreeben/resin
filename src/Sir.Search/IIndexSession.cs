﻿using System.Collections.Generic;

namespace Sir
{
    public interface IIndexSession<T> : IIndexSession
    {
        void Put(long docId, long keyId, T value, bool label);
    }

    public interface IIndexSession
    {
        IndexInfo GetIndexInfo();
        void Put(long docId, long keyId, IEnumerable<ISerializableVector> tokens);
        void Put(VectorNode documentTree);
        void Commit(IndexWriter indexWriter);
        IDictionary<long, VectorNode> GetInMemoryIndices();
    }
}