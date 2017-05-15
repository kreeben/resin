using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using log4net;
using Resin.Analysis;
using Resin.IO;
using Resin.Sys;
using System.Diagnostics;
using System.Threading;

namespace Resin
{
    public abstract class CascadingUpsertOperation
    {
        protected abstract IEnumerable<Document> ReadSource();

        protected static readonly ILog Log = LogManager.GetLogger(typeof(CascadingUpsertOperation));

        protected readonly Dictionary<ulong, object> Pks;

        private readonly string _directory;
        private readonly IAnalyzer _analyzer;
        private readonly Compression _compression;
        private readonly long _indexVersionId;
        private readonly bool _autoGeneratePk;
        private readonly string _primaryKey;

        protected CascadingUpsertOperation(
            string directory, IAnalyzer analyzer, Compression compression, string primaryKey)
        {
            _directory = directory;
            _analyzer = analyzer;
            _compression = compression;
            _indexVersionId = Util.GetChronologicalFileId();
            _autoGeneratePk = string.IsNullOrWhiteSpace(primaryKey);
            _primaryKey = primaryKey;

            Pks = new Dictionary<UInt64, object>();
        }

        public long Commit()
        {
            Log.Info("reading documents");

            var readTimer = new Stopwatch();
            readTimer.Start();

            var count = 0;

            foreach(var doc in ReadSourceAndAssignHash())
            {
                new SingleDocumentUpsertOperation(_directory, _analyzer, _compression, _primaryKey, doc)
                    .Commit();

                count++;
            }
            //Parallel.ForEach(ReadSourceAndAssignHash(), doc =>
            //{
            //    new SingleDocumentUpsertOperation(_directory, _analyzer, _compression, _primaryKey, doc)
            //        .Commit();

            //    Interlocked.Increment(ref count);
            //});

            Log.InfoFormat("wrote {0} documents in {1}", count, readTimer.Elapsed);

            //CreateIxInfo().Serialize(Path.Combine(_directory, _indexVersionId + ".ix"));

            //if (_compression > 0)
            //{
            //    Log.Info("compression: true");
            //}
            //else
            //{
            //    Log.Info("compression: false");
            //}

            return _indexVersionId;
        }

        private IEnumerable<Document> ReadSourceAndAssignHash()
        {
            foreach (var document in ReadSource())
            {
                string pkVal;

                if (_autoGeneratePk)
                {
                    pkVal = Guid.NewGuid().ToString();
                }
                else
                {
                    pkVal = document.Fields[_primaryKey].Value;
                }

                var hash = pkVal.ToHash();

                if (Pks.ContainsKey(hash))
                {
                    Log.WarnFormat("Found multiple occurrences of documents with pk value of {0} (id:{1}). Only first occurrence will be stored.",
                        pkVal, document.Id);
                }
                else
                {
                    Pks.Add(hash, null);

                    document.Hash = hash;

                    yield return document;
                }
            }
        }

        private void SerializeTries(IDictionary<string, LcrsTrie> tries)
        {
            Parallel.ForEach(tries, t => DoSerializeTrie(new Tuple<string, LcrsTrie>(t.Key, t.Value)));
        }

        private void DoSerializeTrie(Tuple<string, LcrsTrie> trieEntry)
        {
            var key = trieEntry.Item1;
            var trie = trieEntry.Item2;
            var fileName = Path.Combine(_directory, string.Format("{0}-{1}.tri", _indexVersionId, key));

            trie.Serialize(fileName);
        }

        private IxInfo CreateIxInfo()
        {
            return new IxInfo
            {
                VersionId = _indexVersionId,
                DocumentCount = Pks.Count,
                Compression = _compression
            };
        }
    }
}