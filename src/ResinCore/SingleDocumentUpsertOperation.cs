using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using log4net;
using Resin.Analysis;
using Resin.IO;
using Resin.IO.Write;
using Resin.Sys;

namespace Resin
{
    public class SingleDocumentUpsertOperation
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(SingleDocumentUpsertOperation));

        private readonly ulong _primaryKeyHash;
        private readonly string _directory;
        private readonly IAnalyzer _analyzer;
        private readonly Compression _compression;
        private readonly Document _document;

        public SingleDocumentUpsertOperation(
            string directory, IAnalyzer analyzer, Compression compression, string primaryKey, Document document)
        {
            if (document.Hash == ulong.MinValue) throw new ArgumentException("document hash not set", "document");

            var autoGeneratePk = string.IsNullOrWhiteSpace(primaryKey);

            _directory = directory;
            _analyzer = analyzer;
            _compression = compression;
            _document = document;
            _primaryKeyHash = autoGeneratePk ? 
                Guid.NewGuid().ToString().ToHash() : 
                document.Fields[primaryKey].Value.ToHash();
        }
        
        public long Commit()
        {
            var trieBuilder = new TrieBuilder();
            var docAddresses = new List<BlockInfo>();

            var analyzed = _analyzer.AnalyzeDocument(_document);

            foreach (var term in analyzed.Words.GroupBy(t => t.Term.Field))
            {
                trieBuilder.Add(term.Key, term.Select(t =>
                {
                    var field = t.Term.Field;
                    var token = t.Term.Word.Value;
                    var posting = t.Posting;
                    return new WordInfo(field, token, posting);
                }).ToList());
            }

            trieBuilder.CompleteAdding();

            var indexVersionId = Util.GetChronologicalFileId();
            var docFileName = Path.Combine(_directory, indexVersionId + ".rdoc");
            var docAddressesFn = Path.Combine(_directory, indexVersionId + ".da");

            BlockInfo adr;

            using (var docWriter = new DocumentWriter(
                new FileStream(docFileName, FileMode.Create, FileAccess.Write, FileShare.None, 4096, true), _compression))
            {
                adr = docWriter.Write(_document);                
            }

            using (var docAddressWriter = new DocumentAddressWriter(
                    new FileStream(docAddressesFn, FileMode.Create, FileAccess.Write, FileShare.None)))
            {
                docAddressWriter.Write(adr);
            }

            var tries = trieBuilder.GetTries();

            var tasks = new List<Task>
                {
                    Task.Run(() =>
                    {
                        var posFileName = Path.Combine(_directory, string.Format("{0}.{1}", indexVersionId, "pos"));
                        using (var postingsWriter = new PostingsWriter(new FileStream(posFileName, FileMode.Create, FileAccess.Write, FileShare.None)))
                        {
                            foreach (var trie in tries)
                            {
                                foreach (var node in trie.Value.EndOfWordNodes())
                                {
                                    node.PostingsAddress = postingsWriter.Write(node.Postings);
                                }

                                if (Log.IsDebugEnabled)
                                {
                                    foreach(var word in trie.Value.Words())
                                    {
                                        Log.Debug(word);
                                    }
                                }
                            }
                        }
                    }),
                    Task.Run(() =>
                    {
                        SerializeTries(tries, indexVersionId);
                    }),
                    Task.Run(() =>
                    {
                        var docHashesFileName = Path.Combine(_directory, string.Format("{0}.{1}", indexVersionId, "pk"));

                        new DocHash[]{new DocHash(_primaryKeyHash)}.Serialize(docHashesFileName);
                    })
                };

            Task.WaitAll(tasks.ToArray());

            new IxInfo
            {
                VersionId = indexVersionId,
                DocumentCount = 1,
                Compression = _compression
            }.Serialize(Path.Combine(_directory, indexVersionId + ".ix"));

            if (_compression > 0)
            {
                Log.Info("compression: true");
            }
            else
            {
                Log.Info("compression: false");
            }

            return indexVersionId;
        }

        private void SerializeTries(IDictionary<string, LcrsTrie> tries, long indexVersionId)
        {
            Parallel.ForEach(tries, t => DoSerializeTrie(new Tuple<string, LcrsTrie>(t.Key, t.Value), indexVersionId));
        }

        private void DoSerializeTrie(Tuple<string, LcrsTrie> trieEntry, long indexVersionId)
        {
            var key = trieEntry.Item1;
            var trie = trieEntry.Item2;
            var fileName = Path.Combine(_directory, string.Format("{0}-{1}.tri", indexVersionId, key));

            trie.Serialize(fileName);
        }
    }
}