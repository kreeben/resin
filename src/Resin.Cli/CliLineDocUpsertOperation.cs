using System.Collections.Generic;
using System.IO;
using Resin.Analysis;
using System.Linq;
using System;
using Resin.IO;
using Resin.Sys;
using log4net;

namespace Resin.Cli
{
    public class CliLineDocUpsertOperation : StreamUpsertOperation
    {
        private readonly int _take;
        private readonly int _skip;
        private readonly bool _autoGeneratePk;
        private readonly string _primaryKey;

        public CliLineDocUpsertOperation(string directory, IAnalyzer analyzer, int skip, int take, Compression compression, string primaryKey, string fileName)
            : base(directory, analyzer, compression, primaryKey, fileName)
        {
            _take = take;
            _skip = skip;

            _autoGeneratePk = string.IsNullOrWhiteSpace(primaryKey);
            _primaryKey = primaryKey;
        }

        public CliLineDocUpsertOperation(string directory, IAnalyzer analyzer, int skip, int take, Compression compression, string primaryKey, Stream documents)
            : base(directory, analyzer, compression, primaryKey, documents)
        {
            _take = take;
            _skip = skip;
            _autoGeneratePk = string.IsNullOrWhiteSpace(primaryKey);
        }

        protected IList<Field> Parse(string document)
        {
            var parts = document.Split(new[] { '\t' }, System.StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length < 3) return null;

            return new Field[] 
            {
                new Field("doctitle", parts[0]),
                new Field("body", parts[2])
            };
        }

        protected override IEnumerable<Document> ReadSource()
        {
            Reader.ReadLine(); // first row is junk

            if (_skip > 0)
            {
                int skipped = 0;

                while (skipped++ < _skip)
                {
                    Reader.ReadLine();
                }
            }

            return ReadInternal().Take(_take);
        }

        protected virtual IEnumerable<Document> ReadInternal()
        {
            string line;
            while ((line = Reader.ReadLine()) != null)
            {
                var doc = line.Substring(0, line.Length - 1);
                var fields = Parse(doc);
                yield return new Document(fields);
            }
            Console.WriteLine("");
        }
    }
}