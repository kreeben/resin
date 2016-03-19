using System;
using System.Collections.Generic;
using System.Linq;

namespace Resin
{
    public class Searcher : IDisposable
    {
        private readonly QueryParser _parser;
        private readonly IndexReader _reader;

        public Searcher(string dir)
        {
            _reader = new IndexReader(new Scanner(dir));
            _parser = new QueryParser(new Analyzer());
        }

        public IEnumerable<Document> Search(string query)
        {
            var terms = _parser.Parse(query).ToList();
            return _reader.GetDocuments(terms); 
        }

        public void Dispose()
        {
            _reader.Dispose();
        }
    }
}