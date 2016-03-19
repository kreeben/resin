using System.Collections.Generic;
using System.Linq;

namespace Resin
{
    public class QueryParser
    {
        private readonly IAnalyzer _analyzer;

        public QueryParser(IAnalyzer analyzer)
        {
            _analyzer = analyzer;
        }

        public IEnumerable<Term> Parse(string query)
        {
            var q = query.Split(' ').Select(t => t.Split(':'));
            foreach (var t in q)
            {
                var field = t[0];
                var value = t[1];
                foreach (var token in _analyzer.Analyze(value))
                {
                    yield return new Term { Field = field, Token = token}; 
                }
            }
        }
    }
}