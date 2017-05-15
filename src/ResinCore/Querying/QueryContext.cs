using System.Collections.Generic;
using System.Linq;
using System.Text;
using Resin.IO;

namespace Resin.Querying
{
    public class QueryContext : SubQuery
    {
        public IEnumerable<Term> Terms { get; set; }
        public IEnumerable<DocumentPosting> Postings { get; set; }
        public IEnumerable<DocumentScore> Scored { get; set; }

        private IList<QueryContext> _queries;
 
        public QueryContext(string field, string value) : base(field, value)
        {
        }

        public IList<QueryContext> ToList()
        {
            return YieldAll().ToList();
        }

        private IEnumerable<QueryContext> YieldAll()
        {
            yield return this;

            if (_queries == null) yield break;

            foreach (var q in _queries)
            {
                foreach (var sq in q.YieldAll()) yield return sq;
            }
        } 

        public IEnumerable<DocumentScore> Reduce()
        {
            var first = Scored.ToList();

            if (_queries != null)
            {
                foreach (var child in _queries)
                {
                    var other = child.Reduce().ToList();

                    if (child.And)
                    {
                        first = DocumentScore.CombineAnd(first, other).ToList();
                    }
                    else if (child.Not)
                    {
                        first = DocumentScore.Not(first, other).ToList();
                    }
                    else // Or
                    {
                        first = DocumentScore.CombineOr(first, other).ToList();
                    }
                } 
            }

            return first;
        }

        public void Add(QueryContext queryContext)
        {
            if (_queries == null) _queries = new List<QueryContext>();

            _queries.Add(queryContext);
        }

        public override string ToString()
        {
            var log = new StringBuilder();

            log.Append(base.ToString());

            if (_queries != null)
            {
                log.Append(' ');

                var entries = new List<string>();

                foreach (var q in _queries)
                {
                    entries.Add(q.ToString());
                    entries.Add(" ");
                }

                foreach (var e in entries.Take(entries.Count-1))
                {
                    log.Append(e);
                }
            }
            
            return log.ToString();
        }
    }
}