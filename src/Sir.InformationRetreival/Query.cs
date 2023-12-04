
using System.Collections.Generic;

namespace Sir
{
    /// <summary>
    /// A boolean query.
    /// </summary>
    /// <example>
    /// {
    ///	        "or":{
    ///		        "collection":"wikipedia",
    ///		        "title":"ferriman gallwey score"
    ///     }
    /// }
    /// </example>
    public class Query : BooleanStatement, IQuery
    {
        public IList<Term> Terms { get; }
        public HashSet<string> Select { get; }
        public IQuery AndQuery { get; set; }
        public IQuery OrQuery { get; set; }
        public IQuery NotQuery { get; set; }

        public Query(
            IList<Term> terms,
            IEnumerable<string> select,
            bool and,
            bool or,
            bool not) : base(and, or, not)
        {
            Terms = terms;
            Select = new HashSet<string>(select);
        }

        public int TotalNumberOfTerms()
        {
            var count = Terms.Count;

            if (AndQuery != null)
            {
                count += AndQuery.TotalNumberOfTerms();
            }
            if (OrQuery != null)
            {
                count += OrQuery.TotalNumberOfTerms();
            }
            if (NotQuery != null)
            {
                count += NotQuery.TotalNumberOfTerms();
            }

            return count;
        }

        public int GetCollectionCount()
        {
            var dic = new HashSet<ulong>();

            GetNumOfCollections(dic);

            return dic.Count;
        }

        public void GetNumOfCollections(HashSet<ulong> dic)
        {
            foreach (var term in Terms)
            {
                dic.Add(term.CollectionId);
            }

            if (AndQuery != null)
            {
                AndQuery.GetNumOfCollections(dic);
            }
            if (OrQuery != null)
            {
                OrQuery.GetNumOfCollections(dic);
            }
            if (NotQuery != null)
            {
                NotQuery.GetNumOfCollections(dic);
            }
        }

        public IEnumerable<Term> AllTerms()
        {
            foreach (var q in All())
                foreach (var term in q.Terms)
                    yield return term;
        }

        public IEnumerable<IQuery> All()
        {
            yield return this;

            if (AndQuery != null)
            {
                foreach (var q in AndQuery.All())
                    yield return q;
            }
            if (OrQuery != null)
            {
                foreach (var q in OrQuery.All())
                    yield return q;
            }
            if (NotQuery != null)
            {
                foreach (var q in NotQuery.All())
                    yield return q;
            }
        }
    }

    public abstract class BooleanStatement
    {
        private bool _and;
        private bool _or;
        private bool _not;

        public bool IsIntersection
        {
            get { return _and; }
            set
            {
                _and = value;

                if (value)
                {
                    IsUnion = false;
                    IsSubtraction = false;
                }
            }
        }
        public bool IsUnion
        {
            get { return _or; }
            set
            {
                _or = value;

                if (value)
                {
                    IsIntersection = false;
                    IsSubtraction = false;
                }
            }
        }
        public bool IsSubtraction
        {
            get { return _not; }
            set
            {
                _not = value;

                if (value)
                {
                    IsIntersection = false;
                    IsUnion = false;
                }
            }
        }

        public BooleanStatement(bool and, bool or, bool not)
        {
            _and = and;
            _or = or;
            _not = not;
        }
    }
}