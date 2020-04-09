using System.Collections.Generic;

namespace Sir
{
    /// <summary>
    /// A boolean query.
    /// </summary>
    /// <example>
    /// {
    ///	        "or":{
    ///		        "collection":"cc_wat",
    ///		        "title":"prom dresses bride"
    ///     }
    /// }
    /// </example>
    public class Query : BooleanStatement, IQuery
    {
        public IList<Term> Terms { get; }
        public HashSet<string> Select { get; }
        public Query And { get; set; }
        public Query Or { get; set; }
        public Query Not { get; set; }

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

        public int GetDivider()
        {
            var terms = GetTermCount();
            var collections = GetCollectionCount();
            return terms / collections;
        }

        public int GetTermCount()
        {
            var count = Terms.Count;

            if (And != null)
            {
                count += And.GetTermCount();
            }
            if (Or != null)
            {
                count += Or.GetTermCount();
            }
            if (Not != null)
            {
                count += Not.GetTermCount();
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

            if (And != null)
            {
                And.GetNumOfCollections(dic);
            }
            if (Or != null)
            {
                Or.GetNumOfCollections(dic);
            }
            if (Not != null)
            {
                Not.GetNumOfCollections(dic);
            }
        }

        public IEnumerable<Query> All()
        {
            yield return this;

            if (And != null)
            {
                foreach (var q in And.All())
                    yield return q;
            }
            if (Or != null)
            {
                foreach (var q in Or.All())
                    yield return q;
            }
            if (Not != null)
            {
                foreach (var q in Not.All())
                    yield return q;
            }
        }
    }

    [System.Diagnostics.DebuggerDisplay("{Key}:{StringValue}")]
    public class Term : BooleanStatement
    {
        public IVector Vector { get; }
        public long KeyId { get; }
        public string Key { get; }
        public ulong CollectionId { get; }
        public IList<long> PostingsOffsets { get; set; }
        public double Score { get; set; }
        public string StringValue { get { return Vector.Data.ToString(); } }

        public Term(
            ulong collectionId,
            long keyId, 
            string key, 
            IVector vector, 
            bool and, 
            bool or, 
            bool not)
            : base(and, or, not)
        {
            CollectionId = collectionId;
            KeyId = keyId;
            Key = key;
            Vector = vector;
            IsIntersection = and;
            IsUnion = or;
            IsSubtraction = not;
        }
    }

    public class BooleanStatement
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

    /// <summary>
    /// A join query.
    /// </summary>
    /// <example>
    /// {
    ///        "join":"cc_wet,url",
    ///        "query":
    ///        {
    ///	        "or":{
    ///		        "collection":"cc_wat",
    ///		        "title":"red dress"
    ///         }
    ///     }
    /// }
    /// </example>
    //public class Join : IQuery
    //{
    //    public Join(Query query, string collection, string primaryKey)
    //    {
    //        Query = query;
    //        Collection = collection;
    //        PrimaryKey = primaryKey;
    //    }

    //    public string PrimaryKey { get;}
    //    public string Collection { get; }
    //    public Query Query { get; }

    //    public int GetDivider()
    //    {
    //        return Query.GetDivider();
    //    }
    //}

    public interface IQuery
    {
        bool IsIntersection { get; }
        bool IsUnion { get; }
        bool IsSubtraction { get; }
        Query And { get; set; }
        Query Or { get; set; }
        Query Not { get; set; }
        IList<Term> Terms { get; }
        HashSet<string> Select { get; }
        IEnumerable<Query> All();
        int GetDivider();
    }
}