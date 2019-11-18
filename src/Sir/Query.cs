using System.Collections.Generic;

namespace Sir
{
    /// <summary>
    /// A boolean query,
    /// </summary>
    public class Query : BooleanStatement
    {
        public IList<Term> Terms { get; }
        public Query And { get; set; }
        public Query Or { get; set; }
        public Query Not { get; set; }

        public Query(
            IList<Term> terms, 
            bool and,
            bool or,
            bool not) : base(and, or, not)
        {
            Terms = terms;
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

            GetCollectionCount(dic);

            return dic.Count;
        }

        public void GetCollectionCount(HashSet<ulong> dic)
        {
            foreach (var term in Terms)
            {
                dic.Add(term.CollectionId);
            }

            if (And != null)
            {
                And.GetCollectionCount(dic);
            }
            if (Or != null)
            {
                Or.GetCollectionCount(dic);
            }
            if (Not != null)
            {
                Not.GetCollectionCount(dic);
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
}