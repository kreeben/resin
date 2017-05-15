using System;

namespace Resin.Querying
{
    public class SubQuery : IEquatable<SubQuery>
    {
        private int _edits;

        public string Field { get; protected set; }
        public string Value { get; protected set; }

        public bool And { get; set; }
        public bool Not { get; set; }
        public bool Prefix { get; set; }
        public bool Fuzzy { get; set; }

        public int Edits
        {
            get
            {
                return _edits;
            }
            set
            {
                _edits = value;

                if (Fuzzy && Edits == 0)
                {
                    Fuzzy = false;
                }
            }
        }

        public float Similarity
        {
            set
            {
                _edits = Convert.ToInt32(Math.Floor(Value.Length * (1 - value)));

                if (Fuzzy && Edits == 0)
                {
                    Fuzzy = false;
                }
            }
        }

        public SubQuery(string field, string value)
        {
            Field = field;
            Value = value;
        }

        public override string ToString()
        {
            return AsReadable();
        }

        public string AsReadable()
        {
            var fldPrefix = And ? "+" : Not ? "-" : string.Empty;
            var tokenSuffix = Prefix ? "*" : Fuzzy ? "~" : string.Empty;
            return string.Format("{0}{1}:{2}{3}", fldPrefix, Field, Value, tokenSuffix);
        }

        public bool Equals(SubQuery other)
        {
            if (other == null) return false;

            return other.Field == Field && other.Value == Value;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 23 + Field.GetHashCode();
                hash = hash * 23 + Value.GetHashCode();
                return hash;
            }
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as SubQuery);
        }
    }
}