using System;
using System.Collections.Generic;
using Resin.Analysis;

namespace Resin.Querying
{
    public class QueryParser : IQueryParser
    {
        private readonly IAnalyzer _analyzer;
        private readonly float _fuzzySimilarity;
        
        // alphabets (this machine's vocabulaly, so to speak):

        // key/value delimiters:
        private static readonly List<char> a0 = new List<char> { ':', '<', '>' };

        // anything that can come immediately after a value:
        // value suffixes
        // value hints (enclosings)
        // key/value pair delimiters
        private static readonly List<char> a1 = new List<char> { '*', '~', '+', '-', ' ', '"', '\'', '\\' };

        // key/value pair delimiters
        private static readonly List<char> a2 = new List<char> { '+', '-', ' ' };

        public QueryParser(float fuzzySimilarity = 0.75f):this(new Analyzer(), fuzzySimilarity) { }

        public QueryParser(IAnalyzer analyzer, float fuzzySimilarity = 0.75f)
        {
            _analyzer = analyzer;
            _fuzzySimilarity = fuzzySimilarity;
        }

        public IList<QueryContext> Parse(string query)
        {
            if (string.IsNullOrWhiteSpace(query)) throw new ArgumentException("query");
            if (query.StartsWith("NOT", StringComparison.OrdinalIgnoreCase)) return null;

            if (query[0] == ' ' || query[0] == '-') throw new ArgumentException("first query must be inclusive (and)");

            int state = 0;
            var queries = new List<QueryContext>();

            // -----------
            // read states
            // -----------
            //
            // state 0: read key until key/value delimiter
            // state 1: read value until either suffix operator or term delimiter
            // state 2: read term delimiter
            // state 3: yield term
            //
            // allowed state transitions: 
            //
            // 0->1
            // 1->2
            // 2->3
            // 3->0
            //
            // state transitions given the following query:
            //
            //  title:'first'~+title:'blood'
            // 0.....1......230.....1.......23
            //
            // phrases are enclosed with double-quotes, terms with single quotes 
            // and dates with backslashes. numbers are not enclosed:
            //
            // title:"john rambo"+genre:'action'+created<\2000-01-01\+rating>3
            //

            var segment = new QuerySegment();
            var not = false;
            var or = false;
            var prevNot = false;
            var prevOr = false;
            bool isPhrase = false;
            bool isTerm = false;
            bool isDate = false;

            Action appendQuery = () =>
            {
                Query q = null;
                var key = new string(segment.Buf0.ToArray());
                var value = new string(segment.Buf1.ToArray());

                if (segment.IsTerm)
                {
                    var values = _analyzer.Analyze(value);

                    if (values.Count == 1)
                    {
                        q = new TermQuery(key, values[0]);
                    }
                    else
                    {
                        q = new PhraseQuery(key, values);
                    }
                }
                else if (segment.IsPhrase)
                {
                    var values = _analyzer.Analyze(value);

                    q = new PhraseQuery(key, values);
                }
                else if (segment.IsDate)
                {
                    q = new TermQuery(key, DateTime.Parse(value));
                }
                else
                {
                    q = new TermQuery(key, long.Parse(value));
                }

                q.GreaterThan = segment.Gt;
                q.LessThan = segment.Lt;
                q.Not = prevNot;
                q.Or = prevOr;
                q.Fuzzy = segment.Fz;
                q.Prefix = segment.Px;

                if (segment.Fz)
                {
                    q.Similarity = _fuzzySimilarity;
                }

                var qc = new QueryContext();
                qc.Query = q;

                Append(queries, qc);

                segment = new QuerySegment();
                prevNot = not;
                prevOr = or;
                state = 0;
            };

            for (int index = 0; index < query.Length; index++)
            {
                var c = query[index];

                if (state == 3)
                {
                    appendQuery();
                }

                if (state == 0)
                {
                    if (a0.Contains(c))
                    {
                        if (c == '<')
                        {
                            segment.Lt = true;
                        }
                        else if (c == '>')
                        {
                            segment.Gt = true;
                        }
                        state = 1;
                    }
                    else
                    {
                        if (c != '+')
                        {
                            segment.Buf0.Add(c);
                        }
                    }
                }
                else if (state == 1)
                {
                    if (!isPhrase && !isTerm && !isDate && a1.Contains(c))
                    {
                        if (c == '-')
                        {
                            not = true;
                            state = 3;
                        }
                        else if (c == '+')
                        {
                            state = 3;
                        }
                        else if (c == ' ')
                        {
                            or = true;
                            state = 3;
                        }
                        else if (c == '*')
                        {
                            segment.Px = true;
                            state = 2;
                        }
                        else if (c == '~')
                        {
                            segment.Fz = true;
                            state = 2;
                        }
                        else if (c == '"')
                        {
                            isPhrase = true;
                            segment.IsPhrase = true;
                        }
                        else if (c == '\'')
                        {
                            isTerm = true;
                            segment.IsTerm = true;
                        }
                        else if (c == '\\')
                        {
                            isDate = true;
                            segment.IsDate = true;
                        }
                    }
                    else if (isPhrase && c == '"')
                    {
                        isPhrase = false;
                    }
                    else if (isTerm && c == '\'')
                    {
                        isTerm = false;
                    }
                    else if (isDate && c == '\\')
                    {
                        isDate = false;
                    }
                    else
                    {
                        segment.Buf1.Add(c);
                    }
                }
                else // state == 2
                {
                    if (a2.Contains(c))
                    {
                        if (c == '-')
                        {
                            not = true;
                        }
                        else if (c == ' ')
                        {
                            or = true;
                        }

                        state = 3;
                    }
                    else
                    {
                        segment.Buf1.Add(c);
                    }
                }
            }

            if (state > 0)
            {
                appendQuery();
            }

            return queries;
        }

        private void Append(IList<QueryContext> list, QueryContext query)
        {
            if (list.Count > 0)
            {
                var prev = list[list.Count - 1];
                if ((query.Query.LessThan || query.Query.GreaterThan) && 
                    (prev.Query.GreaterThan || prev.Query.LessThan))
                {
                    if (!QueryContextHelper.TryCompress(prev, query))
                    {
                        list.Add(query);
                    }
                }
                else
                {
                    list.Add(query);
                }
            }
            else
            {
                list.Add(query);
            }
        }
    }

    public interface IQueryParser
    {
        IList<QueryContext> Parse(string query);
    }

    public class QuerySegment
    {
        public List<char> Buf0 = new List<char>();
        public List<char> Buf1 = new List<char>();
        public bool Gt = false;
        public bool Lt = false;
        public bool Fz = false;
        public bool Px = false;
        public bool IsPhrase = false;
        public bool IsTerm = false;
        public bool IsDate = false;

        public bool TypeUndetermined
        {
            get
            {
                return IsPhrase == false && IsTerm == false && IsDate == false;
            }
        }
    }
}