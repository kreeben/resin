using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Resin.Analysis;

namespace Resin.Querying
{
    public class QueryParser
    {
        private readonly IAnalyzer _analyzer;
        private readonly float _fuzzySimilarity;

        public QueryParser(IAnalyzer analyzer, float fuzzySimilarity = 0.75f)
        {
            _analyzer = analyzer;
            _fuzzySimilarity = fuzzySimilarity;
        }

        public QueryContext Parse(string query)
        {
            if (string.IsNullOrWhiteSpace(query)) throw new ArgumentException("query");
            if (query.StartsWith("NOT", StringComparison.OrdinalIgnoreCase)) return null;

            // http://stackoverflow.com/questions/521146/c-sharp-split-string-but-keep-split-chars-separators

            var trimmedQuery = query.Trim();
            var parts = Regex.Split(trimmedQuery, @"(?<=[ ])");
            var clauses = new List<string>();

            foreach (var part in parts)
            {
                if (part.Contains(':'))
                {
                    clauses.Add(part);
                }
                else
                {
                    clauses[clauses.Count - 1] += part;
                }
            }

            QueryContext root = null;

            for (int i = 0; i < clauses.Count; i++)
            {
                var segs = clauses[i].Split(':');
                var field = segs[0];
                var t = CreateTerm(field, segs[1], i);

                if (root == null)
                {
                    root = t;
                }
                else
                {
                    root.Add(t);
                }
            }

            return root;
        }

        private QueryContext CreateTerm(string field, string word, int positionInQuery)
        {
            var analyze = field[0] != '_' && field.Length > 1 && field[1] != '_';
            QueryContext root = null;

            if (analyze)
            {
                var tokenOperator = word.Trim().Last();
                var analyzable = word.Trim();

                if (tokenOperator == '~' || tokenOperator == '*')
                {
                    analyzable = analyzable.Substring(0, analyzable.Length - 1);
                }

                var analyzed = _analyzer.Analyze(analyzable).ToArray();

                foreach (string token in analyzed)
                {
                    if (root == null)
                    {
                        var t = Parse(field, token, tokenOperator, positionInQuery);
                        
                        root = t;
                    }
                    else
                    {
                        var t = Parse(field, token, tokenOperator, positionInQuery + 1);

                        root.Add(t);
                    }
                }
            }
            else
            {
                root = Parse(field, word);
            }
            return root;
        }

        private QueryContext Parse(string field, string value, char tokenOperator = '\0', int positionInQuery = 0)
        {
            var and = false;
            var not = false;
            var prefix = tokenOperator == '*';
            var fuzzy = tokenOperator == '~';

            string fieldName;

            if (field[0] == '-')
            {
                not = true;
                fieldName = field.Substring(1);
            }
            else if (field[0] == '+')
            {
                and = true;
                fieldName = field.Substring(1);
            }
            else
            {
                fieldName = field;
            }

            if (positionInQuery == 0) and = true;

            DateTime date;

            if (DateTime.TryParse(value, out date))
            {
                value = date.Ticks.ToString();
            }

            return new QueryContext(fieldName, value) { And = and, Not = not, Prefix = prefix, Fuzzy = fuzzy, Similarity = _fuzzySimilarity};
        }
    }
}