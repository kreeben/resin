using System.Collections.Generic;

namespace Sir
{
    public class Scorer
    {
        /// <summary>
        /// Reduce query to a list of scored document IDs.
        /// </summary>
        public void Score(Query query, ref IDictionary<(ulong CollectionId, long DocumentId), double> result)
        {
            IDictionary<(ulong, long), double> queryResult = new Dictionary<(ulong, long), double>();

            Score(query.Terms, ref queryResult);

            if (query.IsIntersection)
            {
                if (result.Count == 0)
                {
                    foreach (var docId in queryResult)
                    {
                        result.Add(docId.Key, docId.Value);
                    }
                }
                else
                {
                    var intersection = new Dictionary<(ulong, long), double>();

                    foreach (var doc in queryResult)
                    {
                        double score;

                        if (result.TryGetValue(doc.Key, out score))
                        {
                            intersection.Add(doc.Key, score + doc.Value);
                        }
                    }

                    result = intersection;
                }
            }
            else if (query.IsUnion)
            {
                if (result.Count == 0)
                {
                    foreach (var docId in queryResult)
                    {
                        result.Add(docId.Key, docId.Value);
                    }
                }
                else
                {
                    foreach (var doc in queryResult)
                    {
                        if (result.ContainsKey(doc.Key))
                        {
                            result[doc.Key] += doc.Value;
                        }
                        else
                        {
                            result[doc.Key] = doc.Value;
                        }
                    }
                }
            }
            else // Not
            {
                if (result.Count > 0)
                {
                    foreach (var docId in queryResult)
                    {
                        result.Remove(docId.Key);
                    }
                }
            }

            if (query.AndQuery != null)
            {
                Score(query.AndQuery, ref result);
            }
            if (query.OrQuery != null)
            {
                Score(query.OrQuery, ref result);
            }
            if (query.NotQuery != null)
            {
                Score(query.NotQuery, ref result);
            }
        }

        public static void Score(IList<Term> terms, ref IDictionary<(ulong CollectionId, long DocumentId), double> result)
        {
            foreach (var term in terms)
            {
                if (term.PostingsOffsets == null)
                    continue;

                if (term.IsIntersection)
                {
                    if (result.Count == 0)
                    {
                        foreach (var docId in term.DocumentIds)
                        {
                            result.Add(docId, term.Score);
                        }
                    }
                    else
                    {
                        var intersection = new Dictionary<(ulong, long), double>();

                        foreach (var doc in term.DocumentIds)
                        {
                            double score;

                            if (result.TryGetValue(doc, out score))
                            {
                                intersection.Add(doc, score + term.Score);
                            }
                        }

                        result = intersection;
                    }
                }
                else if (term.IsUnion)
                {
                    if (result.Count == 0)
                    {
                        foreach (var docId in term.DocumentIds)
                        {
                            result.Add(docId, term.Score);
                        }
                    }
                    else
                    {
                        foreach (var doc in term.DocumentIds)
                        {
                            if (result.ContainsKey(doc))
                            {
                                result[doc] += term.Score;
                            }
                            else
                            {
                                result[doc] = term.Score;
                            }
                        }
                    }
                }
                else // Not
                {
                    if (result.Count > 0)
                    {
                        foreach (var doc in term.DocumentIds)
                        {
                            result.Remove(doc);
                        }
                    }
                }
            }
        }
    }
}