using System.Collections.Generic;

namespace Sir.Search
{
    public abstract class Reducer
    {
        protected abstract IList<(ulong, long)> Read(ulong collectionId, IList<long> postingsOffsets);

        public void Reduce(IQuery mappedQuery, IDictionary<(ulong, long), double> result)
        {
            var queryResult = new Dictionary<(ulong, long), double>();

            foreach (var term in mappedQuery.Terms)
            {
                if (term.PostingsOffsets == null)
                    continue;

                var termResult = Read(term.CollectionId, term.PostingsOffsets);

                if (term.IsIntersection)
                {
                    if (queryResult.Count == 0)
                    {
                        foreach (var docId in termResult)
                        {
                            queryResult.Add(docId, term.Score);
                        }
                    }
                    else
                    {
                        var intersection = new Dictionary<(ulong, long), double>();

                        foreach (var docId in termResult)
                        {
                            double score;

                            if (queryResult.TryGetValue(docId, out score))
                            {
                                intersection.Add(docId, score + term.Score);
                            }
                        }

                        queryResult = intersection;
                    }

                }
                else if (term.IsUnion)
                {
                    if (queryResult.Count == 0)
                    {
                        foreach (var docId in termResult)
                        {
                            queryResult.Add(docId, term.Score);
                        }
                    }
                    else
                    {
                        foreach (var docId in termResult)
                        {
                            double score;

                            if (queryResult.TryGetValue(docId, out score))
                            {
                                queryResult[docId] = score + term.Score;
                            }
                            else
                            {
                                queryResult.Add(docId, term.Score);
                            }
                        }
                    }
                }
                else // Not
                {
                    if (queryResult.Count == 0)
                    {
                        continue;
                    }

                    foreach (var docId in termResult)
                    {
                        queryResult.Remove(docId);
                    }
                }
            }

            if (mappedQuery.IsIntersection)
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
            else if (mappedQuery.IsUnion)
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
                    foreach (var docId in queryResult)
                    {
                        double score;

                        if (result.TryGetValue(docId.Key, out score))
                        {
                            result[docId.Key] = score + docId.Value;
                        }
                        else
                        {
                            result.Add(docId.Key, docId.Value);
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

            if (mappedQuery.And != null)
            {
                Reduce(mappedQuery.And, result);
            }
            if (mappedQuery.Or != null)
            {
                Reduce(mappedQuery.Or, result);
            }
            if (mappedQuery.Not != null)
            {
                Reduce(mappedQuery.Not, result);
            }
        }
    }
}