using System.Collections.Generic;

namespace Sir.Search
{
    public abstract class Reducer
    {
        protected abstract IList<(ulong, long)> Read(ulong collectionId, IList<long> postingsOffsets);

        public void Reduce(Query mappedQuery, IDictionary<(ulong, long), double> result)
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
                        var scored = new HashSet<(ulong, long)>();

                        foreach (var docId in termResult)
                        {
                            double score;

                            if (queryResult.TryGetValue(docId, out score))
                            {
                                queryResult[docId] = score + term.Score;

                                scored.Add(docId);
                            }
                        }

                        var bad = new HashSet<(ulong, long)>();

                        foreach (var doc in queryResult)
                        {
                            if (!scored.Contains(doc.Key))
                            {
                                bad.Add(doc.Key);
                            }
                        }

                        foreach (var docId in bad)
                        {
                            queryResult.Remove(docId);
                        }
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
                    var scored = new HashSet<(ulong, long)>();

                    foreach (var docId in queryResult)
                    {
                        double score;

                        if (result.TryGetValue(docId.Key, out score))
                        {
                            result[docId.Key] = score + docId.Value;

                            scored.Add(docId.Key);
                        }
                    }

                    var bad = new HashSet<(ulong, long)>();

                    foreach (var doc in queryResult)
                    {
                        if (!scored.Contains(doc.Key))
                        {
                            bad.Add(doc.Key);
                        }
                    }

                    foreach (var docId in bad)
                    {
                        queryResult.Remove(docId);
                    }
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