using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.IO;
using Sir.Strings;
using System.Collections.Generic;
using System.IO;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using System;

namespace Sir.Wikipedia
{
    /// <summary>
    /// Download JSON search index dump here: 
    /// https://dumps.wikimedia.org/other/cirrussearch/current/enwiki-20201026-cirrussearch-content.json.gz
    /// </summary>
    /// <example>
    /// indexwikipedia --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --file d:\enwiki-20211122-cirrussearch-content.json.gz --collection wikipedia --skip 0 --take 1000
    /// </example>
    public class IndexWikipediaCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            // Required
            var dataDirectory = args["directory"];
            var fileName = args["file"];
            var collection = args["collection"];

            // Optional
            var skip = args.ContainsKey("skip") ? int.Parse(args["skip"]) : 0;
            var take = args.ContainsKey("take") ? int.Parse(args["take"]) : int.MaxValue;
            var sampleSize = args.ContainsKey("sampleSize") ? int.Parse(args["sampleSize"]) : 1000;
            var pageSize = args.ContainsKey("pageSize") ? int.Parse(args["pageSize"]) : 100000;

            var collectionId = collection.ToHash();
            var fieldsOfInterest = new HashSet<string> { "title", "text", "url" };

            if (take == 0)
                take = int.MaxValue;

            if (!File.Exists(fileName))
            {
                throw new FileNotFoundException($"This file could not be found: {fileName}. Download a wikipedia cirrussearch JSON dump here:  https://dumps.wikimedia.org/other/cirrussearch/current/");
            }

            var model = new BagOfCharsModel();
            var indexStrategy = new LogStructuredIndexingStrategy(model);
            var payload = WikipediaHelper.Read(fileName, skip, take, fieldsOfInterest);
            var embedding = new SortedList<int, float>();
            var meanVectors = new Dictionary<long, Vector<float>>();

            using (var streamDispatcher = new SessionFactory(logger))
            {
                //streamDispatcher.Truncate(dataDirectory, collectionId);

                //// store documents
                //using (var debugger = new BatchDebugger(logger, sampleSize))
                //using (var writeSession = new WriteSession(new DocumentWriter(streamDispatcher, dataDirectory, collectionId)))
                //{
                //    foreach (var document in payload)
                //    {
                //        writeSession.Put(document);

                //        debugger.Step("storing documents");
                //    }
                //}

                //// build mean vectors
                //using (var debugger = new BatchDebugger(logger, sampleSize))
                //using (var documents = new DocumentStreamSession(dataDirectory, streamDispatcher))
                //{
                //    foreach (var document in documents.ReadDocuments(collectionId, fieldsOfInterest, skip, take))
                //    {
                //        foreach (var field in document.Fields)
                //        {
                //            var keyId = streamDispatcher.GetKeyId(dataDirectory, collectionId, field.Name.ToHash());
                //            Vector<float> meanVector;

                //            if (!meanVectors.TryGetValue(keyId, out meanVector))
                //            {
                //                meanVector = CreateVector.Sparse<float>(model.NumOfDimensions);
                //                meanVectors.Add(keyId, meanVector);
                //            }

                //            foreach (var vector in model.CreateEmbedding((string)field.Value, false, embedding))
                //            {
                //                meanVector.Add(vector.Value, meanVector);
                //            }

                //            meanVectors[keyId] = meanVector.Divide(2);
                //        }

                //        debugger.Step("building mean vectors");
                //    }
                //}

                //// store mean vectors
                //SerializeMeanVectors(streamDispatcher, dataDirectory, collectionId, meanVectors);

                // create indices
                using (var debugger = new BatchDebugger(logger, sampleSize))
                using (var documents = new DocumentStreamSession(dataDirectory, streamDispatcher))
                {
                    foreach (var batch in documents.ReadDocuments(collectionId, fieldsOfInterest, skip, take).Batch(pageSize))
                    {
                        using (var indexSession = new IndexSession<string>(model, indexStrategy, streamDispatcher, dataDirectory, collectionId, logger))
                        {
                            foreach (var document in batch)
                            {
                                foreach (var field in document.Fields)
                                {
                                    indexSession.PutMany(document.Id, field.KeyId, model.CreateEmbedding((string)field.Value, true, embedding));
                                }

                                debugger.Step("creating indices");
                            }

                            indexSession.Commit();
                        }
                    }
                }

                // validate indices
                using (var debugger = new BatchDebugger(logger, sampleSize))
                using (var validateSession = new ValidateSession<string>(
                    collectionId,
                    new SearchSession(dataDirectory, streamDispatcher, model, new LogStructuredIndexingStrategy(model), logger),
                    new QueryParser<string>(dataDirectory, streamDispatcher, model, embedding: embedding, logger: logger)))
                {
                    using (var documents = new DocumentStreamSession(dataDirectory, streamDispatcher))
                    {
                        foreach (var doc in documents.ReadDocuments(collectionId, new HashSet<string> { "title" }, skip, take))
                        {
                            validateSession.Validate(doc, "title");

                            Console.WriteLine($"{doc.Id} {doc.Get("title").Value}");

                            debugger.Step("validating documents");
                        }
                    }
                }
            }
        }

        private static void SerializeMeanVectors(IStreamDispatcher streamDispatcher, string directory, ulong collectionId, Dictionary<long, Vector<float>> meanVectors)
        {
            foreach (var field in meanVectors)
            {
                var keyId = field.Key;

                using (var vectorIndexStream = streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "vecix"))
                using (var vectorStream = streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "vec"))
                {
                    // write vector
                    var vectorOffset = SerializeVector(vectorStream, field.Value);

                    // write vector offset
                    Span<byte> obuf = BitConverter.GetBytes(vectorOffset);
                    vectorIndexStream.Write(obuf);

                    // write component count
                    var v = (SparseVectorStorage<float>)field.Value.Storage;
                    Span<byte> cbuf = BitConverter.GetBytes(v.ValueCount);
                    vectorIndexStream.Write(cbuf);
                }
            }
        }

        private static long SerializeVector(Stream stream, Vector<float> vector)
        {
            var pos = stream.Position;
            var storage = (SparseVectorStorage<float>)vector.Storage;

            foreach (var index in storage.Indices)
            {
                stream.Write(BitConverter.GetBytes(index));

                //if (index > 0)
                //    stream.Write(BitConverter.GetBytes(index));
                //else
                //    break;
            }

            foreach (var value in storage.Values)
            {
                stream.Write(BitConverter.GetBytes(value));

                //if (value > 0)
                //    stream.Write(BitConverter.GetBytes(value));
                //else
                //    break;
            }

            return pos;
        }

        private static double CosAngle(Vector<float> vec1, Vector<float> vec2)
        {
            var dotProduct = vec1.DotProduct(vec2);
            var dotSelf1 = vec1.Norm(2);
            var dotSelf2 = vec2.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        private static void Print(string name, VectorNode tree)
        {
            var diagram = PathFinder.Visualize(tree);
            File.WriteAllText($@"c:\temp\{name}.txt", diagram);
        }
    }
}