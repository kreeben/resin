﻿using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.Images;
using Sir.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.Mnist
{
    /// <summary>
    /// Creates a vector index of the MNIST database.
    /// </summary>
    /// <example>
    /// indexmnist --dataDirectory c:\data\resin --imageFileName C:\temp\mnist\train-images.idx3-ubyte --labelFileName C:\temp\mnist\train-labels.idx1-ubyte --collection mnist
    /// </example>
    /// <example>
    /// indexmnist --dataDirectory c:\data\resin --imageFileName C:\temp\mnist\t10k-images.idx3-ubyte --labelFileName C:\temp\mnist\t10k-labels.idx1-ubyte --collection mnist
    /// </example>
    public class IndexMnistCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var time = Stopwatch.StartNew();
            var dataDirectory = args["dataDirectory"];
            var collectionId = args["collection"].ToHash();
            var images = new MnistReader(args["imageFileName"], args["labelFileName"]).Read();
            VectorNode tree;
            var debugger = new IndexDebugger(logger);
            var model = new LinearClassifierImageModel();
            using (var sessionFactory = new SessionFactory(logger))
            {
                sessionFactory.Truncate(dataDirectory, collectionId);

                using (var writeSession = new WriteSession(new DocumentWriter(dataDirectory, collectionId, sessionFactory)))
                using (var indexSession = new InMemoryIndexSession<IImage>(model, model, sessionFactory, dataDirectory, collectionId))
                {
                    var imageIndexId = writeSession.EnsureKeyExists("image");

                    foreach (var image in images)
                    {
                        var imageField = new Field("image", image.Pixels);
                        var labelField = new Field("label", image.Label);
                        var document = new Document(new Field[] { imageField, labelField });

                        writeSession.Put(document);
                        indexSession.Put(document.Id, imageField.KeyId, image, true);

                        debugger.Step(indexSession);
                    }

                    var indices = indexSession.GetInMemoryIndices();

                    tree = indices[imageIndexId];

                    using (var stream = new IndexWriter(dataDirectory, collectionId, sessionFactory, logger: logger))
                    {
                        indexSession.Commit(stream);
                    }
                }
            }

            logger.LogInformation($"indexed {debugger.Steps} mnist images in {time.Elapsed}");

            Print(tree);
        }

        private static void Print(VectorNode tree)
        {
            File.WriteAllText(@"c:\temp\mnisttree.txt", PathFinder.Visualize(tree));
        }
    }
}