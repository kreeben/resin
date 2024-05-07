using Microsoft.Extensions.Logging;
using Sir.Images;
using Sir.KeyValue;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Sir.Mnist
{
    /// <summary>
    /// Test the accuracy of a MNIST index.
    /// </summary>
    /// <example>
    /// validatemnist --dataDirectory c:\data\resin --imageFileName C:\temp\mnist\t10k-images.idx3-ubyte --labelFileName C:\temp\mnist\t10k-labels.idx1-ubyte --collection mnist
    /// </example>
    public class ValidateMnistCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var time = Stopwatch.StartNew();
            var dataDirectory = args["dataDirectory"];
            var images = new MnistReader(args["imageFileName"], args["labelFileName"]).Read();
            var collection = args["collection"];
            var count = 0;
            var errors = 0;
            var model = new LinearClassifierImageModel();

            using (var kvReader = new KeyValueReader(dataDirectory, collection.ToHash()))
            using (var searchSession = new SearchSession<IImage>(dataDirectory, model, new SupervisedLogStructuredIndexingStrategy(model), logger))
            {
                var queryParser = new QueryParser<IImage>(kvReader, model, logger: logger);

                foreach (var image in images)
                {
                    var query = queryParser.Parse(collection, image, field: "image", select: "label", and: true, or: false, true);
                    var result = searchSession.Search(query, 0, 1);

                    count++;

                    if (result.Total == 0)
                    {
                        errors++;
                    }
                    else
                    {
                        var documentLabel = (string)result.Documents.First().Get("label").Value;

                        if (!documentLabel.Equals(image.Label))
                        {
                            errors++;

                            logger.LogDebug($"error. label: {image.Label} document label: {documentLabel}\n{((MnistImage)image).Print()}\n{((MnistImage)image).Print()}");
                        }
                    }

                    logger.LogInformation($"errors: {errors}. total tests {count}. error rate: {(float)errors / count * 100}%");
                }
            }

            logger.LogInformation($"tested {count} mnist images in {time.Elapsed}");
        }
    }
}