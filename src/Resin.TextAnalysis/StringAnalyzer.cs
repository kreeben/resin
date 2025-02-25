using System.Text.Unicode;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using Microsoft.Extensions.Logging;
using Resin.KeyValue;

namespace Resin.TextAnalysis
{
    public class StringAnalyzer
    {
        const string _workingDirectory = @"c:\data";
        const int _pageSize = 4096;
        const int _numOfDimensions = 512;
        ulong _collectionId = "wikipedia".ToHash();
        Vector<float> _unitVector = CreateVector.Sparse<float>(_numOfDimensions, (float)1);

        public void Validate(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var streamFactory = new StreamFactory(new DirectoryInfo(_workingDirectory));

            using (var tokenKeyStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Key))
            using (var tokenValueStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Value))
            using (var tokenAddressStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Address))
            //using (var bucketKeyStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.BucketKey))
            //using (var bucketValueStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.BucketValue))
            //using (var bucketAddressStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.BucketAddress))

            using (var tokenReader = new DoublePageReader(tokenKeyStream, tokenValueStream, tokenAddressStream, _pageSize))
            //using (var bucketReader = new DoublePageReader(bucketKeyStream, bucketValueStream, bucketAddressStream, _pageSize))
            {
                foreach (var token in Tokenize(source, _numOfDimensions, log))
                {
                    var angle = VectorOperations.CosAngle(_unitVector, token.vector);
                    var hit = tokenReader.IndexOf(angle);

                    if (hit < 0)
                    {
                        var msg = $"could not find {token.label} at {angle}";
                        log.LogInformation(msg);
                        throw new InvalidOperationException(msg);
                    }
                    //else
                    //{
                    //    log.LogInformation($"found {token.label} at {angle}");
                    //}
                }
            }
        }

        public void Analyze(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var words = new SortedList<double, (string label, Vector<float> vector)>();
            var streamFactory = new StreamFactory(new DirectoryInfo(_workingDirectory));

            streamFactory.Truncate();

            using (var tokenKeyStream = streamFactory.CreateAppendStream(_collectionId, FileExtensions.Key))
            using (var tokenReadKeyStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Key))
            using (var tokenValueStream = streamFactory.CreateAppendStream(_collectionId, FileExtensions.Value))
            using (var tokenAddressStream = streamFactory.CreateAppendStream(_collectionId, FileExtensions.Address))
            using (var tokenReadAddressStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Address))
            //using (var bucketKeyStream = streamFactory.CreateAppendStream(_collectionId, FileExtensions.BucketKey))
            //using (var bucketReadKeyStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.BucketKey))
            //using (var bucketValueStream = streamFactory.CreateAppendStream(_collectionId, FileExtensions.BucketValue))
            //using (var bucketAddressStream = streamFactory.CreateAppendStream(_collectionId, FileExtensions.BucketAddress))
            //using (var bucketReadAddressStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.BucketAddress))

            using (var tokenWriter = new PageWriter<double>(
                new DoubleWriter(tokenReadKeyStream, tokenValueStream, tokenReadAddressStream, _pageSize),
                tokenKeyStream,
                tokenAddressStream))
            //using (var bucketWriter = new PageWriter<double>(
            //    new DoubleWriter(bucketReadKeyStream, bucketValueStream, bucketReadAddressStream, _pageSize),
            //    bucketKeyStream,
            //    bucketAddressStream))
            {
                foreach (var token in Tokenize(source, _numOfDimensions, log))
                {
                    var angle = VectorOperations.CosAngle(_unitVector, token.vector);

                    //if (!words.TryAdd(angle, (token.label, token.vector)))
                    //{
                    //    var existingVector = words[angle].vector;
                    //    var a = VectorOperations.CosAngle(existingVector, token.vector);

                    //    if (a < 0.99)
                    //    {
                    //        if (bucketWriter.TryPut(angle, VectorOperations.GetBytes(token.vector)))
                    //        {
                    //            if (log != null)
                    //                log.LogInformation($"**** {words[angle].label} is dupe of {token.label}. both have angle {angle}. individual angle {a}");
                    //        }
                    //    }
                    //}
                    //else
                    //{
                    //    if (!tokenWriter.TryPut(angle, VectorOperations.GetBytes(token.vector)))
                    //    {
                    //        throw new InvalidOperationException("what in the world is going on with the tokens???");
                    //    }
                    //}
                    tokenWriter.TryPut(angle, VectorOperations.GetBytes(token.vector));
                }
                //bucketWriter.Serialize();
                tokenWriter.Serialize();
            }
        }

        public int FindMaxWordLength(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            const int numOfDimensions = 512;
            int maxWordLen = 0;
            foreach (var token in Tokenize(source, numOfDimensions, log))
            {
                var lengtOfWord = ((SparseVectorStorage<float>)token.vector.Storage).Values.Length;
                if (lengtOfWord > maxWordLen)
                {
                    maxWordLen = lengtOfWord;
                }
            }
            if (log != null)
                log.LogInformation($"maxWordLen {maxWordLen}");
            return maxWordLen;
        }

        private IEnumerable<(Vector<float> vector, string label)> Tokenize(IEnumerable<string> source, int numOfDimensions, ILogger? log = null)
        {
            int count = 0;
            const char delimiter = ' ';
            foreach (var str in source)
            {
                int index = 0;
                var word = CreateVector.Sparse<float>(numOfDimensions);
                var label = new List<char>();
                foreach (var c in str.ToCharArray())
                {
                    if (c == delimiter)
                    {
                        yield return (word, new string(label.ToArray()));
                        index = 0;
                        word = CreateVector.Sparse<float>(numOfDimensions);
                        label.Clear();
                    }
                    else
                    {
                        word[index++] = c;
                        label.Add(c);
                    }
                }
                if (((SparseVectorStorage<float>)word.Storage).Values.Length > 0)
                {
                    yield return (word, new string(label.ToArray()));
                }
                if (log != null)
                    log.LogInformation($"{count++} {new string(str.Take(100).ToArray())}");
            }
        }

        public UnicodeRange FindUnicodeRange(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            int first = 0;
            int last = 0;
            int count = 0;

            foreach (var str in source)
            {
                foreach (var c in str.ToCharArray())
                {
                    if (c > last)
                    {
                        last = c;
                    }
                    else if (c < last && c < first)
                    {
                        first = c;
                    }
                }
                if (log != null)
                    log.LogInformation($"{count++} {first} {last}");
            }

            return new UnicodeRange(first, last - first);
        }
    }
}
