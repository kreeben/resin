using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.KeyValue;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir
{
    public class SessionFactory : IDisposable
    {
        private ILogger _logger;

        public SessionFactory(ILogger logger = null)
        {
            _logger = logger;
        }

        private void LogInformation(string message)
        {
            if (_logger != null)
                _logger.LogInformation(message);
        }

        private void LogTrace(string message)
        {
            if (_logger != null)
                _logger.LogTrace(message);
        }

        private void LogDebug(string message)
        {
            if (_logger != null)
                _logger.LogDebug(message);
        }

        private void LogError(Exception ex, string message)
        {
            if (_logger != null)
                _logger.LogError(ex, message);
        }

        public long GetDocCount(string directory, string collection)
        {
            var fileName = Path.Combine(directory, $"{collection.ToHash()}.dix");

            if (!File.Exists(fileName))
                return 0;

            return new FileInfo(fileName).Length / DocIndexWriter.BlockSize;
        }

        public void Truncate(string directory, ulong collectionId)
        {
            var count = 0;

            if (Directory.Exists(directory))
            {
                foreach (var file in Directory.GetFiles(directory, $"{collectionId}*"))
                {
                    File.Delete(file);
                    count++;
                }
            }

            LogInformation($"truncated collection {collectionId} ({count} files affected)");
        }

        public void TruncateIndex(string directory, ulong collectionId)
        {
            var count = 0;

            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ixtp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }

            LogInformation($"truncated index {collectionId} ({count} files affected)");
        }

        public void Rename(string directory, ulong currentCollectionId, ulong newCollectionId)
        {
            var count = 0;

            var from = currentCollectionId.ToString();
            var to = newCollectionId.ToString();

            foreach (var file in Directory.GetFiles(directory, $"{currentCollectionId}*"))
            {
                File.Move(file, file.Replace(from, to));
                count++;
            }

            LogInformation($"renamed collection {currentCollectionId} to {newCollectionId} ({count} files affected)");
        }

        public void StoreDataAndBuildInMemoryIndex<T>(IEnumerable<Document> job, WriteSession writeSession, IndexSession<T> indexSession, int reportSize = 1000, bool label = true)
        {
            var debugger = new IndexDebugger(_logger, reportSize);

            foreach (var document in job)
            {
                writeSession.Put(document);

                foreach (var field in document.Fields)
                {
                    if (field.Value != null)
                    {
                        indexSession.Put(document.Id, field.KeyId, (T)field.Value, label);
                    }
                }

                debugger.Step(indexSession);
            }
        }

        public void StoreDataAndBuildInMemoryIndex<T>(
            Document document, 
            WriteSession writeSession, 
            IndexSession<T> indexSession, 
            bool label = true)
        {
            writeSession.Put(document);

            foreach (var field in document.Fields)
            {
                if (field.Value != null && field.Value is T typedValue)
                {
                    indexSession.Put(document.Id, field.KeyId, typedValue, label);
                }
            }
        }

        public void StoreDataAndPersistIndex<T>(string directory, ulong collectionId, IEnumerable<Document> job, IModel<T> model, IIndexReadWriteStrategy indexStrategy, int reportSize = 1000)
        {
            using (var writeSession = new WriteSession(new DocInfoWriter(directory, collectionId)))
            using (var indexSession = new IndexSession<T>(model, indexStrategy, directory, collectionId))
            {
                StoreDataAndBuildInMemoryIndex(job, writeSession, indexSession, reportSize);

                indexSession.Commit();
            }
        }

        public void Store(string directory, ulong collectionId, IEnumerable<Document> job)
        {
            using (var writeSession = new WriteSession(new DocInfoWriter(directory, collectionId)))
            {
                foreach (var document in job)
                    writeSession.Put(document);
            }
        }

        public bool DocumentExists<T>(string directory, string collection, string key, T value, IModel<T> model, bool label = true)
        {
            using (var kvwriter = new KeyValueWriter(directory, collection.ToHash()))
            {
                var query = new QueryParser<T>(directory, kvwriter, model, logger: _logger)
                                .Parse(collection, value, key, key, and: true, or: false, label);

                if (query != null)
                {

                    using (var searchSession = new SearchSession(directory, model, new LogStructuredIndexingStrategy(model), kvwriter, _logger))
                    {
                        var document = searchSession.SearchScalar(query);

                        if (document != null)
                        {
                            if (document.Score >= model.IdenticalAngle)
                                return true;
                        }
                    }
                }
            }

            return false;
        }

        public FileStream CreateLockFile(string directory, ulong collectionId)
        {
            return new FileStream(Path.Combine(directory, collectionId + ".lock"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
                   4096, FileOptions.RandomAccess | FileOptions.DeleteOnClose);
        }

        public void Dispose()
        {
        }
    }
}