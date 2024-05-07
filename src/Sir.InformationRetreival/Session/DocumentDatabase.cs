using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.KeyValue;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir
{
    public class DocumentDatabase<T> : IDisposable
    {
        private readonly string _directory;
        private ulong _collectionId;
        private readonly IIndexReadWriteStrategy _indexStrategy;
        private readonly WriteSession _writeSession;
        private readonly IndexSession<T> _indexSession;
        private readonly SearchSession _searchSession;
        private readonly IModel<T> _model;
        private readonly ILogger _logger;

        public IndexSession<T> IndexSession { get { return _indexSession; } }

        public DocumentDatabase(string directory, ulong collectionId, IModel<T> model = null, IIndexReadWriteStrategy indexStrategy = null, ILogger logger = null)
        {
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));
            _collectionId = collectionId;
            _model = model ?? throw new ArgumentNullException(nameof(model));
            _indexStrategy = indexStrategy ?? throw new ArgumentNullException(nameof(indexStrategy));
            _writeSession = new WriteSession(new DocumentRegistryWriter(directory, collectionId));
            _indexSession = new IndexSession<T>(model, indexStrategy, directory, collectionId, logger);
            _searchSession = new SearchSession(directory, _model, _indexStrategy, logger);
            _logger = logger;
        }

        public SearchResult Read(IQuery query, int skip, int take)
        {
            return _searchSession.Search(query, skip, take);
        }

        public void Write(Document document, bool index = true, bool label = true)
        {
            _writeSession.Put(document);

            if (index)
            {
                foreach (var field in document.Fields)
                {
                    if (field.Value != null && field.Value is T typedValue)
                    {
                        _indexSession.Put(document.Id, field.KeyId, typedValue, label);
                    }
                }
            }
        }

        public void OptimizeIndex(int skip, int take, int pageSize, HashSet<string> select = null)
        {
            using (var debugger = new IndexDebugger(_logger))
            using (var kvReader = new KeyValueReader(_directory, _collectionId))
            using (var documents = new DocumentStreamSession(_directory))
            {
                foreach (var batch in documents.ReadDocuments<T>(_collectionId, select, skip, take).Batch(pageSize))
                {
                    using (var indexSession = new IndexSession<T>(_model, _indexStrategy, _directory, _collectionId, _logger))
                    {
                        foreach (var document in batch)
                        {
                            foreach (var field in document.Fields)
                            {
                                indexSession.Put(document.Id, field.KeyId, (T)field.Value, label: false);
                            }

                            debugger.Step(indexSession);
                        }

                        indexSession.Commit();
                    }
                }
            }
        }

        public void Truncate()
        {
            var count = 0;

            if (Directory.Exists(_directory))
            {
                foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*"))
                {
                    File.Delete(file);
                    count++;
                }
            }

            LogInformation($"truncated collection {_collectionId} ({count} files affected)");
        }

        public void TruncateIndex()
        {
            var count = 0;

            foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*.ixtp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }

            LogInformation($"truncated index {_collectionId} ({count} files affected)");
        }

        public void Rename(ulong newCollectionId)
        {
            var count = 0;

            var from = _collectionId.ToString();
            var to = newCollectionId.ToString();

            foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*"))
            {
                File.Move(file, file.Replace(from, to));
                count++;
            }

            LogInformation($"renamed collection {_collectionId} to {newCollectionId} ({count} files affected)");

            _collectionId = newCollectionId;
        }

        public long GetKeyId(string key)
        {
            return _writeSession.EnsureKeyExists(key);
        }

        private void LogInformation(string message)
        {
            if (_logger != null)
                _logger.LogInformation(message);
        }

        public void Dispose()
        {
            if (_writeSession != null)
                _writeSession.Dispose();

            if (_indexSession != null)
                _indexSession.Dispose();

            if (_searchSession != null)
                _searchSession.Dispose();
        }
    }
}