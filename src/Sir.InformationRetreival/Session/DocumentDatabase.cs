using Microsoft.Extensions.Logging;
using Sir.Documents;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir
{
    /// <summary>
    /// Perform read/write operations on a document collection.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DocumentDatabase<T> : IDisposable
    {
        private readonly string _directory;
        private ulong _collectionId;
        private readonly IIndexReadWriteStrategy _indexStrategy;
        private WriteSession _writeSession;
        private IndexSession<T> _indexSession;
        private SearchSession<T> _searchSession;
        private IndexIndex _indexCache;
        private readonly IModel<T> _model;
        private readonly ILogger _logger;
        public IndexSession<T> IndexSession { get { return _indexSession; } }
        public SearchSession<T> SearchSession { get { return _searchSession; } }
        public ulong IndexCollectionId { get { return $"{_collectionId}.index".ToHash(); } }

        public DocumentDatabase(string directory, ulong collectionId, IModel<T> model = null, IIndexReadWriteStrategy indexStrategy = null, ILogger logger = null)
        {
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));
            _collectionId = collectionId;
            _model = model;
            _indexStrategy = indexStrategy;
            _indexCache = new IndexIndex(_model);
            _writeSession = new WriteSession(new DocumentRegistryWriter(directory, collectionId));
            _indexSession = new IndexSession<T>(directory, collectionId, model, indexStrategy, _indexCache, logger);
            _searchSession = new SearchSession<T>(directory, _model, _indexStrategy, logger);
            _logger = logger;
        }

        public QueryParser<T> CreateQueryParser()
        {
            return new QueryParser<T>(SearchSession.GetKeyValueReader(_collectionId), _model, _logger);
        }

        public IEnumerable<Document> StreamDocuments(HashSet<string> fieldsOfInterest, int skip, int take)
        {
            return _searchSession.ReadDocuments<string>(_collectionId, fieldsOfInterest, skip, take);
        }

        public SearchResult Read(Query query, int skip, int take)
        {
            return _searchSession.Search(query, skip, take);
        }

        public void Write(Document document, bool store = true, bool index = true, bool label = false)
        {
            if (document is null)
            {
                throw new ArgumentNullException(nameof(document));
            }

            if (store)
                _writeSession.Put(document);

            if (index)
            {
                foreach (var field in document.Fields)
                {
                    if (field.Value != null && field.Value is T typedValue)
                    {
                        _indexSession.Put(document.Id.Value, field.KeyId, typedValue, label);
                    }
                }
            }
        }

        public void OptimizeAllIndices(int skipDocuments = 0, int takeDocuments = int.MaxValue, int pageSize = 1000, int sampleSize = 1000, HashSet<string> select = null)
        {
            using (var debugger = new IndexDebugger(_logger, sampleSize))
            {
                foreach (var batch in _searchSession.ReadDocuments<T>(_collectionId, select, skipDocuments, takeDocuments).Batch(pageSize))
                {
                    foreach (var document in batch)
                    {
                        Write(document, store: false, index: true, label: false);

                        debugger.Step(_indexSession);
                    }

                    _indexSession.Commit();
                }
            }
        }

        public void Truncate()
        {
            DisposeInternal();

            var count = 0;

            if (Directory.Exists(_directory))
            {
                foreach (var file in Directory.GetFiles(_directory, $"{_collectionId}*"))
                {
                    File.Delete(file);
                    count++;
                }
            }

            if (Directory.Exists(_directory))
            {
                foreach (var file in Directory.GetFiles(_directory, $"{Database.GetIndexCollectionId(_collectionId)}*"))
                {
                    File.Delete(file);
                    count++;
                }
            }

            LogInformation($"truncated collection {_collectionId} ({count} files affected)");

            _writeSession = new WriteSession(new DocumentRegistryWriter(_directory, _collectionId));
            _indexSession = new IndexSession<T>(_directory, _collectionId, _model, _indexStrategy, _indexCache, _logger);
            _searchSession = new SearchSession<T>(_directory, _model, _indexStrategy, _logger);
        }

        public void TruncateIndexOnly()
        {
            DisposeInternal();

            var count = 0;
            var indexCollectionId = Database.GetIndexCollectionId(_collectionId);

            foreach (var file in Directory.GetFiles(_directory, $"{indexCollectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{indexCollectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{indexCollectionId}*.ixtp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{indexCollectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(_directory, $"{indexCollectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }

            LogInformation($"truncated index {_collectionId} ({count} files affected)");

            _writeSession = new WriteSession(new DocumentRegistryWriter(_directory, _collectionId));
            _indexSession = new IndexSession<T>(_directory, _collectionId, _model, _indexStrategy, _indexCache, _logger);
            _searchSession = new SearchSession<T>(_directory, _model, _indexStrategy, _logger);
        }

        public void Rename(ulong newCollectionId)
        {
            DisposeInternal();

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
            _writeSession = new WriteSession(new DocumentRegistryWriter(_directory, _collectionId));
            _indexSession = new IndexSession<T>(_directory, _collectionId, _model, _indexStrategy, _indexCache, _logger);
            _searchSession = new SearchSession<T>(_directory, _model, _indexStrategy, _logger);
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

        public void Commit()
        {
            _writeSession.Commit();
            _indexSession.Commit();
            _searchSession.ClearCachedReaders();
        }

        public void Dispose()
        {
            Commit();
            DisposeInternal();
        }

        private void DisposeInternal()
        {
            if (_writeSession != null)
                _writeSession.Dispose();

            if (_indexSession != null)
                _indexSession.Dispose();

            if (_searchSession != null)
                _searchSession.Dispose();
        }
    }

    public static class Database
    {
        public static ulong GetIndexCollectionId(ulong collectionId)
        {
            return $"{collectionId}.index".ToHash();
        }
    }
}