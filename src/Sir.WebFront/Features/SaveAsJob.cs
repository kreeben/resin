using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.IO;

namespace Sir.HttpServer.Features
{
    public class SaveAsJob<T> : BaseJob
    {
        private readonly SessionFactory _sessionFactory;
        private readonly QueryParser<string> _queryParser;
        private readonly ILogger _logger;
        private readonly IModel<T> _model;
        private readonly string _directory;
        private readonly HashSet<string> _indexFieldNames;
        private readonly string _target;
        private readonly int _skip;
        private readonly int _take;
        private readonly string[] _select;
        private readonly bool _truncate;
        private readonly IIndexReadWriteStrategy _indexStrategy;
        private readonly IConfigurationProvider _configurationProvider;

        public SaveAsJob(
            string directory,
            SessionFactory sessionFactory,
            QueryParser<string> queryParser,
            IModel<T> model,
            IIndexReadWriteStrategy indexStrategy,
            ILogger logger,
            IConfigurationProvider configurationProvider,
            string target,
            string[] collections,
            string[] fields,
            string[] select,
            string q, 
            bool and, 
            bool or,
            int skip,
            int take,
            bool truncate) 
            : base(collections, fields, q, and, or)
        {
            _directory = directory;
            _indexFieldNames = new HashSet<string>(select);
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _logger = logger;
            _model = model;
            _target = target;
            _skip = skip;
            _take = take;
            _select = select;
            _truncate = truncate;
            _indexStrategy = indexStrategy;
            _configurationProvider = configurationProvider;
        }

        public override void Execute()
        {
            try
            {
                var query = _queryParser.Parse(
                    collections: Collections, 
                    q: Q, 
                    fields: Fields, 
                    select: _select,
                    and: And, 
                    or: Or,
                    label: false);

                var targetCollectionId = _target.ToHash();
                IEnumerable<Document> documents;
                var directory = _configurationProvider.Get("data_dir");
                var keys = new KeyRepository(directory, _sessionFactory);

                using (var readSession = new SearchSession(_directory, keys, _sessionFactory, _model, new LogStructuredIndexingStrategy(_model), _logger))
                {
                    documents = readSession.Search(query, _skip, _take).Documents;
                }

                if (_truncate)
                {
                    _sessionFactory.Truncate(_directory, targetCollectionId);
                }
                
                using (var documentWriter = new DocumentWriter(_sessionFactory, _directory, targetCollectionId))
                {
                    foreach (var field in _indexFieldNames)
                    {
                        documentWriter.EnsureKeyExists(field);
                    }
                }

                _sessionFactory.StoreDataAndPersistIndex(
                    _directory,
                    targetCollectionId,
                    documents,
                    _model,
                    _indexStrategy);
            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {this} {ex}");
            }
        }
    }
}