using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
using Sir.Document;
using Sir.Search;

namespace Sir.HttpServer.Features
{
    public class SaveAsJob : BaseJob
    {
        private readonly SessionFactory _sessionFactory;
        private readonly QueryParser _queryParser;
        private readonly ILogger<SaveAsJob> _logger;
        private readonly IStringModel _model;
        private readonly HashSet<string> _indexFieldNames;
        private readonly string _target;
        private readonly int _skip;
        private readonly int _take;
        private readonly string[] _select;

        public SaveAsJob(
            SessionFactory sessionFactory,
            QueryParser queryParser,
            IStringModel model,
            ILogger<SaveAsJob> logger,
            string target,
            string[] collections,
            string[] fields,
            string[] select,
            string q, 
            bool and, 
            bool or,
            int skip,
            int take) 
            : base(collections, fields, q, and, or)
        {
            _indexFieldNames = new HashSet<string>(select);
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _logger = logger;
            _model = model;
            _target = target;
            _skip = skip;
            _take = take;
            _select = select;
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
                    or: Or);

                var targetCollectionId = _target.ToHash();
                IEnumerable<IDictionary<string, object>> documents;

                using (var readSession = _sessionFactory.CreateReadSession())
                {
                    documents = readSession.Read(query, _skip, _take).Docs;
                }

                //TODO: Remove this when cc_wat is rebuilt.
                var c = "cc_wat".ToHash();
                foreach (var d in documents)
                {
                    d[SystemFields.CollectionId] = c;
                }
                
                using (var documentWriter = new DocumentWriter(targetCollectionId, _sessionFactory))
                {
                    foreach (var field in _indexFieldNames)
                    {
                        documentWriter.EnsureKeyExists(field);
                    }
                }

                _sessionFactory.SaveAs(
                        targetCollectionId,
                        documents,
                        _indexFieldNames,
                        new HashSet<string>(),
                        _model);
            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {this} {ex}");
            }
        }
    }
}