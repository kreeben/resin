using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
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

        public SaveAsJob(
            SessionFactory sessionFactory,
            QueryParser queryParser,
            IStringModel model,
            ILogger<SaveAsJob> logger,
            HashSet<string> indexFieldNames,
            string target,
            string[] collection, 
            string[] field, 
            string q, 
            bool and, 
            bool or) 
            : base(collection, field, q, and, or)
        {
            _indexFieldNames = indexFieldNames;
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _logger = logger;
            _model = model;
            _target = target;
        }

        public override void Execute()
        {
            try
            {
                var query = _queryParser.Parse(
                    Collections, 
                    Q, 
                    Fields, 
                    new string[] {"title", "description"},
                    and: And, 
                    or: Or);

                var targetCollectionId = _target.ToHash();

                using (var readSession = _sessionFactory.CreateReadSession())
                {
                    var documents = readSession.Read(query, 0, int.MaxValue).Docs;

                    _sessionFactory.IndexOnly(
                            targetCollectionId,
                            documents,
                            _indexFieldNames);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {this} {ex}");
            }
        }

    }
}