using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Sir.Documents;
using System;
using System.Collections.Generic;

namespace Sir
{
    public class AnalyzedDocument
    {
        public IList<VectorNode> Nodes { get; }

        public AnalyzedDocument(params VectorNode[] nodes)
        {
            Nodes = nodes;
        }

        public AnalyzedDocument(IList<VectorNode> nodes)
        {
            Nodes = nodes;
        }
    }
}
