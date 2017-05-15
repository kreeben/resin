using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Resin.Analysis;
using Resin.Querying;

namespace Tests
{
    [TestClass]
    public class QueryParserTests
    {
        [TestMethod]
        public void Can_parse_phrase()
        {
            var q = new QueryParser(new Analyzer()).Parse("+title:rambo first blood");

            Assert.AreEqual("+title:rambo +title:first +title:blood", q.ToString());
        }

        [TestMethod]
        public void Can_parse_one_term()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:rambo");

            Assert.AreEqual("+title:rambo", q.ToString());
        }

        [TestMethod]
        public void Can_parse_two_terms_as_AND()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:first +title:blood");

            Assert.AreEqual("+title:first +title:blood", q.ToString());
        }

        [TestMethod]
        public void Can_parse_two_terms_as_OR()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:first title:blood");

            Assert.AreEqual("+title:first title:blood", q.ToString());
        }

        [TestMethod]
        public void Can_parse_two_terms_as_NOT()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:first -title:blood");

            Assert.AreEqual("+title:first -title:blood", q.ToString());
        }

        [TestMethod]
        public void Can_parse_three_terms_as_AND()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:first +title:blood +genre:action");

            Assert.AreEqual("+title:first +title:blood +genre:action", q.ToString());
        }

        [TestMethod]
        public void Can_parse()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:first* -title:blood~ genre:action*");

            Assert.AreEqual("+title:first* -title:blood~ genre:action*", q.ToString());
        }

        [TestMethod]
        public void Can_parse_prefix_term()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:ram*");

            Assert.AreEqual("+title:ram*", q.ToString());
        }

        [TestMethod]
        public void Can_parse_fuzzy_term()
        {
            var q = new QueryParser(new Analyzer()).Parse("title:raymbo~");

            Assert.AreEqual("+title:raymbo~", q.ToString());
        }

        [TestMethod]
        public void Can_parse_fuzzy_phrase()
        {
            var q = new QueryParser(new Analyzer(), 0.5f).Parse("title:was up~");

            Assert.AreEqual("+title:was~ title:up~", q.ToString());
        }

        [TestMethod]
        public void Can_parse_phrases()
        {
            var q = new QueryParser(new Analyzer(), 0.5f).Parse("title:was up~ subtitle:in da house");

            Assert.AreEqual("+title:was~ title:up~ subtitle:in subtitle:da subtitle:house", q.ToString());
        }

        [TestMethod]
        public void Should_prohibit_not_as_first_word()
        {
            var q = new QueryParser(new Analyzer()).Parse("NOT title:rambo");

            Assert.AreEqual(null, q);
        }
    }
}