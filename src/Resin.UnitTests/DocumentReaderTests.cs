using System.Collections.Generic;
using System.IO;
using System.Linq;
using Resin.IO;
using Resin.IO.Read;
using Resin.IO.Write;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tests
{
    [TestClass]
    public class DocumentReaderTests : Setup
    {
        [TestMethod]
        public void Can_read()
        {
            var docs = new List<Document>
            {
                new Document(0, new List<Field>
                {
                    new Field("title", "rambo"),
                    new Field("_id", "0")
                }),
                new Document(1, new List<Field>
                {
                    new Field("title", "rocky"),
                    new Field("_id", "1")
                }),
                new Document(2, new List<Field>
                {
                    new Field("title", "rocky 2"),
                    new Field("_id", "2")
                })
            };

            var fileName = Path.Combine(CreateDir(), "DocumentReaderTests.Can_read");
            var blocks = new Dictionary<int, BlockInfo>();

            using (var fs = new FileStream(fileName, FileMode.Create, FileAccess.Write))
            using (var writer = new DocumentWriter(fs, Compression.GZip))
            {
                foreach (var doc in docs)
                {
                    blocks.Add(doc.Id, writer.Write(doc));
                }
            }

            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            using (var reader = new DocumentReader(fs, Compression.GZip))
            {
                var doc = reader.Get(new[] { blocks[2] });

                Assert.AreEqual(2, doc.First().Id);
            }

            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            using (var reader = new DocumentReader(fs, Compression.GZip))
            {
                var doc = reader.Get(new[] { blocks[1] });

                Assert.AreEqual(1, doc.First().Id);
            }

            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            using (var reader = new DocumentReader(fs, Compression.GZip))
            {
                var doc = reader.Get(new[] { blocks[0] });

                Assert.AreEqual(0, doc.First().Id);
            }

            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            using (var reader = new DocumentReader(fs, Compression.GZip))
            {
                var ds = reader.Get(blocks.Values.OrderBy(b => b.Position).ToList()).ToList();

                Assert.AreEqual(3, docs.Count);

                Assert.IsTrue(ds.Any(d => d.Id == 0));
                Assert.IsTrue(ds.Any(d => d.Id == 1));
                Assert.IsTrue(ds.Any(d => d.Id == 2));
            }
        }
    }
}