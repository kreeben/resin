using System.IO;
using System.Linq;
using Resin.IO;
using Resin.IO.Read;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tests
{
    [TestClass]
    public class MappedTrieReaderTests : Setup
    {
        [TestMethod]
        public void Can_find_near()
        {
            var fileName = Path.Combine(CreateDir(), "MappedTrieReaderTests.Can_find_near.tri");

            var tree = new LcrsTrie('\0', false);
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("ba", 1).Select(w => w.Value).ToList();

                Assert.AreEqual(0, near.Count);
            }

            tree.Add("bad");
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("ba", 1).Select(w => w.Value).ToList();

                Assert.AreEqual(1, near.Count);
                Assert.IsTrue(near.Contains("bad"));
            }

            tree.Add("baby");
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("ba", 1).Select(w => w.Value).ToList();

                Assert.AreEqual(1, near.Count);
                Assert.IsTrue(near.Contains("bad"));
            }

            tree.Add("b");
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("ba", 1).Select(w => w.Value).ToList();

                Assert.AreEqual(2, near.Count);
                Assert.IsTrue(near.Contains("bad"));
                Assert.IsTrue(near.Contains("b"));
            }

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("ba", 2).Select(w => w.Value).ToList();

                Assert.AreEqual(3, near.Count);
                Assert.IsTrue(near.Contains("b"));
                Assert.IsTrue(near.Contains("bad"));
                Assert.IsTrue(near.Contains("baby"));
            }

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("ba", 0).Select(w => w.Value).ToList();

                Assert.AreEqual(0, near.Count);
            }

            tree.Add("bananas");
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("ba", 6).Select(w => w.Value).ToList();

                Assert.AreEqual(4, near.Count);
                Assert.IsTrue(near.Contains("b"));
                Assert.IsTrue(near.Contains("bad"));
                Assert.IsTrue(near.Contains("baby"));
                Assert.IsTrue(near.Contains("bananas"));
            }

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("bazy", 1).Select(w => w.Value).ToList();

                Assert.AreEqual(1, near.Count);
                Assert.IsTrue(near.Contains("baby"));
            }

            tree.Add("bank");
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                var near = reader.Near("bazy", 3).Select(w => w.Value).ToList();

                Assert.AreEqual(4, near.Count);
                Assert.IsTrue(near.Contains("baby"));
                Assert.IsTrue(near.Contains("bank"));
                Assert.IsTrue(near.Contains("bad"));
                Assert.IsTrue(near.Contains("b"));
            }
        }

        [TestMethod]
        public void Can_find_prefixed()
        {
            var fileName = Path.Combine(CreateDir(), "MappedTrieReaderTests.Can_find_prefixed.tri");

            var tree = new LcrsTrie('\0', false);

            tree.Add("rambo");
            tree.Add("rambo");

            tree.Add("2");

            tree.Add("rocky");

            tree.Add("2");

            tree.Add("raiders");

            tree.Add("of");
            tree.Add("the");
            tree.Add("lost");
            tree.Add("ark");

            tree.Add("rain");

            tree.Add("man");

            tree.Serialize(fileName);

            var prefixed = new MappedTrieReader(fileName).StartsWith("ra").Select(w => w.Value).ToList();

            Assert.AreEqual(3, prefixed.Count);
            Assert.IsTrue(prefixed.Contains("rambo"));
            Assert.IsTrue(prefixed.Contains("raiders"));
            Assert.IsTrue(prefixed.Contains("rain"));
        }

        [TestMethod]
        public void Can_find_exact()
        {
            var fileName = Path.Combine(CreateDir(), "MappedTrieReaderTests.Can_find_exact.tri");

            var tree = new LcrsTrie('\0', false);
            tree.Add("xor");
            tree.Add("xxx");
            tree.Add("donkey");
            tree.Add("xavier");
            tree.Serialize(fileName);

            Word word;
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsTrue(reader.HasWord("xxx", out word));
            }
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsFalse(reader.HasWord("baby", out word));
            }
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsFalse(reader.HasWord("dad", out word));
            }

            tree.Add("baby");
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsTrue(reader.HasWord("xxx", out word));
            }
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsTrue(reader.HasWord("baby", out word));
            }
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsFalse(reader.HasWord("dad", out word));
            }

            tree.Add("dad");
            tree.Add("daddy");
            tree.Serialize(fileName);

            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsTrue(reader.HasWord("xxx", out word));
            }
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsTrue(reader.HasWord("baby", out word));
            }
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsTrue(reader.HasWord("dad", out word));
            }
            using (var reader = new MappedTrieReader(fileName))
            {
                Assert.IsTrue(reader.HasWord("daddy", out word));
            }
        }

        [TestMethod]
        public void Can_deserialize_whole_file()
        {
            var dir = CreateDir();

            var fileName = Path.Combine(dir, "MappedTrieReaderTests.Can_deserialize_whole_file.tri");

            var tree = new LcrsTrie('\0', false);
            tree.Add("baby");
            tree.Add("bad");
            tree.Add("bank");
            tree.Add("box");
            tree.Add("dad");
            tree.Add("dance");

            Word found;

            Assert.IsTrue(tree.HasWord("baby", out found));
            Assert.IsTrue(tree.HasWord("bad", out found));
            Assert.IsTrue(tree.HasWord("bank", out found));
            Assert.IsTrue(tree.HasWord("box", out found));
            Assert.IsTrue(tree.HasWord("dad", out found));
            Assert.IsTrue(tree.HasWord("dance", out found));

            tree.Serialize(fileName);

            var recreated = Serializer.DeserializeTrie(dir, new FileInfo(fileName).Name);

            Assert.IsTrue(recreated.HasWord("baby", out found));
            Assert.IsTrue(recreated.HasWord("bad", out found));
            Assert.IsTrue(recreated.HasWord("bank", out found));
            Assert.IsTrue(recreated.HasWord("box", out found));
            Assert.IsTrue(recreated.HasWord("dad", out found));
            Assert.IsTrue(recreated.HasWord("dance", out found));
        }
    }
}