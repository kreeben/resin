namespace Resin.KeyValue.Test
{
    [TestClass]
    public sealed class ByteArrayTests
    {

        [ClassInitialize]
        public static void ClassInit(TestContext context)
        {
            // This method is called once for the test class, before any tests of the class are run.
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            // This method is called once for the test class, after all tests of the class are run.
        }

        [TestInitialize]
        public void TestInit()
        {
            // This method is called before each test method.
        }

        [TestCleanup]
        public void TestCleanup()
        {
            // This method is called after each test method.
        }

        [TestMethod]
        public void CanThrowOutOfPageStorageException()
        {
            const int testCount = 11;
            int pageSize = 10 * sizeof(long);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var writer = new ByteArrayWriter(valueStream, pageSize);

                for (int i = 0; i < testCount; i++)
                {
                    if (i == 10)
                    {
                        Assert.ThrowsException<OutOfPageStorageException>(() =>
                        {
                            writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                        });
                    }
                    else
                    {
                        writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                    }
                }
            }
        }

        [TestMethod]
        public void CanReadAndReturnEmptySpanIfNoMatch()
        {
            const int testCount = 10;
            const int pageSize = 4096;

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<long>();
                var writer = new ByteArrayWriter(valueStream, pageSize: pageSize);
                for (int i = 0; i < testCount; i++)
                {
                    long val = i;
                    writer.TryPut(key: i, value: BitConverter.GetBytes(val));
                    testCases.Add(val);
                }
                writer.Serialize(keyStream, addressStream);
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new ByteArrayReader(keyStream, valueStream, addressStream, pageSize: pageSize);
                for (int i = testCases.Count - 1; i > -1; i--)
                {
                    var buf = reader.Get(i);
                    var val = BitConverter.ToInt64(buf);
                    Assert.IsTrue(val == testCases[i]);
                }
                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void CanReadWritePages()
        {
            const int testCount = 513; // 5004 bytes
            const int pageSize = 512 * sizeof(long); // 4096 bytes

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<long>();
                var writer = new ByteArrayWriter(valueStream, pageSize: pageSize);
                for (int i = 0; i < testCount; i++)
                {
                    try
                    {
                        writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                    }
                    catch (OutOfPageStorageException ex)
                    {
                        writer.Serialize(keyStream, addressStream);
                        writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                    }
                    testCases.Add(i);
                }
                writer.Serialize(keyStream, addressStream);
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new ByteArrayReader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt32(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt32(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt32(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new ByteArrayReader(keyStream, valueStream, addressStream, offset: pageSize, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt32(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageWriter_CanWritePages()
        {
            const int testCount = 513; // 5004 bytes
            const int pageSize = 512 * sizeof(long); // 4096 bytes

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<long>();
                using (var writer = new PageWriter(new ByteArrayWriter(valueStream, pageSize: pageSize), keyStream, addressStream))
                    for (int i = 0; i < testCount; i++)
                    {
                        writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                        testCases.Add(i);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new ByteArrayReader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt32(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt32(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt32(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new ByteArrayReader(keyStream, valueStream, addressStream, offset: pageSize, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt32(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageReader_CanReadPages()
        {
            const int testCount = 513; // 5004 bytes
            const int pageSize = 512 * sizeof(long); // 4096 bytes

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<long>();
                using (var writer = new PageWriter(new ByteArrayWriter(valueStream, pageSize: pageSize), keyStream, addressStream))
                    for (int i = 0; i < testCount; i++)
                    {
                        writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                        testCases.Add(i);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new PageReader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt32(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt32(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt32(reader.Get(511)) == 511);
                Assert.IsTrue(BitConverter.ToInt32(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }
    }
}
