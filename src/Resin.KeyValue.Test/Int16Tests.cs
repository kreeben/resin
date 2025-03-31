namespace Resin.KeyValue.Test
{
    [TestClass]
    public sealed class Int16Tests
    {
        [TestMethod]
        public void ByteArrayWriter_CanThrowOutOfPageStorageException()
        {
            const int testCount = 17;
            int pageSize = 16 * sizeof(Int16);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var writer = new Int16Writer(keyStream, valueStream, addressStream, pageSize);

                for (Int16 i = 0; i < testCount; i++)
                {
                    Int16 val = i;
                    if (i == 16)
                    {
                        Assert.ThrowsException<OutOfPageStorageException>(() =>
                        {
                            writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        });
                    }
                    else
                    {
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                }
            }
        }

        [TestMethod]
        public void ByteArrayReader_CanReadAndReturnEmptySpanIfNoMatch()
        {
            const int testCount = 10;
            const int pageSize = 4096;

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int16>();
                var writer = new Int16Writer(keyStream, valueStream, addressStream, pageSize);
                for (Int16 i = 0; i < testCount; i++)
                {
                    Int16 val = i;
                    writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    testCases.Add(val);
                }
                writer.Serialize();
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new Int16Reader(keyStream, valueStream, addressStream, pageSize: pageSize);
                for (int i = testCases.Count - 1; i > -1; i--)
                {
                    var buf = reader.Get((Int16)i);
                    var val = BitConverter.ToInt16(buf);
                    Assert.IsTrue(val == testCases[i]);
                }
                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get((Int16)i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void ByteArrayReaderWriter_CanReadWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(Int16);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int16>();
                var writer = new Int16Writer(keyStream, valueStream, addressStream, pageSize);
                for (Int16 i = 0; i < testCount; i++)
                {
                    Int16 val = i;
                    try
                    {
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                    catch (OutOfPageStorageException ex)
                    {
                        writer.Serialize();
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                    testCases.Add((Int16)val);
                }
                writer.Serialize();
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new Int16Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt16(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt16(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt16(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new Int16Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt16(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get((Int16)i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageWriter_CanWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(Int16);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int16>();
                using (var writer = new ColumnWriter<Int16>(new Int16Writer(keyStream, valueStream, addressStream, pageSize)))
                    for (Int16 i = 0; i < testCount; i++)
                    {
                        Int16 val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new Int16Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt16(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt16(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt16(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new Int16Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt16(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get((Int16)i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageReader_CanReadPages()
        {
            const int testCount = 513; // 5004 bytes
            const int pageSize = 512 * sizeof(Int16); // 4096 bytes

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int16>();
                using (var writer = new ColumnWriter<Int16>(new Int16Writer(keyStream, valueStream, addressStream, pageSize)))
                    for (int i = 0; i < testCount; i++)
                    {
                        Int16 val = (Int16)i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new ColumnReader<Int16>(keyStream, valueStream, addressStream, sizeOfTInBytes: 2, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt16(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt16(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt16(reader.Get(511)) == 511);
                Assert.IsTrue(BitConverter.ToInt16(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Int16 val = (Int16)i;
                    Assert.IsTrue(reader.Get(val) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void ByteArrayWriter_ReturnsFalseWhenAddingDuplicateKey()
        {
            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var writer = new Int16Writer(keyStream, valueStream, addressStream, 4096);
                Assert.IsTrue(writer.TryPut(key: 0, value: BitConverter.GetBytes((Int16)0)));
                Assert.IsFalse(writer.TryPut(key: 0, value: BitConverter.GetBytes((Int16)0)));
            }
        }

        [TestMethod]
        public void PageWriter_ReturnsFalseWhenAddingDuplicateKeyThatExistsInAPreviousPage()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(Int16);
            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                using (var writer = new ColumnWriter<Int16>(new Int16Writer(keyStream, valueStream, addressStream, pageSize)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        Int16 val = (Int16)i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                    var zeroAlreadyExistsSoThisShouldBeFalse = writer.TryPut(key: 0, value: BitConverter.GetBytes((Int16)0));
                    Assert.IsFalse(zeroAlreadyExistsSoThisShouldBeFalse);
                }
            }
        }

        [TestMethod]
        public void PageWriter_ReturnsFalseWhenAddingDuplicateKeyThatExistsInAPreviousPageWhenUsingFreshWriterInstance()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(Int16);
            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                using (var writer = new ColumnWriter<Int16>(new Int16Writer(keyStream, valueStream, addressStream, pageSize)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        Int16 val = (Int16)i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;
                using (var writer = new ColumnWriter<Int16>(new Int16Writer(keyStream, valueStream, addressStream, pageSize)))
                {
                    var zeroAlreadyExistsSoThisShouldBeFalse = writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0));
                    Assert.IsFalse(zeroAlreadyExistsSoThisShouldBeFalse);
                }
            }
        }

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
    }
}
