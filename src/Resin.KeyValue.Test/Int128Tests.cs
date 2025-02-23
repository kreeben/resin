namespace Resin.KeyValue.Test
{
    [TestClass]
    public sealed class Int128Tests
    {
        const int SizeOfInt128 = 16;

        [TestMethod]
        public void ByteArrayWriter_CanThrowOutOfPageStorageException()
        {
            const int testCount = 11;
            int pageSize = 10 * SizeOfInt128;

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var writer = new Int128Writer(valueStream, pageSize);

                for (Int128 i = 0; i < testCount; i++)
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
        public void ByteArrayReader_CanReadAndReturnEmptySpanIfNoMatch()
        {
            const int testCount = 10;
            const int pageSize = 4096;

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int128>();
                var writer = new Int128Writer(valueStream, pageSize: pageSize);
                for (Int128 i = 0; i < testCount; i++)
                {
                    writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                    testCases.Add(i);
                }
                writer.Serialize(keyStream, addressStream);
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new Int128Reader(keyStream, valueStream, addressStream, pageSize: pageSize);
                for (int i = testCases.Count - 1; i > -1; i--)
                {
                    var buf = reader.Get((Int128)i);
                    var val = BitConverter.ToInt128(buf);
                    Assert.IsTrue(val == testCases[i]);
                }
                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get((Int128)i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void ByteArrayReaderWriter_CanReadWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * SizeOfInt128;

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int128>();
                var writer = new Int128Writer(valueStream, pageSize: pageSize);
                for (Int128 i = 0; i < testCount; i++)
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

                var reader = new Int128Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt128(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt128(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt128(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new Int128Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt128(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get((Int128)i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageWriter_CanWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * SizeOfInt128;

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int128>();
                using (var writer = new PageWriter<Int128>(new Int128Writer(valueStream, pageSize: pageSize), keyStream, addressStream))
                    for (Int128 i = 0; i < testCount; i++)
                    {
                        writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                        testCases.Add(i);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new Int128Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt128(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt128(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt128(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new Int128Reader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt128(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get((Int128)i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageReader_CanReadPages()
        {
            const int testCount = 513;
            const int pageSize = 512 * SizeOfInt128;

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<Int128>();
                using (var writer = new PageWriter<Int128>(new Int128Writer(valueStream, pageSize: pageSize), keyStream, addressStream))
                    for (Int128 i = 0; i < testCount; i++)
                    {
                        writer.TryPut(key: i, value: BitConverter.GetBytes(i));
                        testCases.Add(i);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new PageReader<Int128>(keyStream, valueStream, addressStream, sizeOfTInBytes: SizeOfInt128, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToInt128(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToInt128(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToInt128(reader.Get(511)) == 511);
                Assert.IsTrue(BitConverter.ToInt128(reader.Get(512)) == 512);

                for (Int128 i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
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
                var writer = new Int16Writer(valueStream, 4096);
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
                using (var writer = new PageWriter<Int16>(new Int16Writer(valueStream, pageSize: pageSize), keyStream, addressStream))
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
                using (var writer = new PageWriter<Int16>(new Int16Writer(valueStream, pageSize: pageSize), keyStream, addressStream))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        Int16 val = (Int16)i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                }
                using (var writer = new PageWriter<Int16>(new Int16Writer(valueStream, pageSize: pageSize), keyStream, addressStream))
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
