namespace Resin.KeyValue.Test
{
    [TestClass]
    public sealed class DoubleTests
    {
        [TestMethod]
        public void ByteArrayWriter_CanThrowOutOfPageStorageException()
        {
            const int testCount = 11;
            int pageSize = 10 * sizeof(double);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var writer = new DoubleWriter(keyStream, valueStream, addressStream, pageSize);

                for (int i = 0; i < testCount; i++)
                {
                    double val = i;
                    if (i == 10)
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
                var testCases = new List<double>();
                var writer = new DoubleWriter(keyStream, valueStream, addressStream, pageSize);
                for (int i = 0; i < testCount; i++)
                {
                    double val = i;
                    writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    testCases.Add(val);
                }
                writer.Serialize();
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new DoubleReader(keyStream, valueStream, addressStream, pageSize: pageSize);
                for (int i = testCases.Count - 1; i > -1; i--)
                {
                    var buf = reader.Get(i);
                    var val = BitConverter.ToDouble(buf);
                    Assert.IsTrue(val == testCases[i]);
                }
                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void ByteArrayReaderWriter_CanReadWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<double>();
                var writer = new DoubleWriter(keyStream, valueStream, addressStream, pageSize);
                for (int i = 0; i < testCount; i++)
                {
                    double val = i;
                    try
                    {
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                    catch (OutOfPageStorageException ex)
                    {
                        writer.Serialize();
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                    testCases.Add(val);
                }
                writer.Serialize();
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new DoubleReader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new DoubleReader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageWriter_CanWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<double>();
                using (var writer = new ColumnWriter<double>(new DoubleWriter(keyStream, valueStream, addressStream, pageSize)))
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new DoubleReader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new DoubleReader(keyStream, valueStream, addressStream, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    double val = i;
                    Assert.IsTrue(reader.Get(val) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void PageReader_CanReadPages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                var testCases = new List<double>();
                using (var writer = new ColumnWriter<double>(new DoubleWriter(keyStream, valueStream, addressStream, pageSize)))
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;

                var reader = new ColumnReader<double>(keyStream, valueStream, addressStream, sizeOfTInBytes: 8, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(511)) == 511);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    double val = i;
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
                var writer = new DoubleWriter(keyStream, valueStream, addressStream, 4096);
                Assert.IsTrue(writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0)));
                Assert.IsFalse(writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0)));
            }
        }

        [TestMethod]
        public void PageWriter_ReturnsFalseWhenAddingDuplicateKeyThatExistsInAPreviousPage()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(double);
            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(keyStream, valueStream, addressStream, pageSize)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                    var zeroAlreadyExistsSoThisShouldBeFalse = writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0));
                    Assert.IsFalse(zeroAlreadyExistsSoThisShouldBeFalse);
                }
            }
        }

        [TestMethod]
        public void PageWriter_ReturnsFalseWhenAddingDuplicateKeyThatExistsInAPreviousPageWhenUsingFreshWriterInstance()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(double);
            using (var keyStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var addressStream = new MemoryStream())
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(keyStream, valueStream, addressStream, pageSize)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                }
                keyStream.Position = 0;
                valueStream.Position = 0;
                addressStream.Position = 0;
                using (var writer = new ColumnWriter<double>(new DoubleWriter(keyStream, valueStream, addressStream, pageSize)))
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
