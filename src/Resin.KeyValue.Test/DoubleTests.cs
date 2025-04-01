namespace Resin.KeyValue.Test
{
    [TestClass]
    public sealed class DoubleTests
    {
        [TestMethod]
        public void PageWriter_TryPut_CanThrowOutOfPageStorageException()
        {
            const int testCount = 11;
            int pageSize = 10 * sizeof(double);

            using (var tx = new WriteTransaction())
            {
                var writer = new DoubleWriter(tx, pageSize);

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
        public void PageReader_Get_CanReadAndReturnEmptySpanIfNoMatch()
        {
            const int testCount = 10;
            const int pageSize = 4096;
            var testCases = new List<double>();
            using (var tx = new WriteTransaction())
            {
                var writer = new DoubleWriter(tx, pageSize);
                for (int i = 0; i < testCount; i++)
                {
                    double val = i;
                    writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    testCases.Add(val);
                }
                writer.Serialize();
                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;
                using var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream);
                var reader = new DoubleReader(session, pageSize: pageSize);
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
        public void PageReaderWriter_CanReadWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteTransaction())
            {
                var testCases = new List<double>();
                var writer = new DoubleWriter(tx, pageSize);
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
                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;

                using var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream);
                var reader = new DoubleReader(session, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new DoubleReader(session, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    Assert.IsTrue(reader.Get(i) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void ColumnWriter_TryPut_CanWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteTransaction())
            {
                var testCases = new List<double>();
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx, pageSize)))
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;

                using var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream);
                var reader = new DoubleReader(session, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                Assert.IsTrue(BitConverter.ToDouble(reader.Get(511)) == 511);
                Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                reader = new DoubleReader(session, pageSize: pageSize);

                Assert.IsTrue(BitConverter.ToDouble(reader.Get(512)) == 512);

                for (int i = testCount * 10; i > testCount - 1; i--)
                {
                    double val = i;
                    Assert.IsTrue(reader.Get(val) == ReadOnlySpan<byte>.Empty);
                }
            }
        }

        [TestMethod]
        public void ColumnReader_TryPut_CanReadPages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteTransaction())
            {
                var testCases = new List<double>();
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx, pageSize)))
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;

                using var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream);
                var reader = new ColumnReader<double>(session, sizeOfT: 8, pageSize: pageSize);

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
        public void PageWriter_TryPut_ReturnsFalseWhenAddingDuplicateKey()
        {
            using (var tx = new WriteTransaction())
            {
                var writer = new DoubleWriter(tx, 4096);
                Assert.IsTrue(writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0)));
                Assert.IsFalse(writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0)));
            }
        }

        [TestMethod]
        public void ColumnWriter_TryPut_ReturnsFalseWhenAddingDuplicateKeyThatExistsInAPreviousPage()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(double);
            using (var tx = new WriteTransaction())
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx, pageSize)))
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
        public void ColumnWriter_TryPut_ReturnsFalseWhenAddingDuplicateKeyThatExistsInAPreviousPageWhenUsingFreshWriterInstance()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(double);
            using (var tx = new WriteTransaction())
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx, pageSize)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                }
                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx, pageSize)))
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
