namespace Resin.KeyValue.Test
{
    [TestClass]
    public sealed class DoubleTests
    {
        [TestMethod]
        public void ColumnWriter_CanWriteOneKey()
        {
            const int testCount = 1;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteSession(pageSize))
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
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

                using (var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream, pageSize))
                {
                    var reader = new ColumnReader<double>(session);

                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                    Assert.IsTrue(reader.Get(1) == ReadOnlySpan<byte>.Empty);

                    for (int i = testCount * 10; i > testCount - 1; i--)
                    {
                        double val = i;
                        Assert.IsTrue(reader.Get(val) == ReadOnlySpan<byte>.Empty);
                    }
                }
            }
        }

        [TestMethod]
        public void ColumnWriter_CanWriteExactlyOnePage()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteSession(pageSize))
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
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

                using (var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream))
                {
                    var reader = new ColumnReader<double>(session);

                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(511)) == 511);
                    Assert.IsTrue(reader.Get(512) == ReadOnlySpan<byte>.Empty);

                    for (int i = testCount * 10; i > testCount - 1; i--)
                    {
                        double val = i;
                        Assert.IsTrue(reader.Get(val) == ReadOnlySpan<byte>.Empty);
                    }
                }
            }
        }

        [TestMethod]
        public void ColumnWriter_CanWriteLessThanOnePage()
        {
            const int testCount = 511;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteSession(pageSize))
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
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

                using (var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream, pageSize))
                {
                    var reader = new ColumnReader<double>(session);

                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(510)) == 510);
                    Assert.IsTrue(reader.Get(511) == ReadOnlySpan<byte>.Empty);

                    for (int i = testCount * 10; i > testCount - 1; i--)
                    {
                        double val = i;
                        Assert.IsTrue(reader.Get(val) == ReadOnlySpan<byte>.Empty);
                    }
                }
            }
        }

        [TestMethod]
        public void ColumnWriter_TryPut_CanWritePages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteSession(pageSize))
            {
                var testCases = new List<double>();
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                }

                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;

                using (var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream, pageSize))
                {
                    var reader = new ColumnReader<double>(session);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(0)) == 0);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(1)) == 1);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(511)) == 511);
                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(512)) == 512);
                    Assert.IsTrue(reader.Get(513) == ReadOnlySpan<byte>.Empty);

                    reader = new ColumnReader<double>(session);

                    Assert.IsTrue(BitConverter.ToDouble(reader.Get(512)) == 512);

                    for (int i = testCount * 10; i > testCount - 1; i--)
                    {
                        double val = i;
                        Assert.IsTrue(reader.Get(val) == ReadOnlySpan<byte>.Empty);
                    }
                }
            }
        }

        [TestMethod]
        public void PageWriter_TryPut_CanThrowOutOfPageStorageException()
        {
            const int testCount = 11;
            int pageSize = 10 * sizeof(double);

            using (var tx = new WriteSession(pageSize))
            {
                var writer = new DoubleWriter(tx);

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
        public void ColumnReader_Get_CanReadPages()
        {
            const int testCount = 513;
            const int pageSize = 512 * sizeof(double);

            using (var tx = new WriteSession(pageSize))
            {
                var testCases = new List<double>();
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                        testCases.Add(val);
                    }
                }

                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;

                using (var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream, pageSize))
                {
                    var reader = new ColumnReader<double>(session);

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
        }

        [TestMethod]
        public void PageWriter_TryPut_ReturnsFalseWhenAddingDuplicateKey()
        {
            using (var tx = new WriteSession())
            {
                var writer = new DoubleWriter(tx);
                Assert.IsTrue(writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0)));
                Assert.IsFalse(writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0)));
            }
        }

        [TestMethod]
        public void ColumnWriter_TryPut_ReturnsFalseWhenAddingDuplicateKeyThatExistsInAPreviousPage()
        {
            const int testCount = 512;
            const int pageSize = 512 * sizeof(double);
            using (var session = new WriteSession(pageSize))
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(session)))
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        double val = i;
                        writer.TryPut(key: val, value: BitConverter.GetBytes(val));
                    }
                }
                session.KeyStream.Position = 0;
                session.ValueStream.Position = 0;
                session.AddressStream.Position = 0;
                using (var writer = new ColumnWriter<double>(new DoubleWriter(session)))
                {
                    var zeroAlreadyExistsSoThisShouldBeFalse = writer.TryPut(key: 0, value: BitConverter.GetBytes((double)0));
                    Assert.IsFalse(zeroAlreadyExistsSoThisShouldBeFalse);
                }
            }
        }

        [TestMethod]
        public void ColumnReader_GetMany_ReturnsSingleValueAndCount1()
        {
            const int pageSize = 64 * sizeof(double);
            using (var tx = new WriteSession(pageSize))
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
                {
                    writer.TryPut(1.0, BitConverter.GetBytes(1.0));
                }

                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;

                using (var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream, pageSize))
                {
                    var reader = new ColumnReader<double>(session);
                    var span = reader.GetMany(1.0, out var count);
                    Assert.AreEqual(1, count);
                    Assert.AreEqual(1.0, BitConverter.ToDouble(span));
                }
            }
        }

        [TestMethod]
        public void ColumnWriter_PutOrAppend_TailAppendsValuesAcrossColumn()
        {
            const int pageSize = 64 * sizeof(double);
            using (var tx = new WriteSession(pageSize))
            {
                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
                {
                    Assert.IsTrue(writer.TryPut(2.0, BitConverter.GetBytes(2.0)));
                }

                using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
                {
                    writer.PutOrAppend(2.0, BitConverter.GetBytes(3.0));
                    writer.PutOrAppend(2.0, BitConverter.GetBytes(4.0));
                }

                tx.KeyStream.Position = 0;
                tx.ValueStream.Position = 0;
                tx.AddressStream.Position = 0;

                using (var session = new ReadSession(tx.KeyStream, tx.ValueStream, tx.AddressStream, pageSize))
                {
                    var reader = new ColumnReader<double>(session);

                    var span = reader.GetMany(2.0, out var count);
                    Assert.AreEqual(3, count);

                    // Tail-appending order: original 2.0 first (converted to head->tail), then appended 3.0, then 4.0
                    var d1 = BitConverter.ToDouble(span.Slice(0, sizeof(double)));
                    var d2 = BitConverter.ToDouble(span.Slice(sizeof(double), sizeof(double)));
                    var d3 = BitConverter.ToDouble(span.Slice(sizeof(double) * 2, sizeof(double)));

                    Assert.AreEqual(2.0, d1);
                    Assert.AreEqual(3.0, d2);
                    Assert.AreEqual(4.0, d3);
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
