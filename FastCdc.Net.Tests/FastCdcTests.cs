namespace FastCdc.Net.Tests;

public class FastCdcTests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void FastCdc_Logarithm2()
    {
        Assert.AreEqual(16, FastCdc.Logarithm2(65537));
        Assert.AreEqual(16, FastCdc.Logarithm2(65536));
        Assert.AreEqual(16, FastCdc.Logarithm2(65535));
        Assert.AreEqual(15, FastCdc.Logarithm2(32769));
        Assert.AreEqual(15, FastCdc.Logarithm2(32768));
        Assert.AreEqual(15, FastCdc.Logarithm2(32767));
        
        Assert.True(FastCdc.Logarithm2(FastCdc.AverageMin) >= 8);
        Assert.True(FastCdc.Logarithm2(FastCdc.AverageMax) <= 28);
    }
    
    
    [Test]
    public void FastCdc_CeilDiv()
    {
        Assert.AreEqual(2, FastCdc.CeilDiv(10, 5));
        Assert.AreEqual(3, FastCdc.CeilDiv(11, 5));
        Assert.AreEqual(4, FastCdc.CeilDiv(10, 3));
        Assert.AreEqual(3, FastCdc.CeilDiv(9, 3));
        Assert.AreEqual(3, FastCdc.CeilDiv(6, 2));
        Assert.AreEqual(3, FastCdc.CeilDiv(5, 2));
    }
    
    [Test]
    public void FastCdc_CenterSize()
    {
        Assert.AreEqual(0, FastCdc.CenterSize(50, 100, 50));
        Assert.AreEqual(50, FastCdc.CenterSize(200, 100, 50));
        Assert.AreEqual(40, FastCdc.CenterSize(200, 100, 40));
    }
    
    [Test]
    public void FastCdc_MaskLow()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => FastCdc.Mask(0)); 
    }
    
    [Test]
    public void FastCdc_MaskHigh()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => FastCdc.Mask(32)); 
    }
    
    [Test]
    public void FastCdc_Mask()
    {
        Assert.AreEqual(16_777_215, FastCdc.Mask(24));
        Assert.AreEqual(65535, FastCdc.Mask(16));
        Assert.AreEqual(1023, FastCdc.Mask(10)); 
        Assert.AreEqual(255, FastCdc.Mask(8));
    }
    
    [Test]
    public void FastCdc_MinimumTooLow()
    {
        var array = new byte[2048];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = new FastCdc(array, 63, 256, 1024);
        }); 
    }
    
    [Test]
    public void FastCdc_MinimumTooHigh()
    {
        var array = new byte[2048];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = new FastCdc(array, 67_108_867, 256, 1024);
        }); 
    }
    
    [Test]
    public void FastCdc_AverageTooLow()
    {
        var array = new byte[2048];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = new FastCdc(array, 64, 255, 1024);
        }); 
    }
    
    [Test]
    public void FastCdc_AverageTooHigh()
    {
        var array = new byte[2048];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = new FastCdc(array, 64, 268_435_457, 1024);
        }); 
    }
    
    [Test]
    public void FastCdc_MaximumTooLow()
    {
        var array = new byte[2048];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = new FastCdc(array, 64, 256, 1023);
        }); 
    }
    
    [Test]
    public void FastCdc_MaximumTooHigh()
    {
        var array = new byte[2048];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = new FastCdc(array, 64, 256, 1_073_741_825);
        }); 
    }
    
    [Test]
    public void FastCdc_TestAllZeros()
    {
        var array = new byte[10240];
        var fastCdc = new FastCdc(array, 64, 256, 1024);
        var results = fastCdc.GetChunks().ToList();
        
        Assert.AreEqual(results.Count, 10);
        foreach (var entry in results)
        {
            Assert.AreEqual(3106636015, entry.Hash);
            Assert.AreEqual(0, entry.Offset % 1024);
            Assert.AreEqual(1024, entry.Length);
        }
    }
    
    [Test]
    public void FastCdc_TestSekien16kChunks()
    {
        var array = File.ReadAllBytes("./fixtures/SekienAkashita.jpg");
        
        var fastCdc = new FastCdc(array, 8192, 16384, 32768);
        var results = fastCdc.GetChunks().ToList();
        
        Assert.AreEqual(6, results.Count);
        Assert.AreEqual(0, results[0].Offset);
        Assert.AreEqual(22366, results[0].Length);
        Assert.AreEqual(1527472128, results[0].Hash);

        Assert.AreEqual(22366, results[1].Offset);
        Assert.AreEqual(8282, results[1].Length);
        Assert.AreEqual(1174757376, results[1].Hash);

        Assert.AreEqual(30648, results[2].Offset);
        Assert.AreEqual(16303, results[2].Length);
        Assert.AreEqual(2687197184, results[2].Hash);

        Assert.AreEqual(46951, results[3].Offset); 
        Assert.AreEqual(18696, results[3].Length);
        Assert.AreEqual(1210105856, results[3].Hash);

        Assert.AreEqual(65647, results[4].Offset);
        Assert.AreEqual(32768, results[4].Length);
        Assert.AreEqual(2984739645, results[4].Hash);

        Assert.AreEqual(98415, results[5].Offset);
        Assert.AreEqual(11051, results[5].Length);
        Assert.AreEqual(1121740051, results[5].Hash);
    }
    
    [Test]
    public void FastCdc_TestSekien16kChunks_Streaming()
    {
        var filePath = "./fixtures/SekienAkashita.jpg";
            
        var array = File.ReadAllBytes(filePath);
        
        var chunkOffsets = new uint[] {0, 22366, 30648, 46951, 65647, 98415};
        var chunkSizes = new uint[] {22366, 8282, 16303, 18696, 32768, 11051};
        
        const uint bufSize = 32768;
        var fileInfo = new FileInfo(filePath);
        var fileSize = fileInfo.Length;

        var filePos = 0;
        var chunkIndex = 0;

        var groupSizes = new[] { 2, 1, 1, 1, 1 };
        
        foreach (var groupSize in groupSizes)
        {
            var upperBound = filePos + bufSize;
            
            var (eof, slice) = upperBound >= fileSize ?
                (true, array[filePos..]) :
                (false, array[filePos..(int)upperBound]);
            
            var fastCdc = new FastCdc(slice, 8192, 16384, 32768, eof);
            var results = fastCdc.GetChunks().ToList();

            Assert.AreEqual(groupSize, results.Count); 
            for(var idx = 0; idx < groupSize; idx++)
            {
                Assert.AreEqual(chunkOffsets[chunkIndex], results[idx].Offset + filePos);
                Assert.AreEqual(chunkSizes[chunkIndex], results[idx].Length);
                chunkIndex++;
            }

            foreach (var result in results)
                filePos += (int)result.Length;
        }
        
        Assert.AreEqual(fileSize, filePos);
    }
    
    [Test]
    public void FastCdc_TestSekien32kChunks()
    {
        var array = File.ReadAllBytes("./fixtures/SekienAkashita.jpg");
        
        var fastCdc = new FastCdc(array, 16384, 32768, 65536);
        var results = fastCdc.GetChunks().ToList();
        
        Assert.AreEqual(3, results.Count);
        Assert.AreEqual(0, results[0].Offset);
        Assert.AreEqual(32857, results[0].Length);
        Assert.AreEqual(2772598784, results[0].Hash);

        Assert.AreEqual(32857, results[1].Offset);
        Assert.AreEqual(16408, results[1].Length);
        Assert.AreEqual(1651589120, results[1].Hash);

        Assert.AreEqual(49265, results[2].Offset);
        Assert.AreEqual(60201, results[2].Length);
        Assert.AreEqual(1121740051, results[2].Hash);
    }
    
    [Test]
    public void FastCdc_TestSekien64kChunks()
    {
        var array = File.ReadAllBytes("./fixtures/SekienAkashita.jpg");
        
        var fastCdc = new FastCdc(array, 32768, 65536, 131_072);
        var results = fastCdc.GetChunks().ToList();
        
        Assert.AreEqual(2, results.Count);
        Assert.AreEqual(0, results[0].Offset);
        Assert.AreEqual(32857, results[0].Length);
        Assert.AreEqual(2772598784, results[0].Hash);

        Assert.AreEqual(32857, results[1].Offset);
        Assert.AreEqual(76609, results[1].Length);
        Assert.AreEqual(1121740051, results[1].Hash);
    }
}