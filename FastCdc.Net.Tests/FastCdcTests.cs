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
        Assert.That(FastCdc.Logarithm2(65537), Is.EqualTo(16));
        Assert.That(FastCdc.Logarithm2(65536), Is.EqualTo(16));
        Assert.That(FastCdc.Logarithm2(65535), Is.EqualTo(16));

        Assert.That(FastCdc.Logarithm2(32769), Is.EqualTo(15));
        Assert.That(FastCdc.Logarithm2(32768), Is.EqualTo(15));
        Assert.That(FastCdc.Logarithm2(32767), Is.EqualTo(15));
        
        Assert.True(FastCdc.Logarithm2(FastCdc.AverageMin) >= 8);
        Assert.True(FastCdc.Logarithm2(FastCdc.AverageMax) <= 28);
    }
    
    
    [Test]
    public void FastCdc_CeilDiv()
    {
        Assert.That(FastCdc.CeilDiv(10, 5), Is.EqualTo(2));
        Assert.That(FastCdc.CeilDiv(11, 5), Is.EqualTo(3));
        Assert.That(FastCdc.CeilDiv(10, 3), Is.EqualTo(4));
        Assert.That(FastCdc.CeilDiv(9, 3), Is.EqualTo(3));
        Assert.That(FastCdc.CeilDiv(6, 2), Is.EqualTo(3));
        Assert.That(FastCdc.CeilDiv(5, 2), Is.EqualTo(3));
    }
    
    [Test]
    public void FastCdc_CenterSize()
    {
        Assert.That(FastCdc.CenterSize(50, 100, 50), Is.EqualTo(0));
        Assert.That(FastCdc.CenterSize(200, 100, 50), Is.EqualTo(50));
        Assert.That(FastCdc.CenterSize(200, 100, 40), Is.EqualTo(40));
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
        Assert.That(FastCdc.Mask(24), Is.EqualTo(16_777_215));
        Assert.That(FastCdc.Mask(16), Is.EqualTo(65535));
        Assert.That(FastCdc.Mask(10), Is.EqualTo(1023));
        Assert.That(FastCdc.Mask(8), Is.EqualTo(255));
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
        
        Assert.That(10, Is.EqualTo(results.Count));
        foreach (var entry in results)
        {
            Assert.That(entry.Hash, Is.EqualTo(3106636015));
            Assert.That(entry.Offset % 1024, Is.EqualTo(0));
            Assert.That(entry.Length, Is.EqualTo(1024));
        }
    }
    
    [Test]
    public void FastCdc_TestSekien16kChunks()
    {
        var array = File.ReadAllBytes("./fixtures/SekienAkashita.jpg");
        
        var fastCdc = new FastCdc(array, 8192, 16384, 32768);
        var results = fastCdc.GetChunks().ToList();
        
        Assert.That(results.Count, Is.EqualTo(6));
        
        Assert.That(results[0].Offset, Is.EqualTo(0));
        Assert.That(results[0].Length, Is.EqualTo(22366));
        Assert.That(results[0].Hash, Is.EqualTo(1527472128));

        Assert.That(results[1].Offset, Is.EqualTo(22366));
        Assert.That(results[1].Length, Is.EqualTo(8282));
        Assert.That(results[1].Hash, Is.EqualTo(1174757376));

        Assert.That(results[2].Offset, Is.EqualTo(30648));
        Assert.That(results[2].Length, Is.EqualTo(16303));
        Assert.That(results[2].Hash, Is.EqualTo(2687197184));

        Assert.That(results[3].Offset, Is.EqualTo(46951));
        Assert.That(results[3].Length, Is.EqualTo(18696));
        Assert.That(results[3].Hash, Is.EqualTo(1210105856));

        Assert.That(results[4].Offset, Is.EqualTo(65647));
        Assert.That(results[4].Length, Is.EqualTo(32768));
        Assert.That(results[4].Hash, Is.EqualTo(2984739645));

        Assert.That(results[5].Offset, Is.EqualTo(98415));
        Assert.That(results[5].Length, Is.EqualTo(11051));
        Assert.That(results[5].Hash, Is.EqualTo(1121740051));
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

            Assert.That(results.Count, Is.EqualTo(groupSize)); 
            for(var idx = 0; idx < groupSize; idx++)
            {
                Assert.That(results[idx].Offset + filePos, Is.EqualTo(chunkOffsets[chunkIndex]));
                Assert.That(results[idx].Length, Is.EqualTo(chunkSizes[chunkIndex]));
                chunkIndex++;
            }

            foreach (var result in results)
                filePos += (int)result.Length;
        }
        
        Assert.That(fileSize, Is.EqualTo(filePos));
    }
    
    [Test]
    public void FastCdc_TestSekien32kChunks()
    {
        var array = File.ReadAllBytes("./fixtures/SekienAkashita.jpg");
        
        var fastCdc = new FastCdc(array, 16384, 32768, 65536);
        var results = fastCdc.GetChunks().ToList();
        
        Assert.That(results.Count, Is.EqualTo(3));
        Assert.That(results[0].Offset, Is.EqualTo(0));
        Assert.That(results[0].Length, Is.EqualTo(32857));
        Assert.That(results[0].Hash, Is.EqualTo(2772598784));

        Assert.That(results[1].Offset, Is.EqualTo(32857));
        Assert.That(results[1].Length, Is.EqualTo(16408));
        Assert.That(results[1].Hash, Is.EqualTo(1651589120));

        Assert.That(results[2].Offset, Is.EqualTo(49265));
        Assert.That(results[2].Length, Is.EqualTo(60201));
        Assert.That(results[2].Hash, Is.EqualTo(1121740051));
    }
    
    [Test]
    public void FastCdc_TestSekien64kChunks()
    {
        var array = File.ReadAllBytes("./fixtures/SekienAkashita.jpg");
        
        var fastCdc = new FastCdc(array, 32768, 65536, 131_072);
        var results = fastCdc.GetChunks().ToList();
        
        Assert.That(results.Count, Is.EqualTo(2));
        Assert.That(results[0].Offset, Is.EqualTo(0));
        Assert.That(results[0].Length, Is.EqualTo(32857));
        Assert.That(results[0].Hash, Is.EqualTo(2772598784));

        Assert.That(results[1].Offset, Is.EqualTo(32857));
        Assert.That(results[1].Length, Is.EqualTo(76609));
        Assert.That(results[1].Hash, Is.EqualTo(1121740051));

    }
}