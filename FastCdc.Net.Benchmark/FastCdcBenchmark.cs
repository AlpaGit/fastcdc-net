using BenchmarkDotNet.Attributes;

namespace FastCdc.Net.Benchmark;

[MemoryDiagnoser]
public class FastCdcBenchmark
{
    private byte[] _data;
    
    [GlobalSetup]
    public void Setup()
    {
        _data = File.ReadAllBytes("./fixtures/SekienAkashita.jpg");
    }
    
    [Benchmark]
    public void Benchmark16K()
    {
        var fastCdc = new FastCdc(_data, 8192, 16384, 32768);
        _ = fastCdc.GetChunks().ToArray();
    }
        
    
    [Benchmark]
    public void Benchmark32K()
    {
        var fastCdc = new FastCdc(_data, 16384, 32768, 65536);
        _ = fastCdc.GetChunks().ToArray();
    }
    
    [Benchmark]
    public void Benchmark64K()
    {
        var fastCdc = new FastCdc(_data, 32768, 65536, 131_072);
        _ = fastCdc.GetChunks().ToArray();
    }
}