namespace fastcdc_net;

public class Chunk
{
    public uint Hash { get; }
    public uint Offset { get; }
    public uint Length { get; }
    
    public Chunk(uint hash, uint offset, uint length)
    {
        Hash = hash;
        Offset = offset;
        Length = length;
    }
}