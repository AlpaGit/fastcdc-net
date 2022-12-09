using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("FastCdc.Net.Tests")]
namespace FastCdc.Net;

/// <summary>
/// Implements the FastCDC algorithm based on the Rust library fastcdc-rs.
/// </summary>
public sealed class FastCdc
{
    /// Smallest acceptable value for the minimum chunk size.
    public const uint MinimumMin = 64;
    /// Largest acceptable value for the minimum chunk size.
    public const uint MinimumMax = 67_108_864;
    /// Smallest acceptable value for the average chunk size.
    public const uint AverageMin = 256;
    /// Largest acceptable value for the average chunk size.
    public const uint AverageMax = 268_435_456;
    /// Smallest acceptable value for the maximum chunk size.
    public const uint MaximumMin = 1024;
    /// Largest acceptable value for the maximum chunk size.
    public const uint MaximumMax = 1_073_741_824;

    
    private readonly byte[] _source;
    
    private uint _bytesProcessed;
    private int _bytesRemaining;
    
    private readonly uint _minSize;
    private readonly uint _avgSize;
    private readonly uint _maxSize;
    
    private readonly uint _maskS;
    private readonly uint _maskL;
    
    private readonly bool _eof;
    
    public FastCdc(byte[] source, uint minSize, uint avgSize, uint maxSize, bool eof = true)
    {
        if (source == null)
            throw new ArgumentNullException(nameof(source));
        if (source.Length == 0)
            throw new ArgumentException("Source must not be empty.", nameof(source));
        if (minSize is < MinimumMin or > MinimumMax)
            throw new ArgumentOutOfRangeException(nameof(minSize), minSize, $"Minimum chunk size must be between {MinimumMin} and {MinimumMax}.");
        if (avgSize is < AverageMin or > AverageMax)
            throw new ArgumentOutOfRangeException(nameof(avgSize), avgSize, $"Average chunk size must be between {AverageMin} and {AverageMax}.");
        if (maxSize is < MaximumMin or > MaximumMax)
            throw new ArgumentOutOfRangeException(nameof(maxSize), maxSize, $"Maximum chunk size must be between {MaximumMin} and {MaximumMax}.");
        if (minSize > avgSize)
            throw new ArgumentException("Minimum chunk size must not be greater than average chunk size.", nameof(minSize));
        if (avgSize > maxSize)
            throw new ArgumentException("Average chunk size must not be greater than maximum chunk size.", nameof(avgSize));
        
        _source = source;
        _bytesProcessed = 0;
        _bytesRemaining = source.Length;
        _minSize = minSize;
        _avgSize = avgSize;
        _maxSize = maxSize;

        var bits = Logarithm2(avgSize);
        
        _maskS = Mask(bits + 1);
        _maskL = Mask(bits - 1);
        _eof = eof;
    }

    internal (uint, uint) Cut(uint sourceOffset, uint sourceSize)
    {
        if (sourceSize <= _minSize)
            return _eof == false ? (0, 0) : (0u, sourceSize);
        
        sourceSize = Math.Min(sourceSize, _maxSize);
        
        var sourceStart = sourceOffset;
        var sourceLen1 = sourceOffset + CenterSize(_avgSize, _minSize, sourceSize);
        var sourceLen2 = sourceOffset + sourceSize;

        var hash = 0u;
        sourceOffset += _minSize;
        
        // Start by using the "harder" chunking judgement to find chunks
        // that run smaller than the desired normal size.
        while (sourceOffset < sourceLen1)
        {
            var index = _source[sourceOffset];
            sourceOffset++;
            hash = (hash >> 1) + Table.Hashes[index];
            if ((hash & _maskS) == 0)
                return (hash, sourceOffset - sourceStart);
        }

        while (sourceOffset < sourceLen2)
        {
            var index = _source[sourceOffset];
            sourceOffset++;
            hash = (hash >> 1) + Table.Hashes[index];
            if ((hash & _maskL) == 0)
                return (hash, sourceOffset - sourceStart);
        }
        
        return (_eof == false && sourceSize < _maxSize) ? (hash, 0) : (hash, sourceSize);
    }

    private Chunk? Next()
    {
        if (_bytesRemaining == 0)
            return null;
        
        var (chunkHash, chunkSize) = Cut(_bytesProcessed, (uint)_bytesRemaining);
        if(chunkSize == 0)
            return null;
        
        var chunkStart = _bytesProcessed;
        _bytesProcessed += chunkSize;
        _bytesRemaining -= (int)chunkSize;

        return new Chunk(chunkHash, chunkStart, chunkSize);
    }

    public IEnumerable<Chunk> GetChunks()
    {
        while(Next() is { } chunk)
            yield return chunk;
    }

    internal static uint CenterSize(uint average, uint minimum, uint sourceSize)
    {
        var offset = minimum + CeilDiv(minimum, 2);
        if (offset > average)
            offset = average;
        
        var size = average - offset;
        return size > sourceSize ? sourceSize : size;
    }

    internal static uint Logarithm2(uint value) => 
        (uint)Math.Round(Math.Log(value, 2));

    internal static uint CeilDiv(uint x, uint y) => 
        (x + y - 1) / y;

    internal static uint Mask(uint bits){
        if (bits is < 1 or > 31)
            throw new ArgumentOutOfRangeException(nameof(bits), bits, "Bits must be between 1 and 31.");
        
        return (uint)Math.Pow(2, bits) - 1;
    }
}