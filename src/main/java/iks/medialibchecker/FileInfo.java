package iks.medialibchecker;

import net.jpountz.xxhash.*;
import org.slf4j.*;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static iks.medialibchecker.Utils.getFileSizeNice;

class FileInfo {
	private static final Logger performanceLog = LoggerFactory.getLogger( "performance.FileInfo" );
	private static final Logger log = LoggerFactory.getLogger( FileInfo.class );
	final File file;
	int blockSize;
	private static final long HASH_SEED = 0x9747b2842093420L;
	// value you want, but always the same

	private static final XXHashFactory factory = XXHashFactory.fastestInstance();
	private StreamingXXHash64 hash64;

	/**
	 * File extent map.
	 * Sorted from last block to the first (to maximize block list collapsing during checksum scan).
	 */
	List< FileSegment > segments;

	FileInfo( File directory, File file ) {
		this.file = file;
	}

	String getName() {
		return file.getName();
	}

	void setBlockSize( int blockSize ) {
		this.blockSize = blockSize;
	}

	private FileChannel scanChannel;
	private int nextExtentIndex;
	private long bytesRemains;
	private long hash;
	private byte[] mirrorBuffer;

	void setExtentMap( ArrayList< FileSegment > extentMap ) {
		this.segments = extentMap;
	}

	void calcHash() throws IOException {
		if ( scanChannel == null ) {
			scanChannel = new FileInputStream( file ).getChannel();
			hash64 = factory.newStreamingHash64( HASH_SEED );
			bytesRemains = scanChannel.size();
		}
		FileSegment nextSegment = getNextSegment();
		long startTime = System.nanoTime();
		int toBeRead = (int) Math.min( bytesRemains, nextSegment.blockCount * blockSize );
		MappedByteBuffer buffer = scanChannel.map(
				FileChannel.MapMode.READ_ONLY, nextSegment.logicalOffset, toBeRead
		);
		buffer.load();  // try to load whole block FIXME: for performance tests only
		performanceLog.debug(
				"Loaded {} blocks ({} with {}/sec) of {} in {}",
				nextSegment.blockCount, getFileSizeNice( toBeRead ),
				getFileSizeNice( (long) ( ( (double) toBeRead ) * 1e9 / ( System.nanoTime() - startTime + 1 )  ) ),
				file.getName(), Utils.asHumanReadableDelay( startTime )
		);
		startTime = System.nanoTime();
		if ( buffer.hasArray() ) {
			// backed by byte buffer. Read at once
			hash64.update( buffer.array(), 0, toBeRead );
			performanceLog.debug(
					"Hashed {} blocks ({} with {}/sec) of {} AT ONCE in {}",
					nextSegment.blockCount, getFileSizeNice( toBeRead ),
					getFileSizeNice( (long) ( ( (double) toBeRead ) * 1e9 / ( System.nanoTime() - startTime + 1 )  ) ),
					file.getName(), Utils.asHumanReadableDelay( startTime )
			);
		} else {
			// no backed buffer. Read by relatively small buffers
			int toBeRead2 = toBeRead;
			if ( mirrorBuffer == null ) {
				mirrorBuffer = new byte[ blockSize ];
			}
			for ( int i = 0; i < nextSegment.blockCount; ++ i ) {
				int toBeMirrored = (int) Math.min( toBeRead, blockSize );
				if ( toBeMirrored == 0 ) {
					break;
				}
				buffer.get(mirrorBuffer, 0, toBeMirrored);
				hash64.update(mirrorBuffer, 0, toBeMirrored);
				toBeRead -= toBeMirrored;
			}
			performanceLog.debug(
					"Hashed {} blocks ({} with {}/sec) of {} BY MIRROR BUFFER in {}",
					nextSegment.blockCount, getFileSizeNice( toBeRead2 ),
					getFileSizeNice( (long) ( ( (double) toBeRead2 ) * 1e9 / ( System.nanoTime() - startTime + 1 ) ) ),
					file.getName(), Utils.asHumanReadableDelay( startTime )
			);
		}
		bytesRemains -= toBeRead;
		++ nextExtentIndex;
		if ( bytesRemains == 0 ) {
			scanChannel.close();
			hash = hash64.getValue();
			scanChannel = null;
			hash64 = null;
			mirrorBuffer = null;
		}
	}

	long getMaxExtentSize() {
		int maxSizeInBlocks = 0;
		if ( segments != null ) {
			for (FileSegment segment : segments) {
				maxSizeInBlocks = Math.max(maxSizeInBlocks, segment.blockCount);
			}
		}
		return ((long)maxSizeInBlocks) * blockSize;
	}

	boolean isNextExtent( int absBlockIndex ) {
		FileSegment fileSegment = getNextSegment();
		return fileSegment != null && fileSegment.physicalOffset == absBlockIndex;
	}

	private FileSegment getNextSegment() {
		return ( segments == null || segments.isEmpty() ) ? null : segments.get( nextExtentIndex );
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append( "File " ).append( file.getPath() );
		if ( segments != null ) {
			builder.append(" has the next ")
					.append(blockSize).append(" byte extents:");
			for (FileSegment fileSegment : segments) {
				builder.append('\n').append(fileSegment.toString());
			}
		}
		return builder.toString();
	}
}
