package iks.medialibchecker;

/** Segment of file that places in sequenced blocks on drive */
class FileSegment {
	/** Offset of the first block within file */
	int logicalOffset;
	/** Offset of the first block within drive */
	int physicalOffset;
	/** Size of the segment in blocks (size of every block is drive dependent) */
	int blockCount;

	FileSegment ( int logicalOffset, int physicalOffset, int blockCount ) {
		this.logicalOffset = logicalOffset;
		this.physicalOffset = physicalOffset;
		this.blockCount = blockCount;
	}

	@Override
	public String toString() {
		return "logical offset: " + logicalOffset
				+ "\t physicalOffset: " + physicalOffset
				+ "\t size in blocks: " + blockCount;
	}
}
