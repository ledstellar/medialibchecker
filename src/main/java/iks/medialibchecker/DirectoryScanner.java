package iks.medialibchecker;

import org.slf4j.*;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static iks.medialibchecker.Utils.getFileSizeNice;

class DirectoryScanner implements Runnable {
	private static final Logger log = LoggerFactory.getLogger( DirectoryScanner.class );
	private static final Logger performanceLog = LoggerFactory.getLogger( "performance.DirectoryScanner" );

	private final File rootDirectory;
	private final Path rootDirectoryPath;

	/** First directory segment to directory info navigable map */
	private ConcurrentSkipListMap< Integer, DirectoryInfo > directoriesToBeRead = new ConcurrentSkipListMap<>();

	/**
	 * Directory file start block to directory info map.
	 * Needs to find out directory nearest to last scanned one.
	 * The map is populated by asynchronous directory segment map scanner process and is depopulated by directory content
	 * scanner process.
	 **/
	private ConcurrentSkipListMap< Integer, DirectoryInfo > directoryPlacementMap = new ConcurrentSkipListMap<>();

	/** The queue to scan directory segment maps */
	private LinkedBlockingQueue< FileInfo > directoryMapScanQueue = new LinkedBlockingQueue<>();

	private ConcurrentSkipListMap< Integer, DirectoryInfo > directoryExtentMap = new ConcurrentSkipListMap<>();
	private TreeMap< Integer, FileInfo > fileExtentMap = new TreeMap<>();
	private ArrayList< DirectoryInfo > preparedDirectories = new ArrayList<>();

	private ReentrantLock lock = new ReentrantLock();
	private Condition extentMapScannerDone = lock.newCondition();
	private String loggerSuffix;

	DirectoryScanner( String directoryRootPath )  {
		rootDirectory = new File( directoryRootPath );
		rootDirectoryPath = rootDirectory.toPath();
		loggerSuffix = directoryRootPath.replaceAll( "(/[^/])[^/]+(?=/)", "$1" );
	}

	@Override
	public void run() {
		Thread.currentThread().setName( "SCAN-" + loggerSuffix );
		try {
			gatherFilesAndExtents();
			reportStats();
			calcFileHashes();
		} catch ( InterruptedException ie ) {
			// Just finish the work
		}
	}

	private void calcFileHashes() {
		// scan from disk start to the end and vice versa making file checksums
		// single thread implementation. TODO: make it multithreaded
		for ( int scanPassNumber = 0; ! Thread.currentThread().isInterrupted() && ! fileExtentMap.isEmpty(); ++ scanPassNumber ) {
			log.info( "File map checksum scan #{}...", scanPassNumber );
			// it's mandatory to use iterator to avoid ConcurrentModificationException while remove elements from the Map
			Iterator< Map.Entry<Integer, FileInfo> > iterator = fileExtentMap.entrySet().iterator();
			while ( ! Thread.currentThread().isInterrupted() && iterator.hasNext() ) {
				Map.Entry<Integer, FileInfo> fileExtentEntry = iterator.next();
				FileInfo fileInfo = fileExtentEntry.getValue();
				int absExtentIndex = fileExtentEntry.getKey();
				if (fileInfo.isNextExtent(absExtentIndex)) {
					// Ok to scan this block and remove it from extent map
					try {
						fileInfo.calcHash();
					} catch ( IOException ioe ) {
						log.error( String.format( "Error while reading %s", fileInfo.file.getPath() ), ioe );
					}
					iterator.remove();
				}
			}
		}
	}

	private void gatherFilesAndExtents() throws InterruptedException {
		ExtentMapScanner extentMapScanner = new ExtentMapScanner(
				rootDirectory.toPath(), directoryExtentMap, fileExtentMap, lock, extentMapScannerDone
		);
		Thread extentMapScannerThread = new Thread( extentMapScanner );
		extentMapScannerThread.setName( "EXTENT-" + loggerSuffix );;
		extentMapScannerThread.start();
		DirectoryInfo rootDir = new DirectoryInfo( rootDirectory );
		rootDir.readContent( null );
		extentMapScanner.addDirectory( rootDir );
		int currentBlock = 0;
			while ( true ) {
				if (directoryExtentMap.isEmpty()) {
					log.trace( "Extent map is empty. Going to lock" );
					lock.lock();
					try {
						while (directoryExtentMap.isEmpty() && ! extentMapScanner.isIdle()) {
							extentMapScannerDone.await();
							log.trace("Notify received");
						}
						if (directoryExtentMap.isEmpty() && extentMapScanner.isIdle()) {
							// We've done. Stop directory extent mapper
							log.trace( "Extent mapper thread is done. Terminating it" );
							extentMapScannerThread.interrupt();
							// and now all the directories prepared for sure
							return;
						}
					} catch ( InterruptedException ie ) {
						log.trace( "Interrupted" );
						throw ie;
					} finally {
						lock.unlock();
					}
				}
				// directoryExtentMap is not empty here
				Integer greater = directoryExtentMap.ceilingKey(currentBlock);
				Integer less = directoryExtentMap.floorKey(currentBlock);
				// choose nearest to the current block. The block can't be null as we have at least one entry in the map
				int nextKey = (greater == null) ? less : (less == null) ? greater :
						(Math.abs(currentBlock - less) > Math.abs(currentBlock - greater)) ? greater : less;
				DirectoryInfo nextDirectory = directoryExtentMap.remove(nextKey);
				log.trace( "Nearest block index is {} with directory \"{}\"", nextKey, nextDirectory.file.getName() );
				nextDirectory.readContent( rootDirectoryPath );
				extentMapScanner.addDirectory(nextDirectory);
				// actually the directory will be prepared for sure after extentMapScanner will be done only. But ...
				preparedDirectories.add( nextDirectory );
				currentBlock = nextDirectory.segments.get( 0 ).physicalOffset;
			}
	}

	private void reportStats() {
		long startTime = System.nanoTime();
		int totalFiles = 0;
		long totalFileSize = 0;
		long maxExtentSize = 0;
		for ( DirectoryInfo directory : preparedDirectories ) {
			ArrayList< FileInfo > fileInfos = directory.containingFiles;
			if ( fileInfos != null ) {
				totalFiles += fileInfos.size();
				for ( FileInfo fileInfo : fileInfos ) {
					totalFileSize += fileInfo.file.length();
					maxExtentSize = Math.max( maxExtentSize, fileInfo.getMaxExtentSize() );
				}
			}
		}
		log.info(
				"Found {} directories with {} files and total size {}. Max extent size is {}",
				preparedDirectories.size(), totalFiles, getFileSizeNice( totalFileSize ),
				getFileSizeNice( maxExtentSize )
		);
		performanceLog.debug( "File report done in {}", Utils.asHumanReadableDelay(startTime) );
	}
}
