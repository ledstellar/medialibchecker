package iks.medialibchecker;

import org.slf4j.*;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static iks.medialibchecker.ExtentMapScanner.OutputParserState.*;

/**
 * File extents (filesystem's file placement map) scanner.
 * File extents are needed to minimize HDD head's movement during file checksum scans to maximize speed of scan.
 * Also file extent maps used to minimize head movements during folder scans.
 */
class ExtentMapScanner implements Runnable {
	private static final Logger log = LoggerFactory.getLogger( ExtentMapScanner.class );
	private static final Logger performanceLog = LoggerFactory.getLogger( "performance.ExtentMapScanner" );
	private static final String FILE_FRAG_UTILITY_COMMAND[] = new String[] { "filefrag", "-e" };
	private static final String EXTENTS_HEADER = " ext:     logical_offset:        physical_offset: length:   expected: flags:";
	private static final Integer EXPECTED_SEGMENTS_COUNT = 4;

	enum  OutputParserState {
		LOOKING_FOR_FILE_HEADER,
		BYPASS_EXTENTS_HEADER,
		READING_EXTENTS
	}

	/** Result extent map for directories */
	private ConcurrentSkipListMap< Integer, DirectoryInfo > directoryExtentMap;

	/**
	 * Directories count that can be still processed.
	 * If zero then all supplied directories has been processed and the results (if any) has been returned.
	 **/
	private volatile int toBeProcessed;

	/** Result extent map for non-directory files */
	private TreeMap< Integer, FileInfo > fileExtentMap;

	/** Queue of directories for allocation map to be read */
	private LinkedBlockingQueue< DirectoryInfo > directoriesToBeProcessed = new LinkedBlockingQueue<>();

	private ReentrantLock lock;
	private Condition directoriesProcessedCondition;
	private Path rootDirectory;

	ExtentMapScanner(
			Path rootDirectory,
			ConcurrentSkipListMap< Integer, DirectoryInfo > directoryExtentMap,
			TreeMap< Integer, FileInfo > fileExtentMap,
			ReentrantLock lock,
			Condition directoriesProcessedCondition ) {
		this.rootDirectory = rootDirectory;
		this.fileExtentMap = fileExtentMap;
		this.directoryExtentMap = directoryExtentMap;
		this.directoriesProcessedCondition = directoriesProcessedCondition;
		this.lock = lock;
	}

	void addDirectory( DirectoryInfo directory ) {
		++ toBeProcessed;
		directoriesToBeProcessed.offer( directory );   // nonblocking method as we didn't restrict queue size
	}

	boolean isIdle() {
		return toBeProcessed == 0;
	}

	@Override
	public void run() {
		log.debug( "Extent map scanner thread started" );
		try {
			do {
				DirectoryInfo directory = directoriesToBeProcessed.poll();
				if ( directory == null ) {
					// signal to producer thread that we going to sleep.
					// If it already sleeps (due to absence of new directories)
					// then it should wake up and interrupt this thread
					log.trace( "No more directories. Notifying directory scan thread" );
					lock.lock();
					try {
						directoriesProcessedCondition.signalAll();
					} finally {
						lock.unlock();
					}
					directory = directoriesToBeProcessed.take();
				}
				try {
					runExternalFileMappingCommand(directory);
				} catch ( IOException ioe ) {
					log.error( String.format( "I/O error processing directory \"%s\"", directory.file.getPath() ), ioe );
				} catch ( Throwable th ) {
					log.error( String.format( "Error processing firectory \"%s\"", directory.file.getPath() ), th );
				}
				finally {
					-- toBeProcessed;
				}
			} while ( ! Thread.interrupted() );
		} catch ( InterruptedException ignore ) {
			log.trace( "Interrupted. Exiting" );
			// just finish
		}
	}

	private String getCommand() {
		return String.join( " " , (CharSequence[]) FILE_FRAG_UTILITY_COMMAND);
	}

	private void runExternalFileMappingCommand(DirectoryInfo directory) throws IOException {
		if ( directory.containingFiles == null ) {
			log.trace( "'{}' is an empty directory", directory.file.getPath() );
			return;
		}
		log.trace( "Scanning extent map for '{}'", directory.file.getPath() );
		long startTime = System.nanoTime();
		HashMap< String, FileInfo > filesMap = new HashMap<>( directory.containingFiles.size() );
		String[] commandLine = new String[ FILE_FRAG_UTILITY_COMMAND.length + directory.containingFiles.size() ];
		{
			System.arraycopy( FILE_FRAG_UTILITY_COMMAND, 0, commandLine, 0, FILE_FRAG_UTILITY_COMMAND.length );
			int i = FILE_FRAG_UTILITY_COMMAND.length;
			for ( FileInfo fileInfo : directory.containingFiles ) {
				filesMap.put( fileInfo.getName(), fileInfo );
				commandLine[ i ++ ] = fileInfo.getName();
			}
		}
		ProcessBuilder builder = new ProcessBuilder( commandLine );
		builder.redirectError( ProcessBuilder.Redirect.to( new File( "logs/filefrag.error.log" ) ) );
		builder.directory( directory.file );
		log.trace( "Starting filefrag for '{}'", directory.file.getName() );
		Process process = builder.start();
		BufferedReader in = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
		String str = null;
		OutputParserState state = OutputParserState.LOOKING_FOR_FILE_HEADER;
		String fileName = null;
		int blockSize = 0;
		ArrayList< FileSegment > segments = null;
		try {
			while ((str = in.readLine()) != null) {
				switch (state) {
					case LOOKING_FOR_FILE_HEADER:
						if (!str.startsWith("File size of ")) {
							continue;
						}
						fileName = str.substring("File size of ".length(), str.lastIndexOf(" is "));
						if (!str.endsWith(" bytes)")) {
							throw new IllegalStateException(
									"File size header of " + getCommand() +
											" should ends with 'NNNN bytes)' but ends as:\n" + str
							);
						}
						log.trace("Found file name \"{}\" in output", fileName);
						blockSize = Integer.parseInt(
								str.substring(
										str.lastIndexOf(" of ") + " of ".length(),
										str.lastIndexOf(' ')
								)
						);
						log.trace("Block size for file \"{}\" is {}", fileName, blockSize);
						state = BYPASS_EXTENTS_HEADER;
						break;
					case BYPASS_EXTENTS_HEADER:
						if (!EXTENTS_HEADER.equals(str)) {
							throw new IllegalStateException(
									"File extents header of " + getCommand() +
											" should be '" + EXTENTS_HEADER + "' but actually is:\n"
											+ str
							);
						}
						state = READING_EXTENTS;
						segments = new ArrayList<>(EXPECTED_SEGMENTS_COUNT);
						break;
					case READING_EXTENTS:
						if (str.startsWith(fileName)) {
							if (!str.endsWith(" found")) {
								throw new IllegalStateException(
										"File extents bottomline of " + getCommand() +
												" should ends with ' found' but ends as:\n"
												+ str
								);
							}
							FileInfo fileInfo = filesMap.get(fileName);
							if (fileInfo == null) {
								throw new IllegalStateException(
										"Internal error. Unexpected file in output of " + getCommand() +
												": '" + fileName + "'"
								);
							}
							fileInfo.setBlockSize(blockSize);
							// sort IN REVERSE logical block sequence order
							Collections.sort(segments, (x, y) -> y.logicalOffset - x.logicalOffset );
							fileInfo.setExtentMap(segments);
							if (fileInfo.file.isDirectory()) {
								// place directory in extent map
								log.trace(
										"Adding directory \"{}\" (extents {}:{}) to extent map",
										fileInfo.getName(),
										segments.get(segments.size() - 1).physicalOffset,
										segments.get(0).physicalOffset
								);
								directoryExtentMap.put(segments.get(segments.size() - 1).physicalOffset, (DirectoryInfo) fileInfo);
							} else {
								// place general file in extent map
								log.trace(
										"Adding file \"{}\" ({} extents) to extent map",
										fileInfo.getName(),
										segments.size()
								);
								for (FileSegment fileSegment : segments) {
									fileExtentMap.put(fileSegment.physicalOffset, fileInfo);
								}
							}
							state = OutputParserState.LOOKING_FOR_FILE_HEADER;
							break;
						}
						String[] fields = str.split("[ :.]+");
						if (fields.length < 6) {
							throw new IllegalStateException(
									"No enought fields in extent map of " + getCommand() +
											". Should be at least 5, but actual output is:\n"
											+ str
							);
						}
						try {
							segments.add(new FileSegment(
											Integer.parseInt(fields[2]),
											Integer.parseInt(fields[4]),
											Integer.parseInt(fields[6])
									)
							);
						} catch (NumberFormatException nfe) {
							throw new IllegalStateException(
									"Unexpected extent description format in output of " + getCommand() +
											". The line is:\n"
											+ str
							);
						}
				}
			}
		} catch ( Throwable th ) {
			throw new IllegalStateException( String.format( "Error parsing filefrag output string \"%s\"", str ), th );
		}
		if ( performanceLog.isDebugEnabled() ) {
			Path directoryPath = directory.file.toPath();
			performanceLog.debug(
					"Extent map for {} files in directory \"{}\" aquired in {}",
					directory.containingFiles.size(),
					directoryPath.equals( rootDirectory ) ? directoryPath : rootDirectory.relativize( directoryPath ),
					Utils.asHumanReadableDelay(startTime)
			);
		}
		// do not wait for process to finish (if it didn't yet)
	}
}
