package iks.medialibchecker;

import org.slf4j.*;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;

/**
 * Directory info structure.
 *
 * As we can't control the absolute block of directory while reading it then
 * we doesn't interest in exact segment map of directory file. It's enough for us to know just the first
 * and the last directory block device offsets. The latter needs to find the next nearest directory.
 */
class DirectoryInfo extends FileInfo {
	private static final Logger performanceLog = LoggerFactory.getLogger( "performance.DirectoryInfo" );
	private static final Logger log = LoggerFactory.getLogger( DirectoryInfo.class );
	ArrayList< FileInfo > containingFiles;

	DirectoryInfo( File file ) {
		super( null, file );
	}

	void readContent( Path rootDirectoryPath ) {
		long startTime = System.nanoTime();
		log.trace( "Reading content of directory '{}'", file.getPath() );
		File[] innerFiles = file.listFiles();
		if ( innerFiles != null ) {
			containingFiles = new ArrayList<>(innerFiles.length);
			for (File file : innerFiles) {
				if (file.isDirectory()) {
					DirectoryInfo dirInfo = new DirectoryInfo(file);
					containingFiles.add(dirInfo);
				} else {
					containingFiles.add(new FileInfo(super.file, file));
				}
			}
		}
		if ( performanceLog.isDebugEnabled() ) {
			performanceLog.debug(
					"{} files of directory \"{}\" has been read in {}",
					containingFiles == null ? 0 : containingFiles.size(),
					rootDirectoryPath == null ? file.getPath() : rootDirectoryPath.relativize( file.toPath() ),
					Utils.asHumanReadableDelay( startTime )
            );
		}
	}

}
