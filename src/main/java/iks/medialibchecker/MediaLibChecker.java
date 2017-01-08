package iks.medialibchecker;

import org.slf4j.*;

public class MediaLibChecker {
	private static final Logger log = LoggerFactory.getLogger( MediaLibChecker.class );
	private static Thread mainThread;

	public static void main( String[] args ) {
		mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook( new Thread() {
			@Override public void run() {
				log.info("Interrupting processing by shutdown hook...");
				mainThread.interrupt();
			}
		});

		if ( args.length != 1 ) {
			// TODO: take multiple media source root paths
			System.err.println( "Usage: medialibchecker <media source root path>" );
			System.exit( 1 );
		}
		DirectoryScanner directoryScanner = new DirectoryScanner( args[0] );
		directoryScanner.run();
	}
}
