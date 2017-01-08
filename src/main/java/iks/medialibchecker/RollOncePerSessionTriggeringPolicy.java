package iks.medialibchecker;

import ch.qos.logback.core.rolling.TriggeringPolicyBase;

import java.io.File;
import java.util.*;

public class RollOncePerSessionTriggeringPolicy<E> extends TriggeringPolicyBase<E> {
	private static Set< File > triggered = Collections.synchronizedSet( new HashSet<>() );

	@Override
	public boolean isTriggeringEvent(File activeFile, E event) {
		// roll the first time when the event gets called
		if ( ! triggered.contains(activeFile)) {
			triggered.add( activeFile );
			return true;
		}
		return false;
	}
}