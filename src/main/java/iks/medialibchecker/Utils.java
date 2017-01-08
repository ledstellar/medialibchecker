package iks.medialibchecker;

import java.util.*;
import static java.util.Calendar.DAY_OF_YEAR;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;

public class Utils {
	private Utils() {}

	public static String getDoubleDigits(int number){
		return (number < 10 ? "0" : "") + number;
	}

	/**
	 * Преобразует наносекунды в интервалы времени для прочтения человеком
	 *
	 * @param intervalNs интервал в наносекундах
	 * @return строка "XX лет, XX дней, чч:мм:сс.хххххх". Появление всех полей опционально
	 *         и рассчитывается как "минимальное, но ещё читабельное".
	 *         Наносекунды будут опущены, если появились как минимум дни
	 */
	public static String asHumanReadableInterval(long intervalNs){
		StringBuilder builder = new StringBuilder();
		asHumanReadableInterval(intervalNs, builder);
		return builder.toString();
	}

	public static void asHumanReadableInterval(long intervalNs, StringBuilder builder){
		if ( intervalNs > 1_000_000_000L ) {
			Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			calendar.setTimeInMillis(intervalNs / 1000000000L * 1000L );
			long nanoseconds = Math.abs(intervalNs - calendar.getTimeInMillis() * 1000000L );
			int years = calendar.get(YEAR) - 1970;
			if(years > 0){
				builder.append(years).append( " year" );
				if ( years > 1 ) {
					builder.append( 's' );
				}
				builder.append(' ');
			}
			int days = calendar.get(DAY_OF_YEAR) - calendar.getActualMinimum(DAY_OF_YEAR);
			if(days > 0){
				builder.append(days).append( " day" );
				if ( days > 1 ) {
					builder.append( 's' );
				}
				builder.append(' ');
			}
			// если интервал свыше секунды, то показываем в формате [ЧЧ][:ММ][:CC][.нс]
			if (calendar.get(HOUR_OF_DAY) > 0) {
				builder.append(getDoubleDigits(calendar.get(HOUR_OF_DAY))).append(':');
			}
			// здесь нельзя использовать SimpleDateFormat.format( "HH:mm:ss", calendar.getTime() )
			// так как calendar.getTime() безусловно переводит дату в локальную тайм-зону, что приводит к искажению количества часов
			builder.append(getDoubleDigits(calendar.get(MINUTE))).append(':');
			if (intervalNs < 5000000000L) {
				// для интервала до пяти секунд - добавляем наносекунды в виде десятичной мантиссы
				builder.append(getDoubleDigits(calendar.get(SECOND)))
						.append(String.format(".%09d", nanoseconds % 1000000000));
			} else {
				// для интервала свыше 0.5 секунд
				if (nanoseconds / 100000000 >= 5) {
					// добавляем секунду за счёт округления дробных долей секунды
					calendar.add(SECOND, 1);
				}
				builder.append(getDoubleDigits(calendar.get(SECOND)));
			}
		} else {
			if ( intervalNs < 1000L ) {
				builder.append(intervalNs).append(" ns");
			} else if (intervalNs / 1000L < 1000L ) {
				builder.append( ( (double)intervalNs) / 1000L ).append(" µs");
			} else {
				// если больше одной миллисекунды - то показываем в формате миллисекунд (целых плюс их тысячных = микросекунд)
				builder.append( ((double)( intervalNs / 1000L )) / 1000L ).append(" ms");
			}
		}
	}

	/**
	 * Преобразует задержку с указанного времени до текущего в читабельном для человека формате
	 *
	 * @param fromTimeNs время, которое вычитается из текущего для получения интервала (в наносекундах с системного таймера)
	 * @return задержка с указанного времени до текущего в читабельном для человека формате
	 * @see Utils#asHumanReadableInterval(long)
	 * @see System#nanoTime()
	 */
	public static String asHumanReadableDelay(long fromTimeNs){
		return asHumanReadableInterval(System.nanoTime() - fromTimeNs);
	}

	public static void asHumanReadableDelay(long fromTimeNs, StringBuilder builder ){
		asHumanReadableInterval(System.nanoTime() - fromTimeNs, builder);
	}

	public static long asMilliseconds(long fromTimeNs){
		return (System.nanoTime() - fromTimeNs) / 1000000L;
	}

	private static final String[] unitNames = { "bytes", "KB", "MB", "GB", "TB" };
	public static String getFileSizeNice( long sizeInBytes ) {
		String theUnitName = null;
		double size = sizeInBytes;
		for ( String unitName : unitNames ) {
			theUnitName = unitName;
			if ( size < 1024 ) {
				break;
			}
			size /= 1024.;
		}
		return "" + ( ( (int)( size * 100 ) )/ 100. ) + ' ' + theUnitName;
	}

}
