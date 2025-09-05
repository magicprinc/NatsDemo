package examples;

import jakarta.validation.constraints.PositiveOrZero;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Locale;
import java.util.function.IntPredicate;

import static java.nio.charset.StandardCharsets.*;

/// reusable utils
@Slf4j
public final class MagicUtils {
	public static final long MILLI_1K = 1000;
	public static final long NANO_IN_MILLI				= 1_000_000L;
	public static final long FIRST_ACCESS_NANOS  = System.nanoTime();
	public static final long FIRST_ACCESS_MILLIS = System.currentTimeMillis();

	public static final Path TEMP_DIR = Path.of(normPath(System.getProperty("java.io.tmpdir")));

	public static @PositiveOrZero long now () {
		return (System.nanoTime() - FIRST_ACCESS_NANOS) / NANO_IN_MILLI;// == toMilli
	}

	public static void close (@Nullable AutoCloseable closeable) {
		if (closeable != null){
			try {
				closeable.close();
			} catch (Throwable e){
				log.warn("close: failed to close ({}) {}", closeable.getClass().getName(), closeable, e);
			}
		}
	}

	public static void loop (long numberOfRepetitions, Runnable body){
		while (numberOfRepetitions-- > 0){
			body.run();
		}
	}

	public static boolean isEmpty (byte @Nullable [] items){ return items == null || items.length <= 0; }

	public static int len (byte @Nullable [] items){
		return items != null ? items.length : 0;
	}

	public static String toHex (byte @Nullable [] bytes) {
		return isEmpty(bytes) ? ""
				: HexFormat.of().formatHex(bytes);
	}

	public static String asStr (byte @Nullable [] b) {
		if (isEmpty(b)){ return ""; }
		try {
			return new String(b, UTF_8);
		} catch (Throwable e){
			log.error("asStr: failed to convert byte[] to UTF-8 str: ({}) {}", len(b), toHex(b));
			return new String(b, 0/*hiByte*/);// fallback to Latin1
		}
	}

	@SuppressWarnings({"ImplicitDefaultCharsetUsage", "ConstantConditions"})
	public static String asLatin1 (byte @Nullable [] b){
		final int len;
		if (b == null || (len = b.length) <= 0){ return ""; }
		return new String(b, 0/*hiByte*/, 0,len);
	}


	public static Thread execute (Runnable action) {
		return Thread.startVirtualThread(action);
	}

	public static String perfToString (long start, long end, long totalOperations) {
		assert start <= end : "toString: start ≤ end, but " + start + " > " + end;

		long now = now();
		if (now < end){// still going
			long total = end - start;
			long elapsed = now - start;
			long remain = end - now;

			return String.format(Locale.ENGLISH, "%d → %d : %.2f%%, op/s=%.2f",// %.3f
				elapsed, remain, remain * 100.0 / total, totalOperations * (double) MILLI_1K / elapsed);
		} else {// закончили ⇒ финальная статистика по замеру
			long elapsed = end - start;

			return String.format(Locale.ENGLISH, "%d, op/s=%.2f", elapsed, totalOperations * (double) MILLI_1K / elapsed);
		}
	}

	public static String normPath (@Nullable Object filePath) {
		return trimRight(trim(filePath), MagicUtils::isPathSeparatorOrSpace)
			.replace('\\', '/');
	}

	public static String trim (@Nullable Object o) {
		if (o instanceof String s)
			return trim(s);
		else if (o instanceof CharSequence s)
			return trim(s);
		else if (o == null)
			return "";
		else
			return trim(o.toString());
	}
	public static String trim (@Nullable String s) {
		if (s == null){ return ""; }
		int beginIndex = 0, endIndex = s.length() - 1;
		while (beginIndex <= endIndex && isSpaceOrZeroWidth(s.charAt(beginIndex)))
			++beginIndex;
		while (endIndex > beginIndex && isSpaceOrZeroWidth(s.charAt(endIndex)))
			--endIndex;
		return s.substring(beginIndex, endIndex + 1);
	}
	public static String trim (@Nullable CharSequence s) {
		if (s == null){ return ""; }
		int beginIndex = 0, endIndex = s.length() - 1;
		while (beginIndex <= endIndex && isSpaceOrZeroWidth(s.charAt(beginIndex)))
			++beginIndex;
		while (endIndex > beginIndex && isSpaceOrZeroWidth(s.charAt(endIndex)))
			--endIndex;
		return s.subSequence(beginIndex, endIndex + 1).toString();
	}

	public static boolean isSpace (int cp) {
		return cp <= ' '// as trim does
			|| Character.isSpaceChar(cp)
			|| Character.isWhitespace(cp)
			|| cp == 0x85// ICU: 0085	×	COMMON	[LATIN_1_SUPPLEMENT]	 T:Other, Control	 # NEXT LINE (NEL)
			;
		//|| UCharacter.isSpaceChar(ch) || UCharacter.isWhitespace(ch)
		//|| ch == 0x200B;// ZERO WIDTH SPACE
	}

	public static boolean isSpaceOrZeroWidth  (int cp) {
		return isSpace(cp)
			|| cp == 0x200B// Zero Width space
			|| cp == 0x200C// Zero Width non-joiner
			|| cp == 0x200D// Zero Width joiner
			|| cp == 0xFEFF// 0feff	×	COMMON	[ARABIC_PRESENTATION_FORMS_B]	 T:Other, Format	 # ZERO WIDTH NO-BREAK SPACE
			;
	}

	public static boolean isPathSeparator (int cp) {
		return cp == '/'
			|| cp == '\\';
	}

	public static boolean isPathSeparatorOrSpace (int cp) {
		return isPathSeparator(cp) || isSpaceOrZeroWidth(cp);
	}

	public static String trimRight (@Nullable CharSequence s, IntPredicate shouldTrim){
		if (s == null){ return ""; }
		int max = s.length() - 1;
		int i = max;
		for (; i>=0; i--){
			char c = s.charAt(i);
			if (!shouldTrim.test(c)){ break;}
		}
		return i >= max ? s.toString() // == → не сдвинулись ⇒ нечего trim (as is)
			: s.subSequence(0, i+1).toString();
	}

}