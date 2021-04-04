package com.sachin.work.kafka.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.stream.IntStream;

public class GenUtil {

  private static final String str = "abcdefghijklmnopqartuvwxyz";

  public static final String getRandomName(final int nChars) {
    final StringBuilder sb = new StringBuilder();
    IntStream.range(0, nChars).forEach( i -> {
      char c = str.charAt(getRandomNumBetween(0, str.length()));
      sb.append(getRandomTrueOrFalse() ? Character.toUpperCase(c) : c);
    });
    return sb.toString();
  }

  public static final int getRandomNumBetween(final int start, final int end) {
    return start + (new Random().nextInt(end - start));
  }
  public static boolean getRandomTrueOrFalse() {
    return new Random().nextBoolean();
  }

  public static String getDateToStr(final Date d) {
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    return sdf.format(d);
  }

}
