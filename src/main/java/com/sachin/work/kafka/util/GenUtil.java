package com.sachin.work.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;



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

  public static List<String> getJsonFromCSVFile(final File file) throws IOException {
    final List<Map<?, ?>> dataList = readObjectsFromCsv(file);
    return writeAsJson(dataList);
  }

  public static final void println(final String s) {
    System.out.println(String.format("[%s - %s] :%s", getDateToStr(new Date()), Thread.currentThread().getName(), s));
  }

  public static final void printlnErr(final String s) {
    System.err.println(String.format("[%s - %s] :%s", getDateToStr(new Date()), Thread.currentThread().getName(), s));
  }


  public static List<Map<?, ?>> readObjectsFromCsv(final File file) throws IOException {
    final CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
    final CsvMapper csvMapper = new CsvMapper();
    final MappingIterator<Map<?, ?>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);

    return mappingIterator.readAll();
  }

  public static List<String> writeAsJson(final List<Map<?, ?>> dataList) {
    return dataList.stream().map(e -> {
      return writeAsJson(e);
    }).filter(e -> Objects.nonNull(e)).collect(Collectors.toList());

  }

  public static String writeAsJson(final Map<?, ?> data) {
    final ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(data);
    } catch (JsonProcessingException ex) {
      return null;
    }
  }

}
