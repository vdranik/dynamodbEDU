package com.vdranik.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LocalDateTimeConverter implements DynamoDBTypeConverter<String, LocalDateTime> {

  private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  @Override
  public String convert(LocalDateTime localDateTime) {
    return localDateTime.format(formatter);
  }

  @Override
  public LocalDateTime unconvert(String s) {
    return LocalDateTime.parse(s, formatter);
  }
}
