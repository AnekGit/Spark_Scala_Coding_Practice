package spark_practice;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;
import java.util.Iterator;
import java.util.stream.IntStream;

public class StreamEx {
    public static void main(String[] args) {


        String date = "20210103";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate convertedDate = LocalDate.parse(date,formatter);

        LocalDate convertedDate1 =
                convertedDate.withDayOfMonth(convertedDate.getMonth().length(convertedDate.isLeapYear()));

        System.out.println("last date1 :: "+convertedDate1.getDayOfMonth());

        String date2 = "20200131";
        LocalDate convertedDate2 = LocalDate.parse(date2,formatter);
       // convertedDate2 = convertedDate2.withDayOfMonth(convertedDate2.getMonth().minLength());
        System.out.println(convertedDate2);

        System.out.println("get 1 day in month :: "+convertedDate2.withDayOfMonth(1).getDayOfMonth());

        System.out.println("minus :: "+(convertedDate2.getDayOfMonth() - convertedDate1.getDayOfMonth() ));






    }

}
