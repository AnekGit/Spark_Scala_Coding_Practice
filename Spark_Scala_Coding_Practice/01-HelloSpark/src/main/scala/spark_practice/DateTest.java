package spark_practice;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

public class DateTest {
    public static void main(String[] args) throws IOException {

        //System.out.println(Math.abs(Integer.parseInt("A") - Integer.parseInt("B")) );
        "anekkk".chars().forEach(System.out::println);
        System.out.println("anekkk".chars());
        System.out.println("anekkumkk".chars()
               .mapToObj(x -> (char)x)
                .collect(Collectors.groupingBy(x -> x,Collectors.counting()))
                .entrySet()
                .stream()
                .max(Entry.comparingByValue())
                .get()
                .getKey()
        );
        String date = "20210103";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate convertedDate = LocalDate.parse(date,DateTimeFormatter.ofPattern("yyyyMMdd"));
        System.out.println("convertedDate :: "+convertedDate);

        LocalDate convertedDate1 =
                convertedDate.withDayOfMonth(convertedDate.getMonth().length(convertedDate.isLeapYear()));

        System.out.println("last date1 :: "+convertedDate1.getDayOfMonth());

        String date2 = "20200131";
        LocalDate convertedDate2 = LocalDate.parse(date2,formatter);
       // convertedDate2 = convertedDate2.withDayOfMonth(convertedDate2.getMonth().minLength());
        System.out.println(convertedDate2);

        System.out.println("get 1 day in month :: "+convertedDate2.withDayOfMonth(1).getDayOfMonth());

        System.out.println("minus :: "+(convertedDate2.getDayOfMonth() - convertedDate1.getDayOfMonth() ));

        int freqNum = 7 ;
        String pStart = "20201221";
        String pEnd = "20201228";
        String pEndTest = "20201229";


        LocalDate pEndTestLocal = LocalDate.parse(pEndTest,DateTimeFormatter.ofPattern("yyyyMMdd"));
        LocalDate pLocalEnd = LocalDate.parse(pEnd,DateTimeFormatter.ofPattern("yyyyMMdd"));
       // LocalDate partEnd = pLocalEnd.minusDays(freqNum);
        LocalDate pLocalStart =  LocalDate.parse( pStart,DateTimeFormatter.ofPattern("yyyyMMdd") );

        System.out.println("pLocalEnd.getDayOfMonth() :: "+  (pLocalEnd.getDayOfMonth())+1 );   // +1 doesnot work
        System.out.println("pLocalStart.getDayOfMonth() :: "+pLocalStart.getDayOfMonth());

        int p = pLocalEnd.getDayOfMonth();
        int check = p+1;
        int check2 = pEndTestLocal.getDayOfMonth();

        boolean ss = (pLocalEnd.getDayOfMonth()+1) == pEndTestLocal.getDayOfMonth() ;
        System.out.println("ss : : "+ss);
        System.out.println("p => "+(p+1));


        boolean b = ( pLocalEnd.getDayOfMonth() - pLocalStart.getDayOfMonth() ) == freqNum;
        System.out.println("check :: "+  b);


        List<String> l = new LinkedList();
        l.add("anek");l.add("prince");l.add("nishant");l.add("Chorwa");
        ListIterator<String> listIterator = l.listIterator();

        while(listIterator.hasNext()){
            String var = listIterator.next();
            if(var.equals("anek")){
                System.out.println("hasnext :: "+var);
                System.out.println("hasPrevious: : "+listIterator.previous());
                break;
            }

        }
        while(listIterator.hasPrevious()){
            String varPrev = listIterator.previous();
            System.out.println("varPrev : : "+varPrev);
        }


    }

}
