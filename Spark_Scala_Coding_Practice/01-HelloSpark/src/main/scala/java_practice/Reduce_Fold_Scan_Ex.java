package java_practice;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Reduce_Fold_Scan_Ex {
    public static void main(String[] args) throws IOException {

        System.out.println(
                Stream.of("Java","Scala","Python")
                .map(e->e.concat("_"))
                .map(String::toUpperCase)
                .reduce("reduce ->",(acc,e) -> acc.concat(e))
                .toString()
        );
        System.out.println(
                Stream.of("Java","Scala","Python")
                .map(e->e.concat("_"))
                .map(String::toUpperCase)
                .reduce("courses ->",(acc,e) -> acc.concat(e))
                .toString()
        );


                Stream.of("scan ", "left ", "too ").parallel()
                .collect(ArrayList<String>::new,
                        (list, element) -> list.add(element),
                        ArrayList<String>::addAll).forEach(System.out::println);

        // reduce with 1 arg
        List<String> list = Arrays.asList("Java","Scala","Python");

        OptionalInt sum = list.stream().mapToInt(String::length).reduce(Integer::sum);
        System.out.println("reduce with mapInt :: "+sum.getAsInt());


        /**   VIDEO    https://www.youtube.com/watch?v=oWlWEKNM5Aw&t=1448s
         *  1. sequential stream test : COMBINER has no role    :check output
         *  2. parallel stream test : COMBINER Comes into play  :check output
         *  3. unlike in other FP languages that have separate mapper and reducer  ::
         *                    java reduce is a combination of ==>  map + reduce
         *
         * Differences ::
         * ===========================================================================
         * collect() can I> only work with mutable result objects.    II> order preserving
         * reduce() is  I>  designed to work with immutable result objects. II> order preserving
         *
         * think in terms of concatenating strings
         * reduce("_",(acc,e) -> acc.concat(e) )  =>each tome new value is being assigned to acc [Immutable]
         *
         * 
         *
         *
         */


        System.out.println("stream test ");
        int streamResult = list.stream().parallel().reduce(
                0,
                (total,s) -> {
                    System.out.println(Thread.currentThread().getName()+"  accumulator: total[" + total + "] s[" + s +
                            "] s" +
                            ".codePointAt(1)[" + s.codePointAt(1) + "]");
                    return total + s.codePointAt(1);
                    },
                (a,b) -> {
                    System.out.println(Thread.currentThread().getName()+"  combiner: a[" + a + "] b[" + b + "]");
                    return 100000;
                }
        );
        
        System.out.println("streamResult: " + streamResult);

        String addAllString = list.stream()
                .parallel()
                .reduce("__",
                        (acc,e) -> acc.concat(e),
                        (e1,e2)-> e1.concat(e2) );


        System.out.println("addAllString parallel:: "+addAllString);

        String addAllStringSeq = list.stream()
                .reduce("__",
                        (acc,e) -> acc.concat(e),
                        (e1,e2)-> e1.concat(e2) );


        System.out.println("addAllStringSeq seq:: "+addAllStringSeq);



        ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
        System.out.println(forkJoinPool.getPoolSize());
        




        



    }
}

class Task extends RecursiveTask{


    @Override
    protected Object compute() {
        return null;
    }
}