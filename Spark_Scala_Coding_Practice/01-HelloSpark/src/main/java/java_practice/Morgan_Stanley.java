package java_practice;

import java.util.Comparator;

/**
 * created by ANEK on Sunday 2/13/2022 at 9:33 PM
 */

public class Morgan_Stanley {
    public static void main(String[] args) {
        //1.
        String[] arr = {"Bangalore","Pune","Hyderabad"};
        //2.
        int x = 5; int y = 0 ;
       //  int iVal = y!=0 &  x/y > 0 ?1:0;
       // System.out.println(iVal);


        //3.
        class A {}
        class B extends  A {}
        class C extends B {}


        B b = new B();
        System.out.println(b instanceof C );

       // 4 .
        Runnable r = () -> System.out.println("r "+Thread.currentThread().getName());
        Runnable r1 = () -> {System.out.println("r1 "+Thread.currentThread().getName()); };
        new Thread(r1).start();

        /*
        *
        * Which design pattern from JDK itself is the collection sort?
          One example of Strategy pattern from JDK itself is a Collections. sort() method and Comparator interface,
*  which is a strategy interface and defines strategy for comparing objects
        *
        * */
       
    }

    static class MySort implements Comparator{


        @Override
        public int compare(Object o1, Object o2) {
            return 0;
        }
    }

}
