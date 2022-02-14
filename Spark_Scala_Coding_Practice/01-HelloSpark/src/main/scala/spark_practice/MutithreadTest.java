package spark_practice;


import org.omg.CORBA.INTERNAL;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MutithreadTest {
    public static void main(String[] args) {

        ExecutorService service = Executors.newFixedThreadPool(2);

        try{
            Future<Integer> f1= service.submit(new JobRunner());
           // System.out.println("f1 get :: "+Thread.currentThread().getName()+" --> "+f1.isDone());

            Future<Integer> f2 = service.submit(new JobRunner());
           // System.out.println("f2.get:: "+Thread.currentThread().getName()+" --> "+f2.isDone());
            System.out.println("job completed");
           }
        catch (Exception e){
            System.out.println("in main() exception ");

        }
          service.shutdown();
    }
}

class JobRunner implements Callable {

    public JobRunner(){}


    @Override
    public Integer call() throws Exception{
        Integer ret = 0;
        try{
            createTable();

        }catch (Exception e) {
            ret = -1;
            System.out.println("call exception  :: "+Thread.currentThread().getName());
            throw new RuntimeException();
        }
         return ret ;
    }

    private void createTable(){
     try {
         System.out.println("in createTable :: "+Thread.currentThread().getName());
        throw new RuntimeException();

    }catch (Exception e ){
         System.out.println("in createTable() exception catch::"+Thread.currentThread().getName());
         throw new RuntimeException("in createTable() exception catch:: "+Thread.currentThread().getName());

     }

    }  //createTable




    }


