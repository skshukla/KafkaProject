package tmp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class SampleOne {

  @Test
  public void m1() throws Exception{

    final int nThreads = 3;
    final int nTasks = 10;

    final Runnable r = () ->{
      System.out.println(String.format("Thread [%s] - Okay", Thread.currentThread().getName()));
      try {
        Thread.currentThread().sleep(2 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    };


    ExecutorService manager = Executors.newFixedThreadPool(nThreads);
    for(int i=0; i< nTasks; i++) {
      manager.submit(r);
    }
    manager.awaitTermination(10, TimeUnit.SECONDS);
  }

}
