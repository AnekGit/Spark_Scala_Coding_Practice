import net.sf.cglib.core.Local;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;

/**
 * created by ANEK on Thursday 8/5/2021 at 3:49 PM
 */

public class JavaDateUtils {
    public static void main(String[] args) {


        String cob_dt = "2021-05-10";

        String lDate  = LocalDate.parse(cob_dt, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                                        .with(TemporalAdjusters.lastDayOfMonth()).toString();
        System.out.println("last day of month :: "+lDate);

        String fDate = LocalDate.parse(cob_dt,DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                                .with(TemporalAdjusters.firstDayOfMonth()).toString();
        System.out.println("first date : : "+fDate);

        String prevMonthEnd = LocalDate.parse(cob_dt,DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                                       .withDayOfMonth(1).minusDays(1).toString();
        System.out.println("first date : : "+prevMonthEnd);




    }

}
