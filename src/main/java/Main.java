import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import receipts.case_objects.Receipt;
import receipts.conf.DataSource;
import scala.Tuple2;

import java.io.IOException;

import static receipts.service.ReceiptService.setupReceiptsDataRdd;
import static receipts.service.ReceiptService.stateTotalCount;

public class Main {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "c:/winutils");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String fileDir = null;

        // init data into javaRDD
        JavaRDD<Receipt> initData = setupReceiptsDataRdd(sc, DataSource.REWARDS_RECEIPTS_LAT_V2);

        // analyze 1:
        stateTotalCount(initData);

        // analyze 2:

        // analyze 3:
    }

}
