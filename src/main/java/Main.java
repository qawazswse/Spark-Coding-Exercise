import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import receipts.case_objects.Receipt;
import receipts.case_objects.ReceiptItem;
import receipts.conf.DataSource;

import static receipts.csv_to_rdd.ReceiptItemRDD.setupReceiptsItemDataRdd;
import static receipts.csv_to_rdd.ReceiptRDD.setupReceiptsDataRdd;
import static receipts.service.ReceiptRddService.stateTotalCount;
import static receipts.service.ReceiptItemRddService.categoryDiscount;

public class Main {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "c:/winutils");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // analyze 1:   calculate total receipt price for each state in descending order [in last * month]
        // output in JSON format
        JavaRDD<Receipt> ReceiptData = setupReceiptsDataRdd(sc, DataSource.REWARDS_RECEIPTS_LAT_V3);
        stateTotalCount(ReceiptData);

        // analyze 2:   calculate average discount percentage for each category in descending order [in last * month]
        // output in csv format
        JavaRDD<ReceiptItem> ReceiptItemData = setupReceiptsItemDataRdd(sc, DataSource.REWARDS_RECEIPTS_ITEM_LAT_V2);
        categoryDiscount(ReceiptItemData);

        // analyze 3:
    }

}
