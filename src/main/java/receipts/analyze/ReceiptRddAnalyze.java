package receipts.analyze;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import receipts.Util.Util;
import receipts.case_objects.Receipt;
import receipts.conf.DataSourceConf;
import receipts.result_objects.StateTotalRecord;
import scala.Tuple2;

import java.io.IOException;

import static receipts.conf.DataSourceConf.OUTPUT_DIR;
import static receipts.csv_read.ReceiptRDD.setupReceiptsDataRdd;

/**
    deal with logic related to receipts data
 */
public class ReceiptRddAnalyze {


    public static void main(String[] args) {

        int months = Integer.parseInt(args[0]);
        String fileType = args[1];

        // initialize
        System.setProperty("hadoop.home.dir", "c:/winutils");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // variables
        String fileName = "state_total_in_last_" + months + "_months";
        JavaRDD<Receipt> receiptData = setupReceiptsDataRdd(sc, DataSourceConf.REWARDS_RECEIPTS_LAT_V3);

        // only choose the information in 'm' months
        JavaRDD<Receipt> InMonthsRdd = receiptData
                .filter(receipt -> receipt.getScanDate().after(Util.getDateByMonthBefore(months)));

        InMonthsRdd.collect().forEach(System.out::println);

        // get state and receipt total data from the raw data
        JavaPairRDD<String, Double> stateAndReceiptTotalRdd = InMonthsRdd
                .mapToPair(receipt -> new Tuple2<>(
                                receipt.getStoreState(),
                                receipt.getReceiptTotal()
                        )
                );

        // remove rows that doesn't have 'STATE' or 'receipt total' record
        JavaPairRDD<String, Double> receiptsWithFullInformationRdd = stateAndReceiptTotalRdd
                .filter(row -> row._1!=null && row._2!=null);

        // add receipt total with same state together
        JavaPairRDD<String, Double> stateTotal = receiptsWithFullInformationRdd
                .reduceByKey(Double::sum);

        // sort by value(receipt total)
        JavaPairRDD<String, Double> sortedByValueRdd = stateTotal
                .mapToPair(row -> new Tuple2<>(row._2, row._1))
                .sortByKey(false)
                .mapToPair(row -> new Tuple2<>(row._2, row._1));

        // justify the receipt total value to two-digits numbers
//        JavaPairRDD<String, String> numberInTwoDigitsRdd = sortedByValueRdd
//                .mapValues(Util::toTwoDigitsDouble);

        // print the result out
//        numberInTwoDigitsRdd.collect().forEach(System.out::println);

        JavaRDD<StateTotalRecord> rdd = sortedByValueRdd.map(row -> new StateTotalRecord(row._1, row._2));

        JavaRDD<String> toFile = rdd.map(row -> row.getString(fileType));

        toFile.saveAsTextFile(OUTPUT_DIR + fileName);

        try {
            Util.toOneFile(toFile, fileName, fileType);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(
                fileName + "." + fileType + "\n"
                        + "is now ready in the output folder."
        );

    }

}
