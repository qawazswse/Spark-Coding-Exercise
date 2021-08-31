package receipts.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import receipts.Util.Util;
import receipts.case_objects.Receipt;
import receipts.result_objects.StateTotalRecord;
import scala.Tuple2;

import java.util.List;

/*
    deal with logic related to receipts data
 */
public class ReceiptRddService {


    /*
        calculate total receipt price for each state in descending order
        input: raw data JavaPairRDD<Receipt>, and how many month before
        output: counted JavaPairRDD<String, Double> in descending order
     */

    public static Dataset<Row> stateTotalCount(JavaRDD<Receipt> initData, Integer m) {

        // only choose the information in 'm' months
        JavaRDD<Receipt> InMonthsRdd = initData
                .filter(receipt -> receipt.getScanDate().after(Util.getDateByMonthBefore(m)));

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
        JavaPairRDD<String, String> numberInTwoDigitsRdd = sortedByValueRdd
                .mapValues(Util::toTwoDigitsDouble);

        // print the result out
        numberInTwoDigitsRdd.collect().forEach(System.out::println);

        JavaRDD<StateTotalRecord> rdd = sortedByValueRdd.map(row -> new StateTotalRecord(row._1, row._2));

        SparkSession spark = SparkSession.builder().appName("stateTotalCount").master("local[*]")
                .config("spark.sql.wareHouse.dir", "file:///c:tmp/")
                .getOrCreate();

        return spark.createDataFrame(rdd, StateTotalRecord.class);
    }

}
