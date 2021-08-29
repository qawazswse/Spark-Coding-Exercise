package receipts.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import receipts.Util.Util;
import receipts.case_objects.Receipt;
import scala.Tuple2;

/*
    deal with logic related to receipts data
 */
public class ReceiptRddService {



    /*
        calculate total receipt price for each state in descending order
        input: raw data JavaPairRDD<Receipt>
        output: counted JavaPairRDD<String, Double> in descending order
     */

    public static JavaPairRDD<String, Double> stateTotalCount(JavaRDD<Receipt> initData) {

        // get state and receipt total data from the raw data
        JavaPairRDD<String, Double> stateAndReceiptTotal = initData
                .mapToPair(receipt -> new Tuple2<>(
                        receipt.getStoreState(),
                        receipt.getReceiptTotal()
                        )
                );

        // remove rows that doesn't have 'STATE' or 'receipt total' record
        JavaPairRDD<String, Double> receiptsWithFullInformationRdd = stateAndReceiptTotal
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

        return sortedByValueRdd;
    }

}