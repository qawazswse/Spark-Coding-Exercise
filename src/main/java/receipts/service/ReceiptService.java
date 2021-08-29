package receipts.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import receipts.Util.Util;
import receipts.case_objects.Receipt;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class ReceiptService {



    /*
        input: raw data JavaPairRDD<Receipt>
        output: counted JavaPairRDD<String, Double> in descending order
     */

    public static JavaPairRDD<String, Double> stateTotalCount(JavaRDD<Receipt> initData) {

        // get state and receipt total data from the raw data
        JavaPairRDD<String, Double> stateAndReceiptTotal = initData
                .mapToPair(receipt -> new Tuple2<>(receipt.getStoreState(), receipt.getReceiptTotal()));

        // remove rows that doesn't have 'STATE' record
        JavaPairRDD<String, Double> receiptsWithStatesRdd = stateAndReceiptTotal
                .filter(row -> row._1!=null && row._2!=null);

        // add receipt total with same state together
        JavaPairRDD<String, Double> stateTotal = receiptsWithStatesRdd
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


    /*
        input: JavaSparkContext, and file direction as a String
        output: raw data JavaRDD
     */

    public static JavaRDD<Receipt> setupReceiptsDataRdd(JavaSparkContext sc, String fileDir) {

        // using the modified file instead of the original one
        try {
            fileDir = Util.CommaToQuestionMarkInCSV(fileDir);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Cannot find file: " + fileDir);
        }

        SimpleDateFormat formatter = new SimpleDateFormat("dd-M-yyyy hh:mm:ss.sss");

        return sc.textFile(fileDir)
                .mapPartitionsWithIndex((index, iter) -> {  // remove the header row
                    if (index == 0 && iter.hasNext()) {
                        iter.next();
                    }
                    return iter;
                }, true)
                .map(row -> {     // make the value(state and receipt total) into Receipt Objects
                    String[] cells = row.split(",");
                    return new Receipt(
                            cells[0].length()>0? cells[0] : null,
                            cells[1].length()>0? cells[1] : null,
                            cells[2].length()>0? cells[2] : null,
                            cells[3].length()>0? cells[3] : null,
                            cells[4].length()>0? cells[4] : null,
                            cells[5].length()>0? cells[5] : null,
                            cells[6].length()>0? cells[6] : null,
                            cells[7].length()>0? Integer.parseInt(cells[7]) : null,
                            cells[8].length()>0? cells[8] : null,
                            cells[9].length()>0? formatter.parse(cells[9]) : null,
                            cells[10].length()>0? formatter.parse(cells[10]) : null,
                            cells[11].length()>0? Double.parseDouble(cells[11]) : null,
                            cells[12].length()>0? Integer.parseInt(cells[12]) : null,
                            cells[13].length()>0? cells[13] : null,
                            cells[14].length()>0? formatter.parse(cells[14]) : null,
                            cells[15].length()>0? Boolean.parseBoolean(cells[15]) : null
                    );
                });
    }

}
