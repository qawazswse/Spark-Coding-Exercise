import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import receipts.case_objects.Receipt;
import receipts.case_objects.ReceiptItem;
import receipts.conf.DataSource;

import static receipts.Util.Util.*;
import static receipts.csv_read.ReceiptItemRDD.setupReceiptsItemDataRdd;
import static receipts.csv_read.ReceiptRDD.setupReceiptsDataRdd;
import static receipts.service.ReceiptRddService.stateTotalCount;
import static receipts.service.ReceiptItemRddService.categoryDiscount;
import static receipts.service.ReceiptSQLService.userTotalPurchase;

public class Main {

    public static void main(String[] args) {

        // check parameters number
        if(args.length != 3) {
            System.out.println("parameters number invalid " +
                    "(there should be 3 parameters but " +
                    args.length
            );
            return;
        }

        // make sure the first parameter is valid
        if(args[0].length()>1
                || !Character.isDigit(args[0].charAt(0))
                ||  Integer.parseInt(args[0]) < 0
                ||  Integer.parseInt(args[0]) < 0) {
            System.out.println("The 1ST parameter is invalid");
            return;
        }

        // make sure the second parameter is valid
        for(int i=0; i<args[1].length(); i++) {
            if(!Character.isDigit(args[1].charAt(i))) {
                System.out.println("The 2ND parameter is invalid");
                return;
            }
        }

        // make sure the third parameter is valid
        if(!args[2].equals("csv") && !args[2].equals("json") && !args[2].equals("parquet")) {
            System.out.println("The 3RD parameter is invalid");
            return;
        }

        // parameters to int
        int analyze = Integer.parseInt(args[0]);
        int months = Integer.parseInt(args[1]);
        String fileType = args[2];

        // initialize
        System.setProperty("hadoop.home.dir", "c:/winutils");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // variables
        String fileName = "";
        Dataset<Row> result;
        JavaRDD<Receipt> ReceiptData = setupReceiptsDataRdd(sc, DataSource.REWARDS_RECEIPTS_LAT_V3);;
        JavaRDD<ReceiptItem> ReceiptItemData = setupReceiptsItemDataRdd(sc, DataSource.REWARDS_RECEIPTS_ITEM_LAT_V2);

        switch (analyze) {

            // analyze 0:   calculate total receipt price for each state in descending order
            case 0:
                result = stateTotalCount(ReceiptData, months);
                fileName = "state_total_in_last_" + months + "_months";
                chooseOutputFileFormat(result, fileName, fileType);
                break;

            // analyze 1:   calculate average discount percentage for each category in descending order
            case 1:
                result = categoryDiscount(ReceiptData, ReceiptItemData, months);
                fileName = "category_discount_in_last_" + months + "_months";
                chooseOutputFileFormat(result, fileName, fileType);
                break;

            // analyze 2:   calculate total receipt price for each user in descending order
            case 2:
                result = userTotalPurchase(months);
                fileName = "user_total_purchase_in_last_" + months + "_months";
                chooseOutputFileFormat(result, fileName, fileType);
                break;
        }

        System.out.println(
                fileName + "." + fileType + "\n"
                + "is now ready in the output folder."
        );

    }


    /*
        decide which function to use by args[2]
     */

    private static void chooseOutputFileFormat(Dataset<Row> input, String fileName, String fileType) {

        switch (fileType) {

            // output in csv format
            case "csv":
                datasetToCSV(input, fileName);
                break;

            // output in JSON format
            case "json":
                datasetToJSON(input, fileName);
                break;

            // output in parquet format
            case "parquet":
                datasetToParquet(input, fileName);
                break;
        }
    }

}
