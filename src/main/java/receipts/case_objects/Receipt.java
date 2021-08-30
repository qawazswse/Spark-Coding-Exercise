package receipts.case_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@AllArgsConstructor
public class Receipt implements Serializable {

    String ReceiptId;
    String StoreName;
    String StoreAddress;
    String StoreCity;
    String StoreState;
    String StoreZIP;
    String StorePhone;
    Integer StoreNumber;
    String UserId;
    Date ScanDate;
    Date ReceiptPurchaseDate;
    Double ReceiptTotal;
    Integer ReceiptItemCount;
    String ConsumerUserAgent;
    Date ModifyDate;
    Boolean DigitalReceipt;

}
