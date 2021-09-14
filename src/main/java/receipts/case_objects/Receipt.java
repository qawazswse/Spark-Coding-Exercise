package receipts.case_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@AllArgsConstructor
public class Receipt implements Serializable {

    private String ReceiptId;
    private String StoreName;
    private String StoreAddress;
    private String StoreCity;
    private String StoreState;
    private String StoreZIP;
    private String StorePhone;
    private Integer StoreNumber;
    private String UserId;
    private Date ScanDate;
    private Date ReceiptPurchaseDate;
    private Double ReceiptTotal;
    private Integer ReceiptItemCount;
    private String ConsumerUserAgent;
    private Date ModifyDate;
    private Boolean DigitalReceipt;

}
