import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DBLoad {
	
	public static void main(String[] args) {
		
		int heapPageSize; // Size of a single page in bytes
		int recordsPerPage; // number of records in that can fit in a page
		int recordID = 0; // ID for records
		int pageNum = 0; // page number for marking end of page
		int sizeOfRecord = 4 + 341; // 4 bytes + size of record
		String dataFileName;
		String heapFileName;
		
		String lineRead;
		BufferedReader dataFile;
		FileOutputStream heapFile;
		
		if(args[0].equals("-p") && args[1].matches("[0-9]+")) {
			heapPageSize = Integer.parseInt(args[1]); // Size of heap from args 
			dataFileName = args[2]; // Name of file from args
			heapFileName = "heap." + args[1]; // Name of file is heap + sizeOfPage
			recordsPerPage = (heapPageSize-4)/sizeOfRecord; // Accounts for 4 bytes for page number
			
			if(recordsPerPage >= 1) {
				try {
					dataFile = new BufferedReader(new FileReader(dataFileName)); // Reader for file to be loaded
					heapFile = new FileOutputStream(new File(heapFileName)); // Writer for heapfile
					lineRead = dataFile.readLine(); // Starting time for timer
					long start = System.nanoTime();
					while(lineRead != null) {
						int recordCounter = 0; // Counter for record on a page
						int currOffset = 0; // Offset for record placement on a page
						byte[] heapPage = new byte[heapPageSize]; // Empty page for heapfile
						ByteBuffer pageNumBytes = ByteBuffer.allocate(4).putInt(pageNum); // Turning pageNum into a byte array
						// Appending the pageNum to the bottom of the page (last 4 bytes)
						System.arraycopy(pageNumBytes.array(), 0, heapPage, heapPage.length-4, pageNumBytes.array().length);
						while(recordCounter != recordsPerPage) {
							if(lineRead == null ) {
								// Breaks the loop if the current line is null
								// This prevents writing null to the same page infinitely
								break;
							}
							String[] tokArr = lineRead.split("\t", -1); // Splits line on tabs
							byte[] record = new byte[sizeOfRecord]; // Creates an empty record of fixed size
							byte[] rid = ByteBuffer.allocate(4).putInt(recordID).array(); 
							System.arraycopy(rid, 0, record, 0, rid.length); // Puts rid as bytes into the record
							// The following section iterates through each field and converts the string
							// token into a byte[] which can be copied into the record using System.arraycopy
							byte[] registerName = tokArr[0].getBytes("utf-8");
							byte[] REGISTER_NAME = new byte[14];
							if (tokArr[0] != "") {
								System.arraycopy(registerName, 0, REGISTER_NAME, 0, registerName.length);
							}
							System.arraycopy(REGISTER_NAME, 0, record, 4, REGISTER_NAME.length);
							byte[] bnName = tokArr[1].getBytes();
							byte[] BN_NAME = new byte[256];
							if (tokArr[1] != "") {
								System.arraycopy(bnName, 0, BN_NAME, 0, bnName.length);
							}
							System.arraycopy(BN_NAME, 0, record, 18, BN_NAME.length);
							byte[] bnStatus = tokArr[2].getBytes();
							byte[] BN_STATUS = new byte[12];
							if (tokArr[2] != "") {
								System.arraycopy(bnStatus, 0, BN_STATUS, 0, bnStatus.length);
							}
							System.arraycopy(BN_STATUS, 0, record, 274, BN_STATUS.length);
							byte[] bnRegDt = tokArr[3].getBytes();
							byte[] BN_REG_DT = new byte[10];
							if (tokArr[3] != "") {
								System.arraycopy(bnRegDt, 0, BN_REG_DT, 0, bnRegDt.length);
							}
							System.arraycopy(BN_REG_DT, 0, record, 286, BN_REG_DT.length);
							byte[] bnCancelDt = tokArr[4].getBytes();
							byte[] BN_CANCEL_DT = new byte[10];
							if (tokArr[4] != "") {
								System.arraycopy(bnCancelDt, 0, BN_CANCEL_DT, 0, bnCancelDt.length);
							}
							System.arraycopy(BN_CANCEL_DT, 0, record, 296, BN_CANCEL_DT.length);
							byte[] bnRenewDt = tokArr[5].getBytes();
							byte[] BN_RENEW_DT = new byte[10];
							if (tokArr[5] != "") {
								System.arraycopy(bnRenewDt, 0, BN_RENEW_DT, 0, bnRenewDt.length);
							}
							System.arraycopy(BN_RENEW_DT, 0, record, 306, BN_RENEW_DT.length);
							byte[] bnStateNum = tokArr[6].getBytes();
							byte[] BN_STATE_NUM = new byte[12];
							if (tokArr[6] != "") {
								System.arraycopy(bnStateNum, 0, BN_STATE_NUM, 0, bnStateNum.length);
							}
							System.arraycopy(BN_STATE_NUM, 0, record, 316, BN_STATE_NUM.length);
							byte[] bnStateOfReg = tokArr[7].getBytes();
							byte[] BN_STATE_OF_REG = new byte[3];
							if (tokArr[7] != "") {
								System.arraycopy(bnStateOfReg, 0, BN_STATE_OF_REG, 0, bnStateOfReg.length);
							}
							System.arraycopy(BN_STATE_OF_REG, 0, record, 328, BN_STATE_OF_REG.length);
							byte[] bnAbn = tokArr[8].getBytes();
							byte[] BN_ABN = new byte[14];
							if (tokArr[8] != "") {
								System.arraycopy(bnAbn, 0, BN_ABN, 0, bnAbn.length);
							}
							System.arraycopy(BN_ABN, 0, record, 331, BN_ABN.length);
							System.arraycopy(record, 0, heapPage, currOffset, record.length);
							currOffset += sizeOfRecord; // Increments record by sizeOfRecord
							recordCounter++; // Increments recordCounter by 1
							recordID++; // Increments record ID by 1
							lineRead = dataFile.readLine(); // Reads next line for loop
						}
						heapFile.write(heapPage); // When page is full, writes to the heapfile
						pageNum++; // Increment page number
					}
					long finish = System.nanoTime(); // Finish time for execution
					System.out.println("Executed in: " + (finish-start)/1000000 + "ms"); // Time in milliseconds
					System.out.println(recordID + " records loaded."); // Number of records loaded
					System.out.println(pageNum + " pages created."); // Number of pages created
					heapFile.close();
				} catch(FileNotFoundException e) {
					System.out.println(dataFileName + " could not be found.");
				} catch(IOException e) {
					e.printStackTrace();
				}
			} else {
				System.out.println("Page size is too small. Please use allow at least 350 bytes per a page.");
			}
		} else {
			System.out.println("One of the commandline parameters you've entered are invalid.");
		}
	}
}
