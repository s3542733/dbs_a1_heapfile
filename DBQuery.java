import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DBQuery {

	public static void main(String[] args) {
		
		int sizeOfRecord = 4 + 341; // Size of a record
		
		if(args[0] != "" && args[1].matches("[0-9]+")) {
			int sizeOfPage = Integer.parseInt(args[1]); // Size of a page from args
			int recordsPerPage = sizeOfPage/sizeOfRecord; // Number of records that can fit on a page
			String searchTerm = args[0].toLowerCase();  // Turns the search term into lowercase
			String heapFileName = "heap." + sizeOfPage; // Creates heapfile name from page size
			long startTime = System.nanoTime(); // Starting time
			try {
				// File reader for heapfile
				FileInputStream readIn = new FileInputStream(new File(heapFileName));
				while(readIn.available() != 0) {
					byte[] page = new byte[sizeOfPage];
					readIn.read(page, 0, sizeOfPage); // Reads in a single page
					int recordOffset = 0; // Offset for reach record in the page
					byte[] pageNumArr = new byte[4];
					System.arraycopy(page, page.length-4, pageNumArr, 0, 4);
					int heapPageNum = ByteBuffer.wrap(pageNumArr).getInt();
					
					// Iterates through every record on a page
					for(int i=0; i < recordsPerPage; i++) {
						byte[] line = new byte[345];
						System.arraycopy(page, recordOffset, line, 0, sizeOfRecord);
						byte[] BN_NAME = new byte[256];
						System.arraycopy(line, 18, BN_NAME, 0, 256);
						String bnName = new String(BN_NAME);
						// Checks if BN_NAME matches the search term
						if(bnName.toLowerCase().contains(searchTerm)) {
							// Following section gets each field from the record and turns
							// them into strings for printing. Only bnNames is trimmed.
							byte[] ridBytes = new byte[4];
							ByteBuffer ridWrap = ByteBuffer.wrap(ridBytes);
							System.arraycopy(line, 0, ridBytes, 0, 4);
							int rid = ridWrap.getInt();
							byte[] REGISTER_NAME = new byte[14];
							System.arraycopy(line, 4, REGISTER_NAME, 0, 14);
							String regName = new String(REGISTER_NAME);
							byte[] BN_STATUS = new byte[12];
							System.arraycopy(line, 274, BN_STATUS, 0, 12);
							String bnStatus = new String(BN_STATUS);
							byte[] BN_REG_DT = new byte[10];
							System.arraycopy(line, 286, BN_REG_DT, 0, 10);
							String bnRegDt = new String(BN_REG_DT);
							byte[] BN_CANCEL_DT = new byte[10];
							System.arraycopy(line, 296, BN_CANCEL_DT, 0, 10);
							String bnCancelDt = new String(BN_CANCEL_DT);
							byte[] BN_RENEW_DT = new byte[10];
							System.arraycopy(line, 306, BN_RENEW_DT, 0, 10);
							String bnRenewDt = new String(BN_RENEW_DT);
							byte[] BN_STATE_NUM = new byte[12];
							System.arraycopy(line, 316, BN_STATE_NUM, 0, 12);
							String bnStateNum = new String(BN_STATE_NUM);
							byte[] BN_STATE_OF_REG = new byte[3];
							System.arraycopy(line, 328, BN_STATE_OF_REG, 0, 3);
							String bnStateOfReg = new String(BN_STATE_OF_REG);
							byte[] BN_ABN = new byte[14];
							System.arraycopy(line, 331, BN_ABN, 0, 14);
							String bnAbn = new String(BN_ABN);
							System.out.println(heapPageNum + ":" + rid + " " + regName + " " + bnName.trim() + " " + bnStatus + " "
							+ bnRegDt + " " + bnCancelDt + " " + bnRenewDt + " " + bnStateNum + " " + bnStateOfReg + " " + bnAbn);
						}
						recordOffset += sizeOfRecord; // Increments the offset by one record
					}
				}
				readIn.close();
				long endTime = System.nanoTime(); // End time of execution
				System.out.println("Executed query in: " + (endTime-startTime)/1000000);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
