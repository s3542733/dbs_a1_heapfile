package dbs_a1_heapfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

public class DBLoad {
	
	public static void main(String[] args) {
		
		int heapPageSize;
		int recordsPerPage;
		int recordID = 0;
		int sizeOfRecord = 4 + 341;
		String dataFileName;
		String heapFileName;
		
		String lineRead;
		BufferedReader dataFile;
		FileOutputStream heapFile;
		
		StringTokenizer	strTok;
		
		if(true) {
			
			heapPageSize = Integer.parseInt(args[1]);	
			dataFileName = args[2];
			heapFileName = "heap." + args[1];
			recordsPerPage = heapPageSize/sizeOfRecord;
			
			if(recordsPerPage >= 1) {
				try {
					dataFile = new BufferedReader(new FileReader(dataFileName));
					heapFile = new FileOutputStream(new File(heapFileName));
					lineRead = dataFile.readLine();
					while(lineRead != null) {
						String[] tokArr = lineRead.split("\t", -1);
						byte[] record = new byte[sizeOfRecord];
						byte[] rid = ByteBuffer.allocate(4).putInt(recordID).array();
						System.arraycopy(rid, 0, record, 0, rid.length);
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
						heapFile.write(record);
						
						lineRead = dataFile.readLine();
						recordID++;
						
					}
					
					heapFile.close();
					
				} catch(FileNotFoundException e) {
					e.printStackTrace();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
			try {
				FileInputStream readIn = new FileInputStream(new File(heapFileName));
				while (readIn.read() != -1) {
					byte[] line = new byte[345];
					readIn.read(line, 0, 345);
					byte[] ridBytes = new byte[4];
					ByteBuffer ridWrap = ByteBuffer.wrap(ridBytes);
					System.arraycopy(line, 0, ridBytes, 0, 4);
					int rid = ridWrap.getInt();
					System.out.write(line);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
