package com.crc32;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class CRC32Test {

    public static void main(String[] args) throws IOException {
        HQCRC32 crc32 = HQCRC32.getInstance();
        byte[] data = "password".getBytes();
        long result = crc32.encrypt(data);
        System.err.println(result);
//        System.out.println(getCRC32("D:\\admin\\Desktop\\log1"));
//        System.out.println( getCRC32("D:\\admin\\Desktop\\log2.csv"));
        System.out.println(getCRC("password"));

    }
    public static int getCRC(String data212) {
        int CRC = 0xFFFF;
        int num = 0xA001;
        int inum = 0;
        byte[] sb = data212.getBytes();
        for(int j = 0; j < sb.length; j ++) {
            inum = sb[j];
            CRC = (CRC >> 8) & 0x00FF;
            CRC ^= inum;
            for(int k = 0; k < 8; k++) {
                int flag = CRC % 2;
                CRC = CRC >> 1;

                if(flag == 1) {
                    CRC = CRC ^ num;
                }
            }
        }
        return CRC;
    }

    /**
     * 使用CheckedInputStream计算CRC
     */
    public static Long getCRC32(String filepath) throws IOException {
        CRC32 crc32 = new CRC32();
        FileInputStream fileinputstream = new FileInputStream(new File(filepath));
        CheckedInputStream checkedinputstream = new CheckedInputStream(fileinputstream, crc32);
        while (checkedinputstream.read() != -1) {
        }
        checkedinputstream.close();
        return crc32.getValue();
    }
}
