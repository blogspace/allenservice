package com.crc32;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class CRC32Test {

    public static void main(String[] args) throws IOException {
//        HQCRC32 crc32 = HQCRC32.getInstance();
//        byte[] data = "12346".getBytes();
//        long result = crc32.encrypt(data);
//        System.err.println(result);
        System.out.println(getCRC32("D:\\admin\\Desktop\\log1"));
        System.out.println( getCRC32("D:\\admin\\Desktop\\log2.csv"));
       ;

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
