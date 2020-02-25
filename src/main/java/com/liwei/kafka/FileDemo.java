package com.liwei.kafka;

import java.io.*;

public class FileDemo {
    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader("D:\\result.txt"));
        String data ="";
        while (bf.readLine()!=null){
            data +=bf.readLine()+"\n";
        }
        System.out.println(data);
    }
}
