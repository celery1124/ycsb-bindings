package com.yahoo.ycsb.db;

import java.io.*;
//import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Concrete ReadOptions implementation.
 */
public class ReadOptions { 
   
   static {
      System.loadLibrary("kvleveldbjni"); // Load native library
   }
   private long cppPtr;
   // Declare an instance native methods
   protected native void init(byte[] key, int keylen);
   protected native void destory();
}
