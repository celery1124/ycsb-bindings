package com.yahoo.ycsb.db;

import java.io.*;
//import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Concrete ReadOptions implementation.
 */
public class ReadOptions { 
   
   static {
      System.loadLibrary("kvrangedbjni"); // Load native library
   }
   private long cppPtr;
   // Declare an instance native methods
   protected native void init(byte[] key, int keylen);
   protected native void setscan(int scanLen);
   protected native void setpack(int packHint);
   protected native void destory();
   protected native void hello();  
}
