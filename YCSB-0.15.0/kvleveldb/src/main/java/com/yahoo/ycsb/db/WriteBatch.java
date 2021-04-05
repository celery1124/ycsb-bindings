package com.yahoo.ycsb.db;

import java.io.*;
//import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Concrete writebatch implementation.
 */
public class WriteBatch { 
   
   static {
      System.loadLibrary("kvleveldbjni"); // Load native library
   }
   private long cppPtr;
   // Declare an instance native methods
   protected native void init();
   protected native void destory();
   protected native void put(byte[] key, int keylen, byte[] value, int vallen, byte type);
   protected native void clear();

}
