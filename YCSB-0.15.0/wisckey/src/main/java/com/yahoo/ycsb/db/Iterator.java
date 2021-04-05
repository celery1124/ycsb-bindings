package com.yahoo.ycsb.db;

import java.io.*;
//import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Concrete iterator implementation.
 */
public class Iterator { 
   
   static {
      System.loadLibrary("wisckeyjni"); // Load native library
   }
   private long cppPtr;
   // Declare an instance native methods
   protected native void init(ReadOptions rdopts);
   protected native void destory();
   protected native void seek(byte[] key, int keylen);
   protected native void next();
   protected native boolean valid();
   protected native byte[] key();
   protected native byte[] value();
}
