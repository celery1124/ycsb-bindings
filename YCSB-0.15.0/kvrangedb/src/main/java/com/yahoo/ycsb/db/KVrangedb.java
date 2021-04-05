package com.yahoo.ycsb.db;

import java.io.*;
//import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Concrete kvrangedb bindings implementation.
 */
public class KVrangedb {  
   static {
      System.loadLibrary("kvrangedbjni"); // Load native library
   }
 
   // Declare an instance native method sayHello() which receives no parameter and returns void
   protected native boolean init();
   protected native boolean close();
   protected native int getBatchSize();
   protected native byte[] get(ReadOptionsV2 rdopts, byte[] key, int keylen);
   protected native boolean insert(byte[] key, int keylen, byte[] value, int vallen);
   protected native boolean update(byte[] key, int keylen, byte[] value, int vallen);
   protected native boolean delete(byte[] key, int keylen);
   
   protected native boolean writeBatch(WriteBatch writebatchObject);
   protected native void hello();
   }
