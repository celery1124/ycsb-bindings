/**
 * Copyright (c) 2014-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
//import java.util.ArrayList;
import java.util.HashMap;
//import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

//import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Concrete KVrangedb client implementation.
 */
public class KVrangedbClient extends DB {
  static final String PROPERTY_BATCH_SIZE = "batch.size";
  static final String PROPERTY_HINT_SCAN_LEN = "hint.scanLen";
  static final String PROPERTY_HINT_PACK = "hint.pack";
  static final String PROPERTY_SCAN_VALUE = "scan.value";
  static final String PROPERTY_SCAN_DIST = "scan.dist";
  private Logger logger = Logger.getLogger(getClass());

  /**
   * The KVrangedbClient implementation that will be used to communicate
   * with the kvrangedb server.
   */
  private static KVrangedb client;
  private static int references = 0;
  private static int batchSize = 0;
  private static boolean hintScanLen = false;
  private static boolean scanValue = true;
  private static int scanDist = -1;
  private static int hintPack = 0;
  private int batchCount = 0;
  private WriteBatch batch;
  /**
   * @returns Underlying Memcached protocol client, implemented by
   *     SpyMemcached.
   */

  @Override
  public void init() throws DBException {
    synchronized(KVrangedbClient.class) {
      if(client == null) {
        batchSize = Integer.parseInt(getProperties().getProperty(PROPERTY_BATCH_SIZE, "-1"));
        hintScanLen = Boolean.parseBoolean(getProperties().getProperty(PROPERTY_HINT_SCAN_LEN, "false"));
        scanValue = Boolean.parseBoolean(getProperties().getProperty(PROPERTY_SCAN_VALUE, "true"));
        scanDist = Integer.parseInt(getProperties().getProperty(PROPERTY_SCAN_DIST, "-1"));
        hintPack = Integer.parseInt(getProperties().getProperty(PROPERTY_HINT_PACK, "0"));
        client = new KVrangedb();
        System.out.printf("initial KVrangedb\n");
        try {
          client.init();
          System.out.printf("client init\n");
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
      references++;
    }
    batch = new WriteBatch();
    batch.init();
  }
  
  @Override
  public Status read(
      String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    //key = createQualifiedKey(table, key);
    try {
      ReadOptionsV2 rdopts = new ReadOptionsV2();
      rdopts.init(null, 0);
      rdopts.setpack(hintPack);
      
      byte[] value = client.get(rdopts, key.getBytes(UTF_8), key.length());
      if (value == null) return Status.NOT_FOUND;
      deserializeValues(value, fields, result);
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    //startkey = createQualifiedKey(table, startkey);
    try {
      ReadOptionsV2 rdopts = new ReadOptionsV2();
      if (scanDist >= 0) {  
        byte[] bytes = startkey.getBytes();
        int lsbint = bytes[12] << 24 | (bytes[13] & 0xFF) << 16 | (bytes[14] & 0xFF) << 8 | (bytes[15] & 0xFF);
        lsbint = lsbint + scanDist;
        bytes[12] = (byte)((lsbint >> 24) & 0xFF);
        bytes[13] = (byte)((lsbint >> 16) & 0xFF);
        bytes[14] = (byte)((lsbint >> 8) & 0xFF);
        bytes[15] = (byte)((lsbint >> 0) & 0xFF);
        rdopts.init(bytes, 16); // 16 bytes keys
      } else {
        rdopts.init(null, 0);
      }
      if (hintScanLen) {
        rdopts.setscan(recordcount);
      }
      IteratorV2 it = new IteratorV2();
      it.init(rdopts);
      int iterations = 0;
      for (it.seek(startkey.getBytes(UTF_8), startkey.length()); it.valid() && iterations < recordcount; it.next()) {
        HashMap<String, ByteIterator> value = new HashMap<>();
        if (scanValue) {
          deserializeValues(it.value(), fields, value);
        }
        result.add(value);
        iterations++;
      }
      it.destory();
      rdopts.destory();
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error encountered for key: " + startkey, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(
      String table, String key, Map<String, ByteIterator> values) {
    //key = createQualifiedKey(table, key);
    try {
      byte[] value = serializeValues(values);
      if (batchSize <= 0) {
        client.update(key.getBytes(), key.length(), value, value.length);
      } else {
        if (batchCount++ < batchSize) {
          batch.put(key.getBytes(), key.length(), value, value.length, (byte)0);
        } else {
          client.writeBatch(batch);
          batch.clear();
          batchCount = 0;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      String table, String key, Map<String, ByteIterator> values) {
    //key = createQualifiedKey(table, key);
    try {
      byte[] value = serializeValues(values);
      if (batchSize <= 0) {
        client.insert(key.getBytes(), key.length(), value, value.length);
      } else {
        if (batchCount++ < batchSize) {
          batch.put(key.getBytes(), key.length(), value, value.length, (byte)0);
        } else {
          client.writeBatch(batch);
          batch.clear();
          batchCount = 0;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    //key = createQualifiedKey(table, key);
    try {
      client.delete(key.getBytes(UTF_8), key.length());
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error deleting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    synchronized(KVrangedbClient.class) {
      if (references == 1) {
        client.close();
        System.out.printf("KVrangedb cleanup\n");
      }
      references--;
    }
    batch.destory();
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected Map<String, ByteIterator> deserializeValues(byte[] values, Set<String> fields, 
  Map<String, ByteIterator> result) {
    ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  protected byte[] serializeValues(Map<String, ByteIterator> values) throws IOException {
    try(ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      ByteBuffer buf = ByteBuffer.allocate(4);

      for(Map.Entry<String, ByteIterator> value : values.entrySet()) {
        byte[] keyBytes = value.getKey().getBytes(UTF_8);
        byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }
}
