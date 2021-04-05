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
 * Concrete KVleveldb client implementation.
 */
public class KVleveldbClient extends DB {

  private Logger logger = Logger.getLogger(getClass());

  /**
   * The KVleveldbClient implementation that will be used to communicate
   * with the kvleveldb server.
   */
  private static KVleveldb client;
  private static int references = 0;

  /**
   * @returns Underlying Memcached protocol client, implemented by
   *     SpyMemcached.
   */

  @Override
  public void init() throws DBException {
    synchronized(KVleveldbClient.class) {
      if(client == null) {
        client = new KVleveldb();
        System.out.printf("initial KVleveldb\n");

        try {
          client.init();
          System.out.printf("client init\n");
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
      references++;
    }
  }
  
  @Override
  public Status read(
      String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    try {
      byte[] value = client.get(key.getBytes(UTF_8), key.length());
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
    startkey = createQualifiedKey(table, startkey);
    try {
      ReadOptions rdopts = new ReadOptions();
      rdopts.init(null, 0);
      Iterator it = new Iterator();
      it.init(rdopts);
      int iterations = 0;
      for (it.seek(startkey.getBytes(UTF_8), startkey.length()); it.valid() && iterations < recordcount; it.next()) {
        HashMap<String, ByteIterator> value = new HashMap<>();
        deserializeValues(it.value(), fields, value);
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
    key = createQualifiedKey(table, key);
    try {
      byte[] value = serializeValues(values);
      client.update(key.getBytes(), key.length(), value, value.length);
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      byte[] value = serializeValues(values);
      client.insert(key.getBytes(), key.length(), value, value.length);
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    key = createQualifiedKey(table, key);
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
    synchronized(KVleveldbClient.class) {
      if (references == 1) {
        client.close();
        System.out.printf("KVleveldb cleanup\n");
      }
      references--;
    }
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
