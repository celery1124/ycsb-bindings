#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <cstddef>
#include <fstream>
#include <iostream>
#include "com_yahoo_ycsb_db_KVleveldb.h"
#include "com_yahoo_ycsb_db_WriteBatch.h"
#include "com_yahoo_ycsb_db_Iterator.h"
#include "com_yahoo_ycsb_db_ReadOptions.h"

#include "json11.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "leveldb/options.h"
#include "leveldb/db.h"

// global variables
leveldb::DB *db;
leveldb::Options options;
leveldb::Env* g_env = NULL;
int batch_size;

using namespace leveldb;

jboolean Java_com_yahoo_ycsb_db_KVleveldb_init(JNIEnv* env, jobject thisobj/*jdb*/) { 
    std::ifstream ifs("kvleveldb_config.json");
    std::string file_content( (std::istreambuf_iterator<char>(ifs) ),
                       (std::istreambuf_iterator<char>()    ) );
    std::string err;
    if (file_content.size() == 0) { // default jason
        file_content = R"({"dev_name":"/dev/kvemul",
                        "index_type":0,
        				"batch_Size":6})";
        printf("Using default leveldb config file\n");
    }

    // parse json
    const auto config = json11::Json::parse(file_content, err);
		std::string dev_name = config["dev_name"].string_value();
    int index_type = config["index_type"].int_value();
	batch_size = config["batch_size"].int_value();
    
    KVS_CONT *kvs_conts = (KVS_CONT *)malloc(sizeof(KVS_CONT) * 1);
    for (int i = 0; i < 1; i++) {
        (void) new (&kvs_conts[i]) KVS_CONT((char *)dev_name.c_str(), 64, 107374182400);
    }
    fprintf(stderr, "%s open\n", dev_name.c_str());
    g_env = leveldb::NewKVEnv(leveldb::Env::Default(), kvs_conts);
    db = NULL;
    options.env = g_env;
    options.create_if_missing = true;
    options.write_buffer_size = 1800 << 10;
    options.max_file_size = 1800 << 10;
    options.max_open_files = 1000;
    options.reuse_logs = true;
    leveldb::Status s = DB::Open(options, "test", &db);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
    return s.ok();
}

jboolean Java_com_yahoo_ycsb_db_KVleveldb_close(JNIEnv* env, jobject /*jdb*/) { 
    delete db;
}

jint Java_com_yahoo_ycsb_db_KVleveldb_getBatchSize (JNIEnv *env, jobject) {
    return batch_size;
}

static jbyteArray copyBytes(JNIEnv* env, char *value, unsigned int val_len) {
    jbyteArray jbytes = env->NewByteArray(val_len);
    if(jbytes == nullptr) {
        // exception thrown: OutOfMemoryError	
        return nullptr;
    }
    
    env->SetByteArrayRegion(jbytes, 0, val_len,
    const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value)));
    return jbytes;
}

jbyteArray Java_com_yahoo_ycsb_db_KVleveldb_get(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len) {
	leveldb::ReadOptions rdopts;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
  
    leveldb::Slice kv_key((const char*)key, jkey_len);
    std::string kv_val;
	db->Get(rdopts, kv_key, &kv_val);

    jbyteArray jret_value = copyBytes(env, (char*)kv_val.data(), kv_val.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    // cleanup
    delete[] key;

    return jret_value;
}

jboolean Java_com_yahoo_ycsb_db_KVleveldb_insert(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
	leveldb::WriteOptions wropts;
	leveldb::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    leveldb::Slice kv_key((const char*)key, jkey_len);
    leveldb::Slice kv_val((const char*)value, jval_len);

    ret = db->Put(wropts, kv_key, kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret.ok();
}

jboolean Java_com_yahoo_ycsb_db_KVleveldb_update(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
    leveldb::WriteOptions wropts;
	leveldb::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    leveldb::Slice kv_key((const char*)key, jkey_len);
    leveldb::Slice kv_val((const char*)value, jval_len);

    ret = db->Put(wropts, kv_key, kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret.ok();
}

jboolean Java_com_yahoo_ycsb_db_KVleveldb_delete(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len) {
	leveldb::WriteOptions wropts;
	leveldb::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
    
    leveldb::Slice kv_key((const char*)key, jkey_len);

    ret = db->Delete(wropts, kv_key);

    // cleanup
    delete[] key;

    return ret.ok();
}


jboolean Java_com_yahoo_ycsb_db_KVleveldb_writeBatch(JNIEnv* env, jobject thisObj /*jdb*/,
                            jobject writebatchObject) {
    jclass writebatchClass = env->GetObjectClass(writebatchObject);
    jfieldID fidcppPtr = env->GetFieldID(writebatchClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(writebatchObject, fidcppPtr);
    WriteBatch *batch =  *(WriteBatch**)&cpp_ptr;
    
	leveldb::Status ret;
    
    leveldb::WriteOptions wropts;
    ret = db->Write(wropts, batch);

    return ret.ok();
}


static WriteBatch *_WB_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(WriteBatch**)&cpp_ptr;
}
static void _WB_set_java_ptr(JNIEnv *env, jobject thisObj, WriteBatch *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

void Java_com_yahoo_ycsb_db_WriteBatch_init(JNIEnv *env, jobject thisobj) {
    WriteBatch *self = new WriteBatch();
    _WB_set_java_ptr(env, thisobj, self);
}

void Java_com_yahoo_ycsb_db_WriteBatch_destory(JNIEnv *env, jobject thisobj) {
    WriteBatch *self = _WB_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        delete self;
        _WB_set_java_ptr(env, thisobj, NULL);
    }
}

void Java_com_yahoo_ycsb_db_WriteBatch_put(JNIEnv* env, jobject thisobj /*jdb*/,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len, jbyte jtype) {
    
}

void Java_com_yahoo_ycsb_db_WriteBatch_clear(JNIEnv *env, jobject thisobj) {
    
}


static Iterator *_IT_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(Iterator**)&cpp_ptr;
}
static void _IT_set_java_ptr(JNIEnv *env, jobject thisObj, Iterator *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

void Java_com_yahoo_ycsb_db_Iterator_init(JNIEnv *env, jobject thisobj,
                                        jobject readoptionsObject) {
    jclass readoptionsClass = env->GetObjectClass(readoptionsObject);
    jfieldID fidcppPtr = env->GetFieldID(readoptionsClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(readoptionsObject, fidcppPtr);
    const leveldb::ReadOptions *rdopts =  *(leveldb::ReadOptions**)&cpp_ptr;
    Iterator *self = db->NewIterator(*rdopts);
    _IT_set_java_ptr(env, thisobj, self);
}

void Java_com_yahoo_ycsb_db_Iterator_destory(JNIEnv *env, jobject thisobj) {
    Iterator *self = _IT_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        delete self;
        _IT_set_java_ptr(env, thisobj, NULL);
    }
}

void Java_com_yahoo_ycsb_db_Iterator_seek(JNIEnv *env, jobject thisobj,
                                        jbyteArray jkey, jint jkey_len) {
    Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
    leveldb::Slice kv_key((const char*)key, jkey_len);

    it->Seek(kv_key);
    // cleanup
    delete[] key;
}

void Java_com_yahoo_ycsb_db_Iterator_next(JNIEnv *env, jobject thisobj) {
    Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    it->Next();
}

jboolean Java_com_yahoo_ycsb_db_Iterator_valid(JNIEnv *env, jobject thisobj) {
    Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    return it->Valid();
}

jbyteArray Java_com_yahoo_ycsb_db_Iterator_key(JNIEnv* env, jobject thisobj) {
	Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    leveldb::Slice kv_key = it->key();
    jbyteArray jret_value = copyBytes(env, (char*)kv_key.data(), kv_key.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}

jbyteArray Java_com_yahoo_ycsb_db_Iterator_value(JNIEnv* env, jobject thisobj) {
	Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    leveldb::Slice kv_val = it->value();
    jbyteArray jret_value = copyBytes(env, (char*)kv_val.data(), kv_val.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}



static ReadOptions *_RO_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(ReadOptions**)&cpp_ptr;
}
static void _RO_set_java_ptr(JNIEnv *env, jobject thisObj, ReadOptions *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

void Java_com_yahoo_ycsb_db_ReadOptions_init(JNIEnv *env, jobject thisobj,
                                        jbyteArray jkey, jint jkey_len) {
    leveldb::ReadOptions *self = new leveldb::ReadOptions;
    jbyte* key;
    if (jkey_len == 0) {
        //self->upper_key = NULL;
    }
    else {
        key = new jbyte[jkey_len];
        env->GetByteArrayRegion(jkey, 0, jkey_len, key);
        //self->upper_key = new leveldb::Slice((const char*)key, jkey_len);
    }
    _RO_set_java_ptr(env, thisobj, self);
}

void Java_com_yahoo_ycsb_db_ReadOptions_destory(JNIEnv *env, jobject thisobj) {
    leveldb::ReadOptions *self = _RO_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        // if (self->upper_key != NULL) {
        //     delete [] (self->upper_key->data());
        //     delete (self->upper_key);
        // }
        delete self;
        _RO_set_java_ptr(env, thisobj, NULL);
    }
}
