#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <cstddef>
#include <fstream>
#include <iostream>
#include "com_yahoo_ycsb_db_KVrangedb.h"
#include "com_yahoo_ycsb_db_WriteBatch.h"
#include "com_yahoo_ycsb_db_IteratorV2.h"
#include "com_yahoo_ycsb_db_ReadOptionsV2.h"

#include "json11.h"
#include "kvrangedb/slice.h"
#include "kvrangedb/write_batch.h"
#include "kvrangedb/comparator.h"
#include "kvrangedb/db.h"

using namespace kvrangedb;

class CustomComparator : public kvrangedb::Comparator {
public:
  CustomComparator() {}
  ~CustomComparator() {}
  int Compare(const kvrangedb::Slice& a, const kvrangedb::Slice& b) const {
    return a.compare(b);
  }
};

// global variables
kvrangedb::DB *db;
CustomComparator cmp;
kvrangedb::Options options;
kvrangedb::WriteOptions wropts;
int batch_size;

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_init(JNIEnv* env, jobject thisobj) { 
    std::ifstream ifs("kvrangedb_config.json");
    std::string file_content( (std::istreambuf_iterator<char>(ifs) ),
                       (std::istreambuf_iterator<char>()    ) );
    std::string err;
    if (file_content.size() == 0) { // default jason
        file_content = R"({"index_type":0,
        								"batch_Size":6})";
        printf("Using default kvrangedb config file\n");
    }

    // parse json
    const auto config = json11::Json::parse(file_content, err);
		std::string dev_name = config["dev_name"].string_value();
    int index_type = config["index_type"].int_value();
    options.indexNum = config["index_num"].int_value();
    options.packThreadsNum = config["pack_threads_num"].int_value();
    options.packSize = config["pack_size_max"].int_value();
    options.packThres = config["pack_size_thres"].int_value();
    options.maxPackNum = config["pack_num_max"].int_value();
    options.dataCacheSize = config["data_cache_size"].int_value() ;
    options.indexCacheSize = config["index_cache_size"].int_value() ;
    options.prefetchDepth = config["max_prefetch_depth"].int_value() ;
    options.statistics = kvrangedb::Options::CreateDBStatistics();
    options.packThreadsDisable = config["pack_disable"].bool_value();
    wropts.batchIDXWrite = config["index_batch"].bool_value();
    wropts.batchIDXSize = config["index_batch_size"].int_value();    

    int rf_type = config["rf_type"].int_value();
    if (rf_type == 1) {
        options.rfType = kvrangedb::HiBloom;
    }
    else if (rf_type == 2) {
        options.rfType = kvrangedb::RBloom;
    }
    options.rfNumKeys = config["rf_num_keys"].int_value();
    options.rangefilterEnabled = config["rf_enable"].bool_value();
    options.rfBitsPerKey = config["rf_bits_per_key"].int_value();
    options.rfLevels = config["rf_levels"].int_value();

    options.packDequeueTimeout = 500;
    batch_size = config["batch_size"].int_value();
    switch (index_type) {
    case 0 :
        options.indexType = kvrangedb::LSM;
        printf("%s open, LSM index initiated\n", dev_name.c_str());
        break;
    case 1 :
        options.indexType = kvrangedb::LSMOPT;
        printf("%s open, LSMOPT index initiated\n", dev_name.c_str());
        break;
    case 2 :
        options.indexType = kvrangedb::ROCKS;
        printf("%s open, LSMRocks index initiated\n", dev_name.c_str());
        break;
    case 3 :
        options.indexType = kvrangedb::BTREE;
        printf("%s open, BTREE index initiated\n", dev_name.c_str());
        break;
    case 4 :
        options.indexType = kvrangedb::BASE;
        printf("%s open, BASE index initiated\n", dev_name.c_str());
        break;
    case 5 :
        options.indexType = kvrangedb::INMEM;
        printf("%s open, INMEM index initiated\n", dev_name.c_str());
        break;
    default :
        return false;
    }
    fprintf(stderr, "%s open, %d index initiated\n", dev_name.c_str(), options.indexType);
    options.comparator = &cmp;
    db = NULL;
    kvrangedb::Status ret = kvrangedb::DB::Open(options, dev_name.c_str(), &db);
    return ret.ok();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_close(JNIEnv* env, jobject ) { 
    delete db;
}

JNIEXPORT jint JNICALL Java_com_yahoo_ycsb_db_KVrangedb_getBatchSize (JNIEnv *env, jobject) {
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

JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_KVrangedb_get(JNIEnv* env, jobject ,
                            jobject readoptionsObject, jbyteArray jkey, jint jkey_len) {
    jclass readoptionsClass = env->GetObjectClass(readoptionsObject);
    jfieldID fidcppPtr = env->GetFieldID(readoptionsClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(readoptionsObject, fidcppPtr);
    const kvrangedb::ReadOptions *rdopts =  *(kvrangedb::ReadOptions**)&cpp_ptr;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
  
    kvrangedb::Slice kv_key((const char*)key, jkey_len);
    std::string kv_val;
    kvrangedb::Status get_ret = db->Get(*rdopts, kv_key, &kv_val);
    if (get_ret.IsNotFound()) return nullptr;

    jbyteArray jret_value = copyBytes(env, (char*)kv_val.data(), kv_val.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }
//fprintf(stderr, "getKey %s\n", std::string((char*)key, jkey_len).c_str());
    // cleanup
    delete[] key;

    return jret_value;
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_insert(JNIEnv* env, jobject ,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
    kvrangedb::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    kvrangedb::Slice kv_key((const char*)key, jkey_len);
    kvrangedb::Slice kv_val((const char*)value, jval_len);

    ret = db->Put(wropts, kv_key, kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret.ok();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_update(JNIEnv* env, jobject ,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
    kvrangedb::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    kvrangedb::Slice kv_key((const char*)key, jkey_len);
    kvrangedb::Slice kv_val((const char*)value, jval_len);

    ret = db->Put(wropts, kv_key, kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret.ok();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_delete(JNIEnv* env, jobject ,
                            jbyteArray jkey, jint jkey_len) {
    kvrangedb::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
    
    kvrangedb::Slice kv_key((const char*)key, jkey_len);

    ret = db->Delete(wropts, kv_key);

    // cleanup
    delete[] key;

    return ret.ok();
}


JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_writeBatch(JNIEnv* env, jobject thisObj ,
                            jobject writebatchObject) {
    jclass writebatchClass = env->GetObjectClass(writebatchObject);
    jfieldID fidcppPtr = env->GetFieldID(writebatchClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(writebatchObject, fidcppPtr);
    WriteBatch *batch =  *(WriteBatch**)&cpp_ptr;
    
	kvrangedb::Status ret;
    
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

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_init(JNIEnv *env, jobject thisobj) {
    WriteBatch *self = new WriteBatch();
    _WB_set_java_ptr(env, thisobj, self);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_destory(JNIEnv *env, jobject thisobj) {
    WriteBatch *self = _WB_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        delete self;
        _WB_set_java_ptr(env, thisobj, NULL);
    }
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_put(JNIEnv* env, jobject thisobj ,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len, jbyte jtype) {
    WriteBatch *batch = _WB_get_cpp_ptr(env, thisobj);
    assert(batch != NULL);


    char* key = new char[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, (jbyte *)key);

    char* value = new char[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, (jbyte *)value);
    
    kvrangedb::Slice *kv_key = new kvrangedb::Slice(key, jkey_len);
	kvrangedb::Slice *kv_val = new kvrangedb::Slice(value, jval_len);

    batch->Put(*kv_key, *kv_val);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_clear(JNIEnv *env, jobject thisobj) {
    WriteBatch *batch = _WB_get_cpp_ptr(env, thisobj);
    assert(batch != NULL);

    batch->Clear();
}

JNIEXPORT jint JNICALL Java_com_yahoo_ycsb_db_WriteBatch_size(JNIEnv *env, jobject thisobj) {
    WriteBatch *batch = _WB_get_cpp_ptr(env, thisobj);
    assert(batch != NULL);

    return batch->Size();
}

static kvrangedb::Iterator *_IT_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(Iterator**)&cpp_ptr;
}
static void _IT_set_java_ptr(JNIEnv *env, jobject thisObj, kvrangedb::Iterator *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_init(JNIEnv *env, jobject thisobj,
                                        jobject readoptionsObject) {
    jclass readoptionsClass = env->GetObjectClass(readoptionsObject);
    jfieldID fidcppPtr = env->GetFieldID(readoptionsClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(readoptionsObject, fidcppPtr);
    const kvrangedb::ReadOptions *rdopts =  *(kvrangedb::ReadOptions**)&cpp_ptr;
    kvrangedb::Iterator *self = db->NewIterator(*rdopts);
    _IT_set_java_ptr(env, thisobj, self);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_destory(JNIEnv *env, jobject thisobj) {
    kvrangedb::Iterator *self = _IT_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        delete self;
        _IT_set_java_ptr(env, thisobj, NULL);
    }
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_seek(JNIEnv *env, jobject thisobj,
                                        jbyteArray jkey, jint jkey_len) {
    kvrangedb::Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
    kvrangedb::Slice kv_key((const char*)key, jkey_len);

    it->Seek(kv_key);
    // cleanup
    delete[] key;
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_next(JNIEnv *env, jobject thisobj) {
    kvrangedb::Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    it->Next();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_IteratorV2_valid(JNIEnv *env, jobject thisobj) {
    kvrangedb::Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    return it->Valid();
}

JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_IteratorV2_key(JNIEnv* env, jobject thisobj) {
	kvrangedb::Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    kvrangedb::Slice kv_key = it->key();
    jbyteArray jret_value = copyBytes(env, (char*)kv_key.data(), kv_key.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}

JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_IteratorV2_value(JNIEnv* env, jobject thisobj) {
	kvrangedb::Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    kvrangedb::Slice kv_val = it->value();
    jbyteArray jret_value = copyBytes(env, (char*)kv_val.data(), kv_val.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}


static kvrangedb::ReadOptions *_RO_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(ReadOptions**)&cpp_ptr;
}
static void _RO_set_java_ptr(JNIEnv *env, jobject thisObj, kvrangedb::ReadOptions *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_ReadOptionsV2_init(JNIEnv *env, jobject thisobj,
                                        jbyteArray jkey, jint jkey_len) {
    kvrangedb::ReadOptions *self = new kvrangedb::ReadOptions;
    jbyte* key;
    if (jkey_len == 0) {
        self->upper_key = NULL;
    }
    else {
        key = new jbyte[jkey_len];
        env->GetByteArrayRegion(jkey, 0, jkey_len, key);
        self->upper_key = new kvrangedb::Slice((const char*)key, jkey_len);
    }
    _RO_set_java_ptr(env, thisobj, self);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_ReadOptionsV2_setscan(JNIEnv *env, jobject thisobj, jint scan_len) {
    kvrangedb::ReadOptions *self = _RO_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
    	self->scan_length = scan_len;
    	_RO_set_java_ptr(env, thisobj, self);
    }
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_ReadOptionsV2_setpack(JNIEnv *env, jobject thisobj, jint pack_hint) {
    kvrangedb::ReadOptions *self = _RO_get_cpp_ptr(env, thisobj);
    if (self != NULL) {
    	self->hint_packed = pack_hint;
    	_RO_set_java_ptr(env, thisobj, self);
    }
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_ReadOptionsV2_destory(JNIEnv *env, jobject thisobj) {
    kvrangedb::ReadOptions *self = _RO_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        if (self->upper_key != NULL) {
            delete [] (self->upper_key->data());
            delete (self->upper_key);
        }
        delete self;
        _RO_set_java_ptr(env, thisobj, NULL);
    }
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_KVrangedb_hello
  (JNIEnv *env, jobject thisobj) {
  printf("KVrangedb hello\n");
}

