#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <cstddef>
#include <fstream>
#include <iostream>
#include "com_yahoo_ycsb_db_Wisckey.h"
#include "com_yahoo_ycsb_db_Iterator.h"
#include "com_yahoo_ycsb_db_ReadOptions.h"

#include "json11.h"
#include "wisckey/slice.h"
#include "wisckey/db.h"

using namespace wisckey;

// global variables
wisckey::DB *db;
wisckey::Options options;
wisckey::WriteOptions wropts;

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_init(JNIEnv* env, jobject thisobj) { 
    std::ifstream ifs("wisckey_config.json");
    std::string file_content( (std::istreambuf_iterator<char>(ifs) ),
                       (std::istreambuf_iterator<char>()    ) );
    std::string err;
    if (file_content.size() == 0) { // default jason
        file_content = R"({"dir":"/mnt/pm983_block/eval_4000",
        				    "data_cache_size":16,
                            "index_cache_size":128 })";
        printf("Using default wisckey config file\n");
    }

    // parse json
    const auto config = json11::Json::parse(file_content, err);
		std::string dir = config["dir"].string_value();
    options.dataCacheSize = config["data_cache_size"].int_value() ;
    options.indexCacheSize = config["index_cache_size"].int_value() ;
    options.logBufSize = (size_t)1 << 20;
    options.statistics = wisckey::Options::CreateDBStatistics();
    int filterType = config["filter_type"].int_value();
    if (filterType == 1) options.filterType=wisckey::Bloom;
    else if (filterType == 2) options.filterType=wisckey::Surf;
    
    options.filterBitsPerKey = config["filter_bits_per_key"].int_value();
    db = NULL;
    wisckey::Status ret = wisckey::DB::Open(options, dir.c_str(), &db);
    return ret.ok();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_close(JNIEnv* env, jobject ) { 
    delete db;
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

JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_Wisckey_get(JNIEnv* env, jobject ,
                            jobject readoptionsObject, jbyteArray jkey, jint jkey_len) {
    jclass readoptionsClass = env->GetObjectClass(readoptionsObject);
    jfieldID fidcppPtr = env->GetFieldID(readoptionsClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(readoptionsObject, fidcppPtr);
    const wisckey::ReadOptions *rdopts =  *(wisckey::ReadOptions**)&cpp_ptr;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
  
    wisckey::Slice kv_key((const char*)key, jkey_len);
    std::string kv_val;
    wisckey::Status get_ret = db->Get(*rdopts, kv_key, &kv_val);
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

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_insert(JNIEnv* env, jobject ,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
    wisckey::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    wisckey::Slice kv_key((const char*)key, jkey_len);
    wisckey::Slice kv_val((const char*)value, jval_len);

    ret = db->Put(wropts, kv_key, kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret.ok();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_update(JNIEnv* env, jobject ,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
    wisckey::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    wisckey::Slice kv_key((const char*)key, jkey_len);
    wisckey::Slice kv_val((const char*)value, jval_len);

    ret = db->Put(wropts, kv_key, kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret.ok();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_delete(JNIEnv* env, jobject ,
                            jbyteArray jkey, jint jkey_len) {
    wisckey::Status ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
    
    wisckey::Slice kv_key((const char*)key, jkey_len);

    ret = db->Delete(wropts, kv_key);

    // cleanup
    delete[] key;

    return ret.ok();
}

static wisckey::Iterator *_IT_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(Iterator**)&cpp_ptr;
}
static void _IT_set_java_ptr(JNIEnv *env, jobject thisObj, wisckey::Iterator *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_Iterator_init(JNIEnv *env, jobject thisobj,
                                        jobject readoptionsObject) {
    jclass readoptionsClass = env->GetObjectClass(readoptionsObject);
    jfieldID fidcppPtr = env->GetFieldID(readoptionsClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(readoptionsObject, fidcppPtr);
    const wisckey::ReadOptions *rdopts =  *(wisckey::ReadOptions**)&cpp_ptr;
    wisckey::Iterator *self = db->NewIterator(*rdopts);
    _IT_set_java_ptr(env, thisobj, self);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_Iterator_destory(JNIEnv *env, jobject thisobj) {
    wisckey::Iterator *self = _IT_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        delete self;
        _IT_set_java_ptr(env, thisobj, NULL);
    }
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_Iterator_seek(JNIEnv *env, jobject thisobj,
                                        jbyteArray jkey, jint jkey_len) {
    wisckey::Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
    wisckey::Slice kv_key((const char*)key, jkey_len);

    it->Seek(kv_key);
    // cleanup
    delete[] key;
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_Iterator_next(JNIEnv *env, jobject thisobj) {
    wisckey::Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    it->Next();
}

JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Iterator_valid(JNIEnv *env, jobject thisobj) {
    wisckey::Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    return it->Valid();
}

JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_Iterator_key(JNIEnv* env, jobject thisobj) {
	wisckey::Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    wisckey::Slice kv_key = it->key();
    jbyteArray jret_value = copyBytes(env, (char*)kv_key.data(), kv_key.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}

JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_Iterator_value(JNIEnv* env, jobject thisobj) {
	wisckey::Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    wisckey::Slice kv_val = it->value();
    jbyteArray jret_value = copyBytes(env, (char*)kv_val.data(), kv_val.size());
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}


static wisckey::ReadOptions *_RO_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(ReadOptions**)&cpp_ptr;
}
static void _RO_set_java_ptr(JNIEnv *env, jobject thisObj, wisckey::ReadOptions *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_ReadOptions_init(JNIEnv *env, jobject thisobj,
                                        jbyteArray jkey, jint jkey_len) {
    wisckey::ReadOptions *self = new wisckey::ReadOptions;
    jbyte* key;
    if (jkey_len == 0) {
        self->upper_key = NULL;
    }
    else {
        key = new jbyte[jkey_len];
        env->GetByteArrayRegion(jkey, 0, jkey_len, key);
        self->upper_key = new wisckey::Slice((const char*)key, jkey_len);
    }
    _RO_set_java_ptr(env, thisobj, self);
}

JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_ReadOptions_destory(JNIEnv *env, jobject thisobj) {
    wisckey::ReadOptions *self = _RO_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        if (self->upper_key != NULL) {
            delete [] (self->upper_key->data());
            delete (self->upper_key);
        }
        delete self;
        _RO_set_java_ptr(env, thisobj, NULL);
    }
}



