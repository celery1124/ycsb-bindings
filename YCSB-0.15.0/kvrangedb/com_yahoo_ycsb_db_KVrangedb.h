/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_yahoo_ycsb_db_KVrangedb */

#ifndef _Included_com_yahoo_ycsb_db_KVrangedb
#define _Included_com_yahoo_ycsb_db_KVrangedb
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    init
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_init
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    close
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_close
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    getBatchSize
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_com_yahoo_ycsb_db_KVrangedb_getBatchSize
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    get
 * Signature: (Lcom/yahoo/ycsb/db/ReadOptionsV2;[BI)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_KVrangedb_get
  (JNIEnv *, jobject, jobject, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    insert
 * Signature: ([BI[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_insert
  (JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    update
 * Signature: ([BI[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_update
  (JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    delete
 * Signature: ([BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_delete
  (JNIEnv *, jobject, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    writeBatch
 * Signature: (Lcom/yahoo/ycsb/db/WriteBatch;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVrangedb_writeBatch
  (JNIEnv *, jobject, jobject);

/*
 * Class:     com_yahoo_ycsb_db_KVrangedb
 * Method:    hello
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_KVrangedb_hello
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif