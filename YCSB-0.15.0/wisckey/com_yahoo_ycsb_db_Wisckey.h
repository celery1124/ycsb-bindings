/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_yahoo_ycsb_db_Wisckey */

#ifndef _Included_com_yahoo_ycsb_db_Wisckey
#define _Included_com_yahoo_ycsb_db_Wisckey
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_yahoo_ycsb_db_Wisckey
 * Method:    init
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_init
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_Wisckey
 * Method:    close
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_close
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_Wisckey
 * Method:    get
 * Signature: (Lcom/yahoo/ycsb/db/ReadOptions;[BI)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_Wisckey_get
  (JNIEnv *, jobject, jobject, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_Wisckey
 * Method:    insert
 * Signature: ([BI[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_insert
  (JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_Wisckey
 * Method:    update
 * Signature: ([BI[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_update
  (JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_Wisckey
 * Method:    delete
 * Signature: ([BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_Wisckey_delete
  (JNIEnv *, jobject, jbyteArray, jint);

#ifdef __cplusplus
}
#endif
#endif
