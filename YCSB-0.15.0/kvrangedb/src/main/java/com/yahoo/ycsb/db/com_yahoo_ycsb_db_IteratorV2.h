/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_yahoo_ycsb_db_IteratorV2 */

#ifndef _Included_com_yahoo_ycsb_db_IteratorV2
#define _Included_com_yahoo_ycsb_db_IteratorV2
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_yahoo_ycsb_db_IteratorV2
 * Method:    init
 * Signature: (Lcom/yahoo/ycsb/db/ReadOptionsV2;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_init
  (JNIEnv *, jobject, jobject);

/*
 * Class:     com_yahoo_ycsb_db_IteratorV2
 * Method:    destory
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_destory
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_IteratorV2
 * Method:    seek
 * Signature: ([BI)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_seek
  (JNIEnv *, jobject, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_IteratorV2
 * Method:    next
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_IteratorV2_next
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_IteratorV2
 * Method:    valid
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_IteratorV2_valid
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_IteratorV2
 * Method:    key
 * Signature: ()[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_IteratorV2_key
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_IteratorV2
 * Method:    value
 * Signature: ()[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_IteratorV2_value
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif
