#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include "assert.h"
#include <cstring>
/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The right attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
    cout << "Algorithm: Indexed NL Join" << endl;

    /* Your solution goes here */
    // because this is a EQ join . 
    assert(op == EQ);
    // initialization ------------------- begin
    // initial the Index heapFileScan
    Status statusHeapFileScan;
    HeapFileScan attrDesc2HeapFileScan(attrDesc2.relName,statusHeapFileScan);
    if(statusHeapFileScan != OK)
    {
        return statusHeapFileScan;
    }
    // initial the attrDesc1 heapfileScan
    HeapFileScan attrDesc1HeapFileScan(attrDesc1.relName,statusHeapFileScan);
    if(statusHeapFileScan != OK)
    {
        return statusHeapFileScan;
    }
    // initial the result HeapFile
    Status statusAfterOpenResult;
    HeapFile resultHeapFile(result,statusAfterOpenResult);
    if(statusAfterOpenResult!=OK)
    {
        return statusAfterOpenResult;
    }
    // initialization --------------------end
    // find the index on attrDesc2;
    Status statusIndex;
    Index attrIndex(attrDesc2.relName,attrDesc2.attrOffset,attrDesc2.attrLen,(Datatype)attrDesc2.attrType,0,statusIndex);
    if(statusIndex != OK)
    {
        return statusIndex;
    } 
    // start iteration of the outer relation
    Status statusAttr1HeapScan;
    RID rid1;
    Record record1;
    while((statusAttr1HeapScan = attrDesc1HeapFileScan.scanNext(rid1,record1))==OK)
    {
        char * data1;
        data1 = new char[attrDesc1.attrLen];
        memcpy(data1,(char*)record1.data+attrDesc1.attrOffset,attrDesc1.attrLen);
        Status statusStartScan;
        statusStartScan = attrIndex.startScan((void*)data1);
        if(statusStartScan != OK)
        {
            return statusStartScan;
        }
        RID rid2;
        Status statusScanNext;
        // look up the index 
        while((statusScanNext = attrIndex.scanNext(rid2))==OK)
        {
            Record record2;
            Status statusGetRandomRec;
            statusGetRandomRec = attrDesc2HeapFileScan.getRandomRecord(rid2,record2);
            if(statusGetRandomRec != OK)
            {
                return statusGetRandomRec;
            }
            int i;
            Record resultRec;
            RID dummy;
            // insert into the result;
            char * dataRlt ;
            dataRlt = new char [reclen];
            char * temp = dataRlt;
            for(i = 0 ; i < projCnt ; i ++)
            {
                if( strcmp(attrDesc1.relName,attrDescArray[i].relName)==0)
                {
                    // if this is an attribute of first relation
                    memcpy(temp,(char*)record1.data+attrDescArray[i].attrOffset,attrDescArray[i].attrLen);
                    temp = temp+attrDescArray[i].attrLen;
                }
                else
                {
                    // this must be an attribute of the second relation
                    assert(strcmp(attrDesc2.relName,attrDescArray[i].relName)==0);

                    memcpy(temp,(char*)record2.data+attrDescArray[i].attrOffset,attrDescArray[i].attrLen);
                    temp = temp+attrDescArray[i].attrLen;
                }
            }
            // insert to the result record;
            resultRec.data = (void*) dataRlt;
            resultRec.length = reclen;
            Status statusInsertRec;
            statusInsertRec = resultHeapFile.insertRecord(resultRec,dummy);
            if(statusInsertRec != OK)
            {
                return statusInsertRec;
            }
        }
        if(statusScanNext !=NOMORERECS)
        {
            return statusScanNext;
        }
        Status statusEndScan;
        statusEndScan = attrIndex.endScan();
        if(statusEndScan != OK)
        {
            return statusEndScan;
        }

    }
    if(statusAttr1HeapScan != FILEEOF)
    {
        return statusAttr1HeapScan;
    }



    return OK;
}

