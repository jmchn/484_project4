#include "catalog.h"
#include "query.h"
#include "index.h"
#include "heapfile.h"
#include <cstring>
Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
    cout << "Algorithm: Index Select" << endl;

    /* Your solution goes here */
    //initial the heapfilescan
    Status statusHeapFileScan;
    HeapFileScan attrHeapFileScan(attrDesc->relName,statusHeapFileScan);
    if(statusHeapFileScan != OK)
    {
        return statusHeapFileScan;
    }
    //initial the result heapfile
    Status statusAfterOpenResult;
    HeapFile resultHeapFile(result,statusAfterOpenResult);
    if(statusAfterOpenResult!=OK)
    {
        return statusAfterOpenResult;
    }
    // find the index
    Status statusIndex;
    Index attrIndex(attrDesc->relName,attrDesc->attrOffset,attrDesc->attrLen,(Datatype)attrDesc->attrType,0,statusIndex);
    if(statusIndex != OK)
    {
        return statusIndex;
    }
    Status statusStartScan;
    statusStartScan = attrIndex.startScan(attrValue);
    if(statusStartScan != OK)
    {
        return statusStartScan;
    }
    RID rid;
    Status statusScanNext;
    // scan for the value and get the rid
    while((statusScanNext = attrIndex.scanNext(rid))== OK)
    {
        // getrandom rec for the rid
        Record attrRec;
        Status statusGetRandomRec;
        statusGetRandomRec = attrHeapFileScan.getRandomRecord(rid,attrRec); 
        if(statusGetRandomRec !=OK)
        {
            return statusGetRandomRec;
        }
        int i;
        Record resultRec;
        RID dummy;// no use
        char* data = new char[reclen];
        char* temp = data;
        for( i = 0 ; i < projCnt ; i++)
        {
            memcpy(temp,(char*)attrRec.data+projNames[i].attrOffset,projNames[i].attrLen);
            temp = temp + projNames[i].attrLen;
        }
        resultRec.data = (void*) data;
        resultRec.length = reclen;
        Status statusInsertRec;
        statusInsertRec =  resultHeapFile.insertRecord(resultRec,dummy);
        if(statusInsertRec !=OK)
        {
            return statusInsertRec;
        }
    }
    if( statusScanNext != NOMORERECS)
    {
        return statusScanNext;
    }
    // end the index scan
    Status statusEndScan;
    statusEndScan = attrIndex.endScan();
    if(statusStartScan != OK)
    {
        return statusEndScan;
    }
    return OK;
}

