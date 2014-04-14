#include "catalog.h"
#include "query.h"
#include "index.h"
#include "heapfile.h"
#include <cstring>
/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
  cout << "Algorithm: File Scan" << endl;
  
     /* Your solution goes here */ 
     // initialization to HeapFileScan
    Status statusAfterScan;
    // whether the attrDesc is NULL or not;
    HeapFileScan* findMatch;
    if( attrDesc == NULL)
    {
         findMatch = new HeapFileScan(projNames[0].relName,statusAfterScan);
    }
    else
    {
        findMatch = new HeapFileScan(attrDesc->relName,attrDesc->attrOffset,attrDesc->attrLen,(Datatype) attrDesc->attrType,(char*)attrValue,op,statusAfterScan);
    }
    if(statusAfterScan!= OK)
    {
        return statusAfterScan;
    }
    //initialization of HeapFile
    Status statusAfterOpenResult;
    HeapFile resultHeapFile(result,statusAfterOpenResult);
    if(statusAfterOpenResult!=OK)
    {
        return statusAfterOpenResult;
    }
    RID outRid;
    Record rec;
    Status statusGetNext;
    while((statusGetNext = findMatch->scanNext(outRid,rec)) ==OK)
    {
        int i;
        Record resultRec;
        RID dummy;// no use
        char* data = new char[reclen];
        char* temp = data;
        for( i = 0 ; i < projCnt ; i++)
        {
            memcpy(temp,(char*)rec.data+projNames[i].attrOffset,projNames[i].attrLen);
            temp = temp + projNames[i].attrLen;
        }
        resultRec.data = (void*) data;
        resultRec.length = reclen;
        Status statusInsertRec;
        statusInsertRec = resultHeapFile.insertRecord(resultRec,dummy);
        if(statusInsertRec != OK)
        {
            return statusInsertRec;
        }
    }
    if(statusGetNext != FILEEOF)
    {
        return statusGetNext;
    }
    delete findMatch;

    return OK;
}
