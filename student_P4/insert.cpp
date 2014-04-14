#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>
#include <map>
#include "index.h"
#include "utility.h"

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */
    // look up the sytemlog to determine the attributes 
    int attrCntInCata;
    AttrDesc* attrInCata, * temp;
    Status statusGetRelInfo;
    statusGetRelInfo=attrCat->getRelInfo(relation,attrCntInCata,attrInCata);
    if(statusGetRelInfo!=OK)
    {
        return statusGetRelInfo;
    }
    temp = attrInCata;
    //check whether this is a valid
    //the attrCnt is the local value , attrCntInCata is the global one , they shall match
    if ( attrCntInCata != attrCnt)
    {
        return ATTRTYPEMISMATCH;
    }
    // determine the length of record.data
    int i ;
    int length = 0;
    for(i = 0 ; i < attrCntInCata ; i ++ )
    {
        length += temp->attrLen;
        temp++;
    }
    char* data = new char[length];
    int j;
    map<int,void*> valueMap; 
    for(i = 0 ; i < attrCnt ; i ++)
    {
        for(j = 0,temp = attrInCata ; j < attrCntInCata ; j ++,temp++ )
        {
            if(strcmp(temp->attrName,attrList[i].attrName)==0)
            {
                // their attrname is the same
                if(temp->attrType != attrList[i].attrType)
                {
                    // the type isn't match
                    return ATTRTYPEMISMATCH;
                }
                valueMap.insert(make_pair(j,attrList[i].attrValue));
                memcpy(data+temp->attrOffset,attrList[i].attrValue,temp->attrLen);
                break;
            }
        }
    }
    Status returnStatus;
    HeapFile heapfile(relation, returnStatus);
    if(returnStatus != OK)
    {
        return returnStatus;
    }
    Record recordToInsert;
    recordToInsert.data = (void *)data;
    recordToInsert.length = length;
    RID outRid;
    Status heapfileInsertStatus;
    heapfileInsertStatus = heapfile.insertRecord(recordToInsert,outRid);
    if(heapfileInsertStatus != OK)
    {
        return heapfileInsertStatus;
    }
    // got rid
    // insert into the indecies;
    // find the attributes with index
    for(i = 0 ,temp = attrInCata; i < attrCntInCata ; i ++,temp++)
    {
        if(temp->indexed == 1)
        {
            // this attribute is indexed
            Status statusIndex;
            Index tempIndex(relation,temp->attrOffset,temp->attrLen,(Datatype) temp->attrType,0,statusIndex);
            if(statusIndex != OK)
            {
                return statusIndex;
            }
            Status statusInsert;
            statusInsert = tempIndex.insertEntry(valueMap[i],outRid);
            if(statusInsert != OK)
            {
                return statusInsert;
            }

        }
    }
    // remember to delete attrInCata
    delete []  attrInCata;
    // debug info
    //Utilities::Print(relation);
    return OK;
}
