#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>

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
    attrCat->getRelInfo(relation,attrCntInCata,attrInCata);
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
                memcpy(data+temp->attrOffset,attrList[i].attrValue,temp->attrLen);
                break;
            }
        }
    }
    // remember to delete attrInCata
    delete []  attrInCata;
    return OK;
}
