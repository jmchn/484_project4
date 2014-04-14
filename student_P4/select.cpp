#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>
#include "assert.h"
/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
//calculate the reclen
    int len=0;
    int j ;
//convert the attrInfo TO AttrDesc
    AttrDesc * projNamesDesc, *attrDesc;
    projNamesDesc = new AttrDesc [projCnt];
    // in this project 
    // the projectNames come from a same relation, thus there is no need checking the relName
    Status statusGetProjectName;
    int attrCnt;
    AttrDesc * attrInCata,*temp;
    assert(projNames!= NULL);
    statusGetProjectName = attrCat->getRelInfo(projNames[0].relName,attrCnt,attrInCata);
    temp = attrInCata;
    if(statusGetProjectName!=OK)
    {
        return statusGetProjectName;
    }
    
    for(j = 0 ; j < projCnt ; j++)
    {
        int k ;
        for ( k = 0 ,temp = attrInCata; k < attrCnt ; k++,temp ++)
        {
            if(attr !=NULL)
            {
                if(strcmp(attr->attrName, temp->attrName)==0)
                {
                    attrDesc =temp;
                }
            }
           
            if(strcmp(projNames[j].attrName,temp->attrName)==0)
            {
                projNamesDesc[j]=*temp;
                len += temp->attrLen;
            }
        }
    }
    if(attr ==NULL)
    {
        attrDesc = NULL;
    }
//make a decition on which method to use

    if(attr !=NULL)
    {
        int i;
        for(i = 0,temp= attrInCata; i <attrCnt ; i ++, temp++)
        {
            if(strcmp(attr->attrName,temp->attrName)==0)
            {
                if(temp->indexed == 1 && op == EQ)// this attribute is indexed and the op is equal
                {
                    // do indexselect
                    Status statusIndexSelect;

                    statusIndexSelect =IndexSelect(result,projCnt,projNamesDesc,attrDesc,op,attrValue,len);
                 
                    return statusIndexSelect;
                    
                }
            }
        }
    }
    Status statusScanSelect;

    statusScanSelect =ScanSelect(result,projCnt,projNamesDesc,attrDesc,op,attrValue,len);
    return statusScanSelect;
}

