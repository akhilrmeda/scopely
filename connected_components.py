import sys
import itertools
import time 
import threading
import pymongo
from pymongo import MongoClient
threading.stack_size(134217728)
sys.setrecursionlimit(2 ** 22)


def recursion(neighbours,component):
    for neighbour in neighbours:
        for item in collection_pagemap.find({"_id":neighbour},timeout=False):
            if item["cc"]==False:   
                collection_pagemap.update({"_id":neighbour},{'$set':{"cc":component}},False,False)
                recursion(item["neighbours"],component)
    return 

if __name__=='__main__':
    start_time = time.time()
    
    client = pymongo.MongoClient("localhost", 27017)
    db=client.scopely
    collection_pagemap=db.pagemap

    count=0
    for item in collection_pagemap.find(timeout=False):
        
        count+=1
        print(count)
        collection_pagemap.update({"_id":item["_id"]},{'$set':{"cc":False}},False,False)

    component=0
    count=0

    for item in collection_pagemap.find(timeout=False):
        break
        count+=1
        print (count)
        if item["cc"]==False:
            component+=1
            collection_pagemap.update({"_id":item["_id"]},{'$set':{"cc":component}},False,False)
            thread = threading.Thread(target=recursion(item["neighbours"],component))
            thread.start()
            thread.exit()      
                  
    client.close()
    print("----done-----")
    print (time.time() - start_time, "seconds")

    

