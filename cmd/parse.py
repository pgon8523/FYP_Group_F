import re
import sys
import json 

pat = re.compile(r'^.+Planning Time:\s+([0-9]+\.[0-9]+)\s+ms\s*$')
pat2 = re.compile(r'^.+Execution Time:\s+([0-9]+\.[0-9]+)\s+ms\s*$')


# sys.argv[1], [2], [3], passing 3 parameters after python3 parse.py 

# file path of log_join
f = open(sys.argv[1], "r")
lines = f.readlines()
lines = [ l.strip("\n") for l in lines ]

# file path of temp.sql
f2 = open(sys.argv[2], "r")
lines2 = f2.readlines()
lines2 = [ l.strip("\n") for l in lines2 ]

# file path of nngp_log_join
f3 = open(sys.argv[3], "r")
lines3 = f3.readlines()
lines3 = [ l.strip("\n") for l in lines3 ]

returnObj = {}
returnList = []

num = 0
# the performance of postgresql planner
for l in lines:
    m = pat.match(l)
    if m is not None:
        returnObj["q" + str(num)] = {}
        returnObj["q" + str(num)]["key"] = str(num + 1)
        returnObj["q" + str(num)]["Query"] = "q" + str(num + 1)
        returnObj["q" + str(num)]["Planning"] = m.group(1)
    m2 = pat2.match(l)
    if m2 is not None:
        returnObj["q" + str(num)]["Execution"] = m2.group(1)
        # returnObj["Execution"] = m2.group(1)
        num += 1

num = 0

# the performance of nngp model
for l in lines3:
    m = pat.match(l)
    if m is not None:
        returnObj["q" + str(num)]["NNGP_Planning"] = m.group(1)
        # returnObj["NNGP_Planning"] = m.group(1)
    m2 = pat2.match(l)
    if m2 is not None:
        returnObj["q" + str(num)]["NNGP_Execution"] = m2.group(1)
        # returnObj["NNGP_Execution"] = m2.group(1)
        num += 1

# the sql command 
for i in range(len(lines2)):
    returnObj["q" + str(i)]["SQL"] = lines2[i].replace("explain analyze ","")
    # returnObj["q" + str(i)]["SQL"] = lines2[i]
    # returnObj["SQL"] = lines2[i].replace("explain analyze ","")


f.close()
f2.close()
f3.close()

returnList = [ returnObj[k] for k in returnObj.keys() ]
# print(json.dumps(returnObj, indent = 4))
print(json.dumps(returnList, indent = 4))
