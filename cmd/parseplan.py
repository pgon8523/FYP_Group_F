import sys
import re
from colour import Color

color_range = 100
red = Color("#f54304")
yellow = Color("#dcc700")
green = Color("#08ac2f")
dark_green = Color("#038f8d")
blue = Color("#0c0e73")

colors = list(blue.range_to(dark_green, 10)) + list(dark_green.range_to(green, 30)) + list(green.range_to(yellow, 30)) + list(yellow.range_to(red, 30))

op = ["PS", "MJ", "HJ",  "NJ", "HASH", "HASH-NO-EXE", "SORT", "SORT-NO-EXE", "SEQ", "PSEQ", "SEQ-NO-EXE", "MAT", "GATHER"]
pat = {}
pat["PS"] = re.compile(r'^.+Partial\sSort.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["MJ"] = re.compile(r'^.+Merge\sJoin.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["HJ"] = re.compile(r'^.+Hash\sJoin.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["NJ"] = re.compile(r'^.+Nested\sLoop.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["HASH"] = re.compile(r'^.+Hash.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["HASH-NO-EXE"] = re.compile(r'^.+Hash.+cost=([0-9]+\.[0-9]+).+rows=([0-9]+).+never\sexecuted.+$')
# pat["HASH_INFO"] = re.compile(r'^Buckets:\s+([0-9]+)\s+Batches:\s+([0-9]+)\s+Memory Usage:\s+([0-9]+)kB.+$')
pat["MAT"] = re.compile(r'^.+Materialize.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["GATHER"] = re.compile(r'^.+Gather.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["SORT"] = re.compile(r'^.+Sort.+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["SORT-NO-EXE"] = re.compile(r'^.+Sort.+cost=([0-9]+\.[0-9]+).+rows=([0-9]+).+never\sexecuted.+$')
pat["SEQ"] = re.compile(r'^.+Seq\sScan\son\s([a-z\_0-9]+).+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["PSEQ"] = re.compile(r'^.+Parallel\sSeq\sScan\son\s([a-z\_0-9]+).+actual\stime=([0-9]+\.[0-9]+)\.\.([0-9]+\.[0-9]+)\srows=([0-9]+)\sloops.+$')
pat["SEQ-NO-EXE"] = re.compile(r'^.+Seq\sScan\son\s([a-z\_]+).+rows=([0-9]+).+never\sexecuted.+$')
pat["End"] = re.compile(r'^.+Execution Time.+$')
pat["SpaceErr"] = re.compile(r"^.+No space left on device$")
pat["TimeOutErr"] = re.compile(r"^.*ERROR:\s+canceling statement due to statement timeout$")
pat["BugErr"] = re.compile(r"^.*server closed the connection.*$")

ini_indent = 1
indent = 6
f = open(sys.argv[1], "r")
fres = open(sys.argv[1] + ".csv", "w")
lines = f.readlines()
lines = [ l.strip('\n') for l in lines ]


fbsort = open(sys.argv[1] + ".bsort.csv", "w")

def process_raw_lines(lines: list):
    statements = []
    for l in lines:
        m = {}
        match = True
        for o in op:
            m[o] = pat[o].match(l)
        if m["PS"] is not None:
            nowm = m["PS"]
            first_c = 'P'
            s = { "name": 'PS', "isjoin": False, "issort": True, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": [] }
            pass
        elif m["MJ"] is not None:
            nowm = m["MJ"]
            first_c = 'M'
            s = { "name": 'MJ', "isjoin": True, "issort": False, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": [] }
            pass
        elif m["HJ"] is not None:
            nowm = m["HJ"]
            first_c = "H"
            s = { "name": "HJ", "isjoin": True, "issort": False, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": []}
        elif m["NJ"] is not None:
            nowm = m["NJ"]
            first_c = "N"
            s = { "name": "NJ", "isjoin": True, "issort": False, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": []}
        elif m["HASH"] is not None:
            nowm = m["HASH"]
            first_c = "H"
            s = { "name": "HASH", "isjoin": False, "issort": False, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": []}
        # elif m["HASH_INFO"] is not None:
        #     nowm = m["HASH_INFO"]
        #     first_c = "H"
        #     s = { "name": "HASH_INFO", "isjoin": False, "issort": False, "stime": 0., "etime": 0., "tuples": float(nowm.group(3)), "tables": [], "mem": float(nowm.group(3))}
        elif m["HASH-NO-EXE"] is not None:
            nowm = m["HASH-NO-EXE"]
            first_c = "H"
            s = { "name": "HASH", "isjoin": False, "issort": False, "stime": float(0), "etime": float(0), "tuples": float(nowm.group(2)), "tables": []}

        elif m["MAT"] is not None:
            nowm = m["MAT"]
            first_c = "M"
            s = { "name": 'MAT', "isjoin": False, "issort": False, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": [] }
            pass
        elif m["GATHER"] is not None:
            nowm = m["GATHER"]
            first_c = "G"
            s = { "name": 'GATHER', "isjoin": False, "issort": False, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": [] }
            pass
        elif m["SORT"] is not None:
            nowm = m["SORT"]
            first_c = 'S'
            s = { "name": 'SORT', "isjoin": False, "issort": True, "stime": float(nowm.group(1)), "etime": float(nowm.group(2)), "tuples": float(nowm.group(3)), "tables": [] }
            pass
        elif m["SORT-NO-EXE"] is not None:
            nowm = m["SORT-NO-EXE"]
            first_c = 'S'
            s = { "name": 'SORT', "isjoin": False, "issort": True, "stime": float(0), "etime": float(0), "tuples": float(nowm.group(2)), "tables": [] }
        elif m["PSEQ"] is not None:
            nowm = m["PSEQ"]
            first_c = 'P'
            s = { "name": 'PSEQ', "isjoin": False, "issort": False, "stime": float(nowm.group(2)), "etime": float(nowm.group(3)), "tuples": float(nowm.group(4)), "tables": [nowm.group(1) ]}
        elif m["SEQ"] is not None:
            nowm = m["SEQ"]
            first_c = 'S'
            s = { "name": 'SEQ', "isjoin": False, "issort": False, "stime": float(nowm.group(2)), "etime": float(nowm.group(3)), "tuples": float(nowm.group(4)), "tables": [nowm.group(1) ]}
            pass
        elif m["SEQ-NO-EXE"] is not None:
            nowm = m["SEQ-NO-EXE"]
            first_c = 'S'
            s = { "name": 'SEQ', "isjoin": False, "issort": False, "stime": float(0), "etime": float(0), "tuples": float(nowm.group(2)), "tables": [nowm.group(1) ]}
        else:
            # Otherwise. 
            match = False
            pass    
        # Common codes to deal with indent or so.
        if match:
            for i in range(0, len(l)):
                if l[i] == first_c:
                    l_indent = i
                    break
            print("first_c =", first_c)
            print("indent = ", l_indent)
            s["level"] = int((l_indent - ini_indent) / 6)
            statements.append(s)
    print("Show  statements:")
    print(statements)
    return statements

class Node:
    def __init__(self, s):
        self.s = s
        self.puresort = 0.
        self.purejoin = 0.
        self.purehash = 0.
        self.purescan = 0.
        self.puretuples = 0.
        self.purehashtuples = 0.
        self.purehashmem = 0.
        self.scost = 0.
        self.jcost = 0.
        self.hcost = 0.
        self.scancost = 0.
        self.tuples = 0.
        self.hashtuples = 0.
        self.hashmem = 0.
        self.children = []
    def totalcost(self):
        return self.scost + self.jcost + self.hcost + self.scancost
    def purecost(self):
        return self.puresort + self.purejoin + self.purehash + self.purescan
    def totaljcost(self):
        return self.jcost
    def totalscost(self):
        return self.scost
    def totalscancost(self):
        return self.scancost
    def totaltuples(self):
        return self.tuples


def process_statements(statements, l, h, level):
    # build the current node. 
    n = Node(statements[l])
    # locate children. 
    lno = []
    for i in range(l + 1, h):
        s = statements[i]
        if s['level'] == level + 1:
            lno.append(i)
    lno.append(h)
    for i in range(0, len(lno) - 1):
        n.children.append(process_statements(statements, lno[i], lno[i + 1], level + 1))
        n.s["tables"] += n.children[-1].s["tables"]
    if n.s['issort']:
        # only have one child
        n.puresort = n.s['etime'] - n.children[0].s['etime']
    elif n.s['isjoin']:
        n.purejoin = n.s['etime'] - n.children[0].s['etime'] - n.children[1].s['etime']
        n.puretuples = n.s['tuples']
        # try:
        #     n.purejoin = n.s['etime'] - n.children[0].s['etime'] - n.children[1].s['etime']
        #     n.puretuples = n.s['tuples']
        # except BaseException:
        #     print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        #     print("Current:")
        #     print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        #     printNode(n)
        #     print(n.children)
        #     for c in n.children:
        #         print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        #         print("Child:")
        #         print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        #         printNode(c)
        #     print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        #     print("Statements:")
        #     print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        #     for s in statements:
        #         print(s)
        #     sys.exit(-1)
    elif n.s['name'] == 'MAT':
        # Can be Materialize node. 
        n.purejoin = n.s['etime'] - n.children[0].s['etime']
    elif n.s['name'] == 'HASH':
        n.purehash = n.s['etime'] - n.children[0].s['etime']
        n.purehashtuples = n.s['tuples']
    if len(n.children) <= 0:
        # leaf node
        n.purescan = n.s['etime']
        n.puretuples = n.s['tuples']
        n.scost = 0.
        n.jcost = 0.
        n.scancost = n.purescan
        n.tuples = n.puretuples
    else:
        n.scost = n.puresort
        n.jcost = n.purejoin
        n.hcost = n.purehash
        n.scancost = n.purescan
        n.tuples = n.puretuples
        n.hashtuples = n.purehashtuples
        for c in n.children:
            n.scost += c.scost
            n.jcost += c.jcost
            n.hcost += c.hcost
            n.scancost += c.scancost
            n.tuples +=  c.tuples
            n.hashtuples += c.hashtuples
    return n

def printNode(r):
    print(" " * (2 * r.s["level"]), r.s)
    for c in r.children:
        printNode(c)

def get_color(ratio):
    ran = [ 0.01 * i for i in range(1, 100) ]
    for i in range(0, len(ran)):
        if ratio < ran[i]:
            return colors[i].hex_l
    return colors[-1].hex_l
    # if ratio < 0.01:
    #     # return "lemonchiffon1"
    #     return "\"#0d0f73\""
    # elif ratio < 0.1:
    #     return "lightgoldenrod"
    # elif ratio < 0.3:
    #     return "lightsalmon"
    # elif ratio < 0.5:
    #     return "indianred1"
    # else:
    #     return "indianred"

def printLabel(f, num, name, puresort, purejoin, purecost, tcost, tjcost, tscost, tscancost, rtcost, ttuples, tuples, tname = None):
    ratio = tcost / rtcost
    color = get_color(ratio)
    labels = [ name, '{:.2%}'.format(tcost / rtcost), '({:.2%})'.format(purecost / rtcost), "t: " + "{:.2f}s".format(tcost/1000.) + ",ts: " + "{:.2f}s".format(tscost / 1000.) + ",tj: " + "{:.2f}s".format(tjcost / 1000.) + ",scan: {:.2f}s".format(tscancost / 1000.), "(s: {:.2f}s".format(puresort / 1000.) + ",j: {:.2f}s)".format(purejoin/1000.), "total tuples: " + str(ttuples),  "(tuples: " + str(tuples) + ")"]
    f.write(str(num) + " [label=\"" + "\\n".join(labels))
    if tname is not None: f.write("\\n" + ",".join(tname))
    f.write("\",shape=rectangle,style=\"filled\",color=white,fontcolor=white,fontname=consoles,fillcolor=\"" + color+ "\"]\n")


def recDumpNode(f, r, pnum, num, d, rtcost):
    currnum = num
    if r.s["name"] == "SEQ" or r.s["name"] == "PSEQ":
        printLabel(f, currnum, r.s["name"], r.puresort, r.purejoin, r.purecost() , r.totalcost() , r.totaljcost(), r.totalscost(), r.totalscancost(), rtcost, r.totaltuples(), r.puretuples, r.s["tables"])
    else:
        printLabel(f, currnum, r.s["name"], r.puresort, r.purejoin , r.purecost(), r.totalcost() , r.totaljcost(), r.totalscost(), r.totalscancost(), rtcost, r.totaltuples(), r.puretuples)
    if pnum >= 0:
        f.write(str(pnum) + " -> " + str(currnum) + "\n")
    next_num = num + 1
    for c in r.children:
        next_num = recDumpNode(f, c, currnum,  next_num, d + 1, rtcost)
    return next_num



def dumpNode(r, qlabel):
    f = open(sys.argv[1] + "-" + qlabel + ".dot", "w")
    f.write("digraph {\n")
    recDumpNode(f, r, -1, 0, 0, r.totalcost())
    f.write("}\n")
    f.close()

def getBaseTableSort(r, p, d):
    if r.s["name"] == "SEQ" or r.s["name"] =="PSEQ":
        if p is not None and p.s["name"] == "SORT":
            d[",".join(r.s["tables"])]  = p.puresort
        else:
            d[",".join(r.s["tables"])]  = -1
    for c in r.children:
        getBaseTableSort(c, r, d)

    

def dumpBaseTableSort(r, fout):
    d = {}
    getBaseTableSort(r, None, d)
    keys = [ k for k in d.keys() ]
    keys.sort()
    fout.write("\n".join([ k + "," + str(d[k]) for k in keys ]) + "\n")

        
def dumpInvalidBaseTableSort(fout):
    for i in range(0, 6):
        fout.write("\n".join([ str(j) + "," + str(-1) for j in range(1, 7) ]) + "\n")



def printCost(r):
    print("Total tuples = ", r.tuples)
    print("Total hash tuples = ", r.hashtuples)
    print("Sort cost = ", r.scost / 1000., "s")
    print("Join cost = ", r.jcost / 1000., "s")
    print("Hash cost = ", r.hcost / 1000., "s")
    print("Scan cost = ", r.scancost / 1000., "s")
    print("Total cost = ", r.totalcost() / 1000., "s")

fres.write("scost,jcost,scancost,tuples\n")
start = 0
q = 1
for i in range(0, len(lines)):
    line = lines[i]
    m = pat["End"].match(line)
    m_err = pat["SpaceErr"].match(line)
    m_terr = pat["TimeOutErr"].match(line)
    m_bugerr = pat["BugErr"].match(line)
    
    if m is not None:
        print("Process Raw  lines...")
        for l in lines[start:i + 1]:
            print(l)
        statements = process_raw_lines(lines[start:i + 1])
        root = process_statements(statements, 0, len(statements), 0)
        printNode(root)
        printCost(root)
        dumpNode(root, "q" + str(q))
        dumpBaseTableSort(root, fbsort)
        # try:
        #     root = process_statements(statements, 0, len(statements), 0)
        # except BaseException:
        #     print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        #     print("lines:")
        #     print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        #     for l in lines[start:i + 1]:
        #         print(l)
        #     root = process_statements(statements, 0, len(statements), 0)
        #     print("statements")
        #     sys.exit(-1)
        fres.write(str(root.scost) + "," + str(root.jcost) + "," + str(root.scancost) + "," + str(root.tuples) + "\n")
        start = i
        q += 1
    elif m_err is not None:
        fres.write('-1,-1,-1,-1\n')
        dumpInvalidBaseTableSort(fbsort)
        # fres.write('-1,-1,-1,-1\n')
        start = i
        q += 1
    elif m_terr is not None:
        fres.write('3270000,327000,1,1\n')
        dumpInvalidBaseTableSort(fbsort)
        # fres.write('-1,-1,-1,-1\n')
        start = i
        q += 1
    elif m_bugerr is not None:
        fres.write('-1,-1,-1,-1\n')
        dumpInvalidBaseTableSort(fbsort)
        start = i
        q += 1

    # print("Query ",q," printed.")

        
fres.close()
fbsort.close()


# root = process_statements(statements, 0, len(statements), 0)
# print(root.scost, root.jcost, root.scancost, root.tuples)
# print(root.scost + root.jcost + root.scancost)