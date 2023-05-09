import sys
f = open(sys.argv[1], "r")
fo = open("explain_" + sys.argv[1], "w")
lines = f.readlines()
# add explain analyze before each sql command so that model can be run properly
for l in lines:
    fo.write("explain analyze " + l)
f.close()
fo.close()