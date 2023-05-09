import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import random
import sys
import json
import os
import pandas as pd

my_path = os.path.abspath(__file__)

with open('/home/fypgf/gui/web/cmd/testing.json') as f:
        data = json.load(f)
planning = data['plan']
execution = data['execution']
nngp_planning = data['nngp_plan']
nngp_execution = data['nngp_execution']
query = []

for i in range(0,len(planning)):
    planning[i] = float(planning[i])
    execution[i] = float(execution[i])
    nngp_planning[i] = float(nngp_planning[i])
    nngp_execution[i] = float(nngp_execution[i])
    query.append("q"+str(i+1))

plan_ratio = []
exec_ratio = []
for i in range(0,len(planning)):
    plan_ratio.append(nngp_planning[i]/planning[i])
    exec_ratio.append(nngp_execution[i]/execution[i])

dict_plan = {'query':query,'ratio':plan_ratio,'nngp_planning':nngp_planning,'planning':planning}
dict_exec = {'query':query,'ratio':exec_ratio,'nngp_execution':nngp_execution,'execution':execution}

##PLAN AND EXEC INTO DATAFRAME##
plan = pd.DataFrame(dict_plan)
plan = plan.sort_values(by='ratio',ascending = False)
exec = pd.DataFrame(dict_exec)
exec = exec.sort_values(by='ratio',ascending = False)
##HPLAN AND LPLAN##
h_plan_ratio = plan.tail(10)
l_plan_ratio = plan.head(10)
h_plan_ratio = h_plan_ratio.sort_values(by='ratio',ascending = True)
##HEXEC AND LEXEC## 
h_exec_ratio = exec.tail(10)
l_exec_ratio = exec.head(10)
h_exec_ratio = h_exec_ratio.sort_values(by='ratio',ascending = True)
##PLAN INTO LIST##
h_nngp_plan = h_plan_ratio["nngp_planning"].values.tolist()
h_plan = h_plan_ratio["planning"].values.tolist()
l_nngp_plan = l_plan_ratio["nngp_planning"].values.tolist()
l_plan = l_plan_ratio["planning"].values.tolist()
##EXEC INTO LIST##
h_nngp_exec = h_exec_ratio["nngp_execution"].values.tolist()
h_exec = h_exec_ratio["execution"].values.tolist()
l_nngp_exec = l_exec_ratio["nngp_execution"].values.tolist()
l_exec = l_exec_ratio["execution"].values.tolist()

def diff(x,y):
    diff = x - y
    return str(round((diff/x)*100,2)) + "%"

##PLOT GRAPH FOR PLANNING TIME (HIGH)##
h_plan_ratio.plot(x="query", y=["planning", "nngp_planning"], kind="bar", figsize=(20,5))
plt.xlabel('Queries',fontsize=12)
plt.ylabel('Time Taken (ms)',fontsize=12)
plt.xticks(rotation=0)
plt.title("Queries with highest planning time ratio (PostgreSQL/NNGP)",fontsize=15)
plt.legend(["PostgreSQL","NNGP"])
axes = plt.gca()
y_min, y_max = axes.get_ylim()
for i in range(len(h_plan_ratio)):
    plt.text(i, y_max*2/3, diff(h_plan[i],h_nngp_plan[i]), ha = 'center',fontsize=14)
plt.savefig("/home/fypgf/gui/web/express/app/views/Planning_Time_Comp_High.png")


##PLOT GRAPH FOR PLANNING TIME (LOW)##
l_plan_ratio.plot(x="query", y=["planning", "nngp_planning"], kind="bar", figsize=(20,5))
plt.xlabel('Queries',fontsize=12)
plt.ylabel('Time Taken (ms)',fontsize=12)
plt.xticks(rotation=0)
plt.title("Queries with lowest planning time ratio (PostgreSQL/NNGP)",fontsize=15)
plt.legend(["PostgreSQL","NNGP"])
axes = plt.gca()
y_min, y_max = axes.get_ylim()
for i in range(len(l_plan_ratio)):
    plt.text(i, y_max*2/3, diff(l_plan[i],l_nngp_plan[i]), ha = 'center',fontsize=14)
plt.savefig("/home/fypgf/gui/web/express/app/views/Planning_Time_Comp_Low.png")


##PLOT GRAPH FOR EXECUTION TIME (HIGH)##
h_exec_ratio.plot(x="query", y=["execution", "nngp_execution"], kind="bar", figsize=(20,5))
plt.xlabel('Queries',fontsize=12)
plt.ylabel('Time Taken (ms)',fontsize=12)
plt.xticks(rotation=0)
plt.title("Queries with highest execution time ratio (PostgreSQL/NNGP)",fontsize=15)
plt.legend(["PostgreSQL","NNGP"])
axes = plt.gca()
y_min, y_max = axes.get_ylim()
for i in range(len(h_exec_ratio)):
    plt.text(i, y_max*2/3, diff(h_exec[i],h_nngp_exec[i]), ha = 'center',fontsize=14)
plt.savefig("/home/fypgf/gui/web/express/app/views/Execution_Time_Comp_High.png")


##PLOT GRAPH FOR EXECUTION TIME (LOW)##
l_exec_ratio.plot(x="query", y=["execution", "nngp_execution"], kind="bar", figsize=(20,5))
plt.xlabel('Queries',fontsize=12)
plt.ylabel('Time Taken (ms)',fontsize=12)
plt.xticks(rotation=0)
plt.title("Queries with lowest execution time ratio (PostgreSQL/NNGP)",fontsize=15)
plt.legend(["PostgreSQL","NNGP"])
axes = plt.gca()
y_min, y_max = axes.get_ylim()
for i in range(len(l_exec_ratio)):
    plt.text(i, y_max*2/3, diff(l_exec[i],l_nngp_exec[i]), ha = 'center',fontsize=14)
plt.savefig("/home/fypgf/gui/web/express/app/views/Execution_Time_Comp_Low.png")


##PLOT GRAPH FOR PLANNING TIME COMPARISON##
plt.figure(figsize=(6,6))
count_red = 0
count_blue = 0
plt.axline([0, 0], slope=1,color='red')
for i in range(0,len(planning)):
    if planning[i]>nngp_planning[i]:
        plt.scatter(planning[i], nngp_planning[i],color='orange')
        count_red += 1
    else:
        plt.scatter(planning[i], nngp_planning[i],color='royalblue')
        count_blue += 1
axes = plt.gca()
x_min, x_max = axes.get_xlim()
y_min, y_max = axes.get_ylim()
if x_max > y_max:
    plt.ylim(0, x_max)
else:
    plt.xlim(0, y_max)
plt.xlabel('PostgreSQL (ms)')
plt.ylabel('NNGP (ms)')
plt.text(y_max+y_max/2, y_max/2, "Blue: Queries with faster planning time in PostgreSQL\nOrange: Queries with faster planning time in NNGP\nRed line indicates similar performance\nNo. of Blue Queries: {}\nNo. of Orange Queries: {}".format(count_blue,count_red), ha = 'center',fontsize=10)
plt.title("Planning Time Comparison",fontsize=13)
plt.savefig('/home/fypgf/gui/web/express/app/views/Planning_Time_Comparison.png',bbox_inches='tight')


##PLOT GRAPH FOR EXECUTION TIME COMPARSION##
plt.figure(figsize=(6,6))
count_blue = 0
count_red = 0
plt.axline([0, 0], slope=1,color='red')
for i in range(0,len(execution)):
    if execution[i]>nngp_execution[i]:
        plt.scatter(execution[i], nngp_execution[i],color='orange')
        count_red += 1
    else:
        plt.scatter(execution[i], nngp_execution[i],color='royalblue')
        count_blue += 1
axes = plt.gca()
x_min, x_max = axes.get_xlim()
y_min, y_max = axes.get_ylim()
if x_max > y_max:
    plt.ylim(0, x_max)
else:
    plt.xlim(0, y_max)
plt.xlabel('PostgreSQL (ms)')
plt.ylabel('NNGP (ms)')
plt.text(y_max+y_max/2, y_max/2, "Blue: Queries with faster execution time in PostgreSQL\nOrange: Queries with faster execution time in NNGP\nRed line indicates similar performance\nNo. of Blue Queries: {}\nNo. of Orange Queries: {}".format(count_blue,count_red), ha = 'center',fontsize=10)
plt.title("Execution Time Comparison",fontsize=13)
plt.savefig("/home/fypgf/gui/web/express/app/views/Execution_Time_Comparison.png",bbox_inches='tight')



##COMPUTING AVERAGE TIME COMPARISON##
avg_planning = sum(planning)/len(planning)
avg_planning_nngp = sum(nngp_planning)/len(nngp_planning)
avg_execution = sum(execution)/len(execution)
avg__nngp_execution = sum(nngp_execution)/len(nngp_execution)
planning_comp = []
planning_comp.append(avg_planning)
planning_comp.append(avg_planning_nngp)
execution_comp = []
execution_comp.append(avg_execution)
execution_comp.append(avg__nngp_execution)
model = ['PostgreSQL','NNGP']


#print(avg_planning)
#print(avg_planning_nngp)
###FUNCTION FOR TEXT###
def diff(x,y):
    diff = x-y
    if diff < 0:
        diff = y-x
        return str(round((diff/x)*100,2))+"%"+"  slower\nthan before"
    else:
        return str(round((diff/x)*100,2))+"%"+"  faster\nthan before"


##PLOT GRAPH FOR AVG COMPARISON##
plt.figure(figsize=(20,6))
plt.subplot(1,2,1)
plt.bar(model, planning_comp, label = "PostgreSQL",color=['royalblue','orange'])
plt.ylabel('Time Taken (ms)',fontsize=12)
plt.title("Average Planning Time Comparison",fontsize=20)
for i in range(len(model)):
    y = planning_comp[i]/2
    plt.text(i, y, str(round(planning_comp[i],4))+"ms", ha = 'center',fontsize=15)
axes = plt.gca()
x_min, x_max = axes.get_xlim()
y_min, y_max = axes.get_ylim()
plt.text(x_max,y_max-(y_max/10), diff(planning_comp[0],planning_comp[1]), ha = 'center',fontsize=15,color='red')


plt.subplot(1,2,2)
plt.bar(model, execution_comp, label = "NNGP",color=['royalblue','orange'])
plt.ylabel('Time Taken (ms)',fontsize=12)
plt.title("Average Execution Time Comparison",fontsize=20)
for i in range(len(model)):
    y = execution_comp[i]/2
    plt.text(i, y, str(round(execution_comp[i],4))+"ms", ha = 'center',fontsize=15)
axes = plt.gca()
x_min, x_max = axes.get_xlim()
y_min, y_max = axes.get_ylim()
plt.text(x_max,y_max-(y_max/10), diff(execution_comp[0],execution_comp[1]), ha = 'center',fontsize=15,color='red')
# plt.savefig("/home/fypgf/gui/web/cmd/AVG_Comparison.png")
plt.savefig("/home/fypgf/gui/web/express/app/views/AVG_Comparison.png")

f.close()
