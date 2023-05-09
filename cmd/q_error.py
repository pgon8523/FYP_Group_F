import csv
import re
import json
import matplotlib.pyplot as plt
import numpy as np

def find_estimated_and_actual_cardinality(s):
    substring = "rows"
    index1 = s.find("rows")
    index2 = s.find("rows", index1 + 1)
    # +5 means that finding the number of rows in rows='number'
    indices = [index1+5, index2+5]
    return indices

def get_estimated_actual_cardinality(num_of_join):
    psql_estimated_card = []
    psql_actual_card = []
    nngp_estimated_card = []
    nngp_actual_card = []

    with open("/home/fypgf/gui/web/cmd/nngp_log_join_"+ str(num_of_join) + ".txt", "r") as f:
        nngp_file = f.read()
    with open("/home/fypgf/gui/web/cmd/log_join_"+ str(num_of_join) + ".txt", "r") as f:
        psql_file = f.read()

    # PostgreSQL
    psql_results = psql_file.split("QUERY PLAN")
    psql_results = psql_results[1:]
    for ele in psql_results:
        indices = find_estimated_and_actual_cardinality(ele)
        psql_estimated_card.append(ele[indices[0]:ele.find("width")-1])
        psql_actual_card.append(ele[indices[1]:ele.find("loops")-1])

    # NNGP 
    nngp_results = nngp_file.split("QUERY PLAN")
    nngp_results = nngp_results[1:]
    for ele in nngp_results:
        indices_nngp = find_estimated_and_actual_cardinality(ele)
        nngp_estimated_card.append(ele[indices_nngp[0]:ele.find("width")-1])
        nngp_actual_card.append(ele[indices_nngp[1]:ele.find("loops")-1])

    return psql_estimated_card, psql_actual_card, nngp_estimated_card, nngp_actual_card

def calculate_q_error(estimated_cardinality, actual_cardinality):
        # Calculate q-error using: q-error = abs(actual - estimated) / estimated
    q_error = []

    
    for x in range(len(estimated_cardinality)):
        if (int(estimated_cardinality[x]) == 0):
            print("estimated_cardinality is 0, check the log file. Estimated Cardinality changed to 1")
            estimated_cardinality[x] = 1

        if (int(actual_cardinality[x]) == 0):
            print("actual_cardinality is 0, check the log file. Actual Cardinality changed to 1")
            actual_cardinality[x] = 1   
                                        
        if abs(int(estimated_cardinality[x]) - int(actual_cardinality[x])) == 0:
            q_error.append(0)
        else:
            temp1 = max(int(estimated_cardinality[x])/(int(actual_cardinality[x])), int(actual_cardinality[x])/(int(estimated_cardinality[x])))
            q_error.append(temp1)
    return q_error

def plot_graph(psql_q_error, nngp_q_error):
    query = []
    for x in range(len(psql_q_error)):
        query.append("q"+str(x+1))
    ticks = np.arange(1, len(psql_q_error)+1)

    # plot graph for q-error in bar chart
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
    ax1.bar(ticks+0.2, psql_q_error, 0.2, label = "PostgreSQL")
    ax1.bar(ticks-0.2, nngp_q_error, 0.2, label = "NNGP")
    ax1.set_title("Q-error in bar chart")
    ax1.set_xticks(ticks)
    ax1.set_xlabel('Query')
    ax1.set_ylabel("Q-error")
    ax1.legend()

    # plot graph for q-error in box plot
    ax2.boxplot([nngp_q_error, psql_q_error], positions=[1, 2])
    ax2.set_xticklabels(['NNGP', 'PostgreSQL'])
    ax2.set_title("Q-error in box plot")
    ax2.set_ylabel("Q-error")

    # Save as pngfile
    plt.savefig("/home/fypgf/gui/web/express/app/views/Q_error_between_PostgreSQL_and_NNGP.png")

# Get No. of join in test queries
try:
    with open("/home/fypgf/gui/web/cmd/error.json") as f:
        data = json.load(f)
except:
    print("There is no error.json file")

num_of_join = data["selectQuery"]
psql_estimated_card, psql_actual_card, nngp_estimated_card, nngp_actual_card  = get_estimated_actual_cardinality(num_of_join)
psql_q_error = calculate_q_error(psql_estimated_card, psql_actual_card)
nngp_q_error = calculate_q_error(nngp_estimated_card, nngp_actual_card)
plot_graph(psql_q_error, nngp_q_error)

## for debug ##
# print("num of join that user selected: ", num_of_join)
# print("psql est card: ", psql_estimated_card)
# print("psql act card: ", psql_actual_card)
# print("nngp est card: ", nngp_estimated_card)
# print("nngp act card: ", nngp_actual_card)
# print("psql q error: ", psql_q_error)
# print("nngp q error: ", nngp_q_error)

















