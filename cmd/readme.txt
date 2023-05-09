 - plot_graph.py : plots the graph based on the planning time, execution time results. Retrieve the data in JSON format. Plots grah using matplotlib.pyplot and pandas.
 - q_error.py : This file calculate q error and generate graphs into two types.
    		1. Bar chart of all tested queries
    		2. Box plot of all tested queries
		It uses the log file of PostgreSQL and NNGP model. 
		The server generate log files and store the No. of join in test queries. 
		This is because, the user fix the No. of join to be tested and the specific number will be the result files.
		For example, if the user select No. of join in GUI as 2, the log_join_2.txt and nngp_log_join_2.txt will be results files.
		This file track the '2' and collect the q error from it. This number will be stored in error.json file.
