import random
import json
import os


with open('/home/fypgf/gui/web/cmd/train_test_ratio/train_test_information.json') as f:
    data = json.load(f)

# Get relation name for selection of train and test datasets
relation_name = data["relation_name"][0]
if str(relation_name) == 'IMDB':

    # Data path to 'imdb'
    data_path_nngp = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_training_backup/"
    data_path_sql = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_sql_backup/"
    data_centric_for_nngp = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training/"
    
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training/join_query_1.txt")
    except:
        print("No join_query_1.txt to be deleted")
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training/join_query_2.txt")
    except:
        print("No join_query_2.txt to be deleted")
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training/join_query_3.txt")
    except:
        print("No join_query_3.txt to be deleted")
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training/join_query_4.txt")
    except: 
        print("No join_query_4.txt to be deleted") 
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training/join_query_5.txt")
    except: 
        print("No join_query_5.txt to be deleted")     

elif str(relation_name) == 'TPCDS':

    data_path_nngp = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/train_data_for_nngp_backup/"
    data_path_sql = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/sql/"
    data_centric_for_nngp = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/join_store_sales_store_item_customer_promotion_10_data_centric_427/"
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/join_store_sales_store_item_customer_promotion_10_data_centric_427/join_query_1.txt")
    except:
        print("No join_query_1.txt to be deleted")
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/join_store_sales_store_item_customer_promotion_10_data_centric_427/join_query_2.txt")
    except:
        print("No join_query_2.txt to be deleted")
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/join_store_sales_store_item_customer_promotion_10_data_centric_427/join_query_3.txt")
    except:
        print("No join_query_3.txt to be deleted")
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/join_store_sales_store_item_customer_promotion_10_data_centric_427/join_query_4.txt")
    except:
        print("No join_query_4.txt to be deleted")
    try:
        os.remove("/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/join_store_sales_store_item_customer_promotion_10_data_centric_427/join_query_5.txt")
    except:
        print("No join_query_5.txt to be deleted")      



# Get minimum and maximum no. of joins 
min_of_joins = data["min_max"]["min"][0]
max_of_joins = data["min_max"]["max"][0]

# Select which datasets will be used
datasets_nngp = []
datasets_sql = []
for x in range(int(min_of_joins), int(max_of_joins)+1):
    datasets_nngp.append("join_query_" + str(x) + ".txt")
    datasets_sql.append("explain_join_query_" + str(x) + ".txt.sql")

# Get join and non join ratio
join_ratio = data["non_join_query_ratio"]["join"][0]
non_join_ratio = data["non_join_query_ratio"]["non_join"][0]

# If non_join_ratio == 0, there are only joined queries (drop "join_query_1.txt" if exists) 
if float(non_join_ratio) == 0.0:
    if "join_query_1.txt" in datasets_nngp:
        datasets_nngp.remove("join_query_1.txt")
        datasets_sql.remove("explain_join_query_1.txt.sql")

# Get train and test ratio
train_ratio = data["train_test_ratio"]["train"][0]
test_ratio = data["train_test_ratio"]["test"][0]

print("----------")
print("relation_name: ", relation_name)
print("min_of_joins: ", min_of_joins)
print("max_of_joins: ", max_of_joins)
print("join_ratio: ", join_ratio)
print("non_join_ratio: ", non_join_ratio)
print("train_ratio: ", train_ratio)
print("test_ratio: ", test_ratio)
print("----------")

# Calculating no. of non join and join queries
total_join_queries_per_files = 0
total_no_join_queries_per_files = 0

if (float(non_join_ratio) != 0.0):
    if min_of_joins == 1:
        print("Non Join ratio is not 0")
        if ((3000*(len(datasets_nngp)-1))/3000) <= (float(join_ratio)/float(non_join_ratio)):
            total_join_queries_per_files = 3000
            total_no_join_queries_per_files = int(total_join_queries_per_files*(len(datasets_nngp)-1)*float(non_join_ratio)/float(join_ratio))
            print("Total join queries: ", total_join_queries_per_files*(len(datasets_nngp)-1))
            print("Total non join queries", total_no_join_queries_per_files)
        else:
            total_no_join_queries_per_files = 3000
            total_join_queries_per_files = int(int(total_no_join_queries_per_files*float(join_ratio)/float(non_join_ratio))/(len(datasets_nngp)-1))
            print("Total join queries: ", total_join_queries_per_files*(len(datasets_nngp)-1))
            print("Total non join queries", total_no_join_queries_per_files)

# Load each datasets(query) and save it into train and test files (NNGP)
for dataset in datasets_nngp:
    ### use data_path + dataset ### 
    with open(data_path_nngp + dataset, 'r', newline='') as f:
        query_data_nngp = f.readlines()
        if not query_data_nngp[-1].endswith('\n'):
            query_data_nngp[-1] += '\n'

        # Shuffle data randomly
        random.Random(5).shuffle(query_data_nngp)

        ##############################
        try:
            if min_of_joins == 1:
                if dataset == "join_query_1.txt" :
                    query_data_nngp = query_data_nngp[:total_no_join_queries_per_files]
                else:
                    query_data_nngp = query_data_nngp[:total_join_queries_per_files]
        except:
            print("Error")    
        ############################## 	

        # Determine percentage split
        split_percent = float(train_ratio)
        split_index = int(len(query_data_nngp) * split_percent)

        # Split the data into training and testing sets
        train_data_nngp = query_data_nngp[:split_index]
        test_data_nngp = query_data_nngp[split_index:]

        # Write out the split data to separate files
        train_file_nngp = 'join_query_{}.txt'.format(dataset.split('_')[2][0])
        test_file_nngp = 'test_nngp_join_query_{}.txt'.format(dataset.split('_')[2][0])

        print("No. of lines in " + dataset + ": " + str(len(query_data_nngp)))
        print("No. of lines in " + "train_data_nngp" + ": " + str(len(train_data_nngp)))
        print("No. of lines in " + "test_data_nngp" + ": " + str(len(test_data_nngp)))
        print("")

        ### Use "data_path_nngp + train_file_nngp" to save files ###
        with open(data_centric_for_nngp + train_file_nngp, 'w') as f:
            f.writelines(train_data_nngp)

        ### Use "data_path_nngp + test_file_nngp" to save files
        # with open('/home/fypgf/gui/web/cmd/train_test_ratio/test_for_saving_train_test/' + test_file_nngp, 'w') as f:
        #     f.writelines(test_data_nngp)
        
# --------------------------------------------------------------------- #        
# Load each datasets(query) and save it into train and test files (SQL)
for dataset in datasets_sql:
    
    ### Must change data path before implement in the server ###
    ### use data_path + dataset ### 
    with open(data_path_sql + dataset, 'r', newline='') as f:
        query_data_sql = f.readlines()
        if not query_data_sql[-1].endswith('\n'):
            query_data_sql[-1] += '\n'

        # Shuffle data randomly
        random.Random(5).shuffle(query_data_sql)

        ##############################
        try:
            if min_of_joins == 1:
                    
                if dataset == "join_query_1.txt" :
                    query_data_sql = query_data_sql[:total_no_join_queries_per_files]
                else:
                    query_data_sql = query_data_sql[:total_join_queries_per_files]
        except:
            print("Error")    
        ############################## 

        # Determine percentage split
        split_percent = float(train_ratio)
        split_index = int(len(query_data_sql) * split_percent)

        # Split the data into training and testing sets
        train_data_sql = query_data_sql[:split_index]
        test_data_sql = query_data_sql[split_index:]

        # Write out the split data to separate files
        train_file_sql = 'train_explain_join_query_{}.txt.sql'.format(dataset.split('_')[3][0])
        test_file_sql = 'explain_join_query_{}.txt.sql'.format(dataset.split('_')[3][0])

        print("No. of lines in " + dataset + ": " + str(len(query_data_sql)))
        print("No. of lines in " + "train_data_sql" + ": " + str(len(train_data_sql)))
        print("No. of lines in " + "test_data_sql" + ": " + str(len(test_data_sql)))
        print("")

        ### Use "data_path_sql + test_file_sql" to save files
        # with open('/home/fypgf/gui/web/cmd/train_test_ratio/test_for_saving_train_test/' + train_file_sql, 'w') as f:
        #     f.writelines(train_data_sql)

        ### Use "data_path_nngp + test_file_nngp" to save files
        with open('/home/fypgf/gui/web/cmd/train_test_ratio/test_for_saving_train_test/' + test_file_sql, 'w') as f:
            f.writelines(test_data_sql)

print("Succefully generated train and test queries")
# Have to implement those train files into data centric folder.

