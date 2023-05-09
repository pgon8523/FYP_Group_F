## To  use parseplan.py
```bash
# The following command will generate a dot file for each of the query.
python3 parseplan.py nngp_log_join_3.txt
# The following command  will  convert one of the dot file into eps  file.
dot -T eps nngp_log_join_3.txt-q30.dot -o q30.eps
# The following command will  convert the eps to pdf file.
epstopdf.pl q30.eps
```
