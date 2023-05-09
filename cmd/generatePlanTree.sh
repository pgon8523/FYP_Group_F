# rm  /home/fypgf/gui/web/cmd/*.txt-*.dot
python3 /home/fypgf/gui/web/cmd/parseplan.py $1
dot -T eps $2 -o $3
epstopdf.pl $3 
cp $4 > /home/fypgf/gui/web/express/app/views

