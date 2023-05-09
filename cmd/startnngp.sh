# sed -i "s/#**shared_preload_libraries=pg_nngp/shared_preload_libraries=pg_nngp/g" /home/fypgf/pgdata/postgresql.conf 
sed -i "/shared_preload_libraries=pg_nngp/d" /home/fypgf/pgdata/postgresql.conf
echo "shared_preload_libraries=pg_nngp" >> /home/fypgf/pgdata/postgresql.conf 
pg_ctl restart > ~/.tmp
