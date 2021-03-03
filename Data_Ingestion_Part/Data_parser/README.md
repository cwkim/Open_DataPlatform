# Pasring raw data files from ELEX / HANURI Tien

#### in UMC collection GW AWS 
1. Install  
Root authority required  
$ sudo sudo pip install pymongo  
$ sudo pip install watchdog

2. Run  
Root authority required 
* LOGDATA_DIRECTORY_PATH is default path "/svc/connected/drd/logData/"  
$ sudo nohup python -u f2db_elex_tcpdaemon.py <LOGDATA_DIRECTORY_PATH> &  
$ sudo tail -f nohup.out  

#### in MongoDB AWS 
Pasring docs of MongoDB from HANURI Tien  
1. Install  
Root authority required  
$ sudo sudo pip install pymongo  

2. Run  
$ sudo nohup python -u f2db_hanuri.py &  
$ sudo tail -f nohup.out  

#### in Monitoring Influx DB AWS
1. Install  
1) Install Influx DB  
2) Install Influx DB Client for Python  
3) Install Mongo DB Client for Python  

2. Run  
$ sudo nohup python -u mongo2influx.py &  
$ sudo tail -f nohup.out  
