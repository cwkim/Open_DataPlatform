#-*- coding: utf-8 -*-
""" 
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    f2db_elex_tcpdaemon.py: Store ELEX and UMC log file of TCP daemon server to collection DB 
"""
from pymongo import MongoClient, errors
import pprint
import sys, os, base64
import logging, time
from logging import handlers
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import PatternMatchingEventHandler
from watchdog.events import LoggingEventHandler
from watchdog.utils.dirsnapshot import DirectorySnapshotDiff
from datetime import timedelta, datetime
from pytz import timezone

KST = timezone('Asia/Seoul')

# DBs Info.
REPLSET_LIST = ["", ""]
REPLSET_NAME = ""
USERNAME = ""
PASSWORD = ""

# aws_mongodb_replset_connect: AWS MongoDB access
def aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME, username=USERNAME, password=PASSWORD):
    try:
        client = MongoClient(replset_list, replicaSet=replset_name, username=username, password=password, connect=False)
        db = client.elex
        return db
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to Mongo DB: %s", str(err))

# mongodb_connect: Remote access to collection MongoDB
def mongodb_connect(id, pw, ip, port):
    try:
        # Access to Collection MongoDB (AWS instance)
        server_config = "mongodb://" + id + ':' + pw + '@' + ip + ':' + port + '/'
        client = MongoClient(server_config)
        db = client.elex
        return db
    except Exception as err:
        logging.error("Failed to connect to mongoDB server {}".format(err))

# raw2dict: Converting lines of read logfile to dictionary of list
def raw2dict(rawdata):
    # Predefined fields: 46 items (common_data_template_20181203.excel)
    B2B_fields = {'REQ_TIME':str,'RECORD_TIME':str,'ORIGIN_TYPE':int,'VEHICLE_TYPE':int,'PHONE_NUM':str, \
                  'DRIVE_STATUS':str,'DRIVE_LENGTH_DAY':int,'DRIVE_LENGTH_TOTAL':int,'DRIVE_SPEED':int, \
                  'RPM':int,'BREAK_SIGNAL':str,'GPS_LAT':float,'GPS_LONG':float, \
                  'GPS_ANGLE':float,'ACCEL_X':float,'ACCEL_Y':float,'DEVICE_STATUS_CD':int, \
                  'FUEL_CONSUM_DAY':int,'FUEL_CONSUM_TOTAL':int, \
                  'BATTERY_VOLTAGE':float,'COOLANT_TEMPER':int,'ENGINE_OIL_TEMPER':int, \
                  'INTAKE_TEMPER':int,'AMBIENT_TEMPER':int,'ACCEL_PEDAL':float, \
                  'TEMPER1':int,'TEMPER2':int,'CHANGE_LEVER':str,'CHANGE_GEAR_STEPS':int,'MAF':int, \
                  'AMP':float,'ENGINE_TORQUE':float,'AIR_GAUGE':float,'ENGINE_GAUGE':float,'DTC':str, \
                  'VEHICLE_NUM':str, 'DEVICE_NUM':str, \
                  'ADDRESS':str,'GPS_STATUS':str, 'GPS_DT':str,'GPS_TIME':str,'GPS_LENGTH':int
                  # 'DTC_2':str,'DTC_3':str,'DTC_4':str,'DTC_5':str,
    }
    docs = list()
    for line in rawdata:
        tmp_doc = dict()
        dics = line.split('|')
        if '\n' in dics: dics.remove('\n')
        for dic in dics:
            try:
                key, value = dic.split('=')[:2]
            except ValueError as ve:
                logging.error("[Exception] value error: dict({0}) for {1}".format(str(dic), str(ve)))
                continue
            key = key.upper()
            if key in B2B_fields.keys():
                try:
                    if not ((len(value) == 0) or value.isspace()):
                        if (key == 'REQ_TIME') or (key == 'RECORD_TIME'):
                            if 'T' in str(value):
                                tmp = str(value).replace('-', '').split('T')
                                tmp_doc[key] = KST.localize(datetime.strptime(tmp[0]+tmp[1].replace(':','').split('.')[0], "%Y%m%d%H%M%S"))
                            else:
                                tmp_doc[key] = KST.localize(datetime.strptime('20'+str(value)[:12], "%Y%m%d%H%M%S"))
                        elif (key == 'GPS_LONG') or (key == 'GPS_LAT'):
                            value = float(value)/1000000.0
                            tmp_doc[key] = B2B_fields[key](value)
                        else:
                            if key == 'BREAK_SIGNAL':
                                tmp_doc['BREAK'] = B2B_fields[key](value)
                            else:
                                tmp_doc[key] = B2B_fields[key](value)    
                except UnicodeEncodeError as uee:
                    if isinstance(value, unicode):
                        tmp_doc[key] = str(value.encode('utf8'))
                except ValueError as ve:
                    if ve.message == "invalid literal for int() with base 10: 'null'":
                        tmp_doc[key] = None
                except Exception as e:
                    logging.error("[Exception] Parsing error for key(%s) and value(%s) due to (%s)", str(key), str(value), str(e))
                    pass
        docs.append(tmp_doc)
    return docs

# insertdb: Inserting documents to monthly  collection of 'elex' database
def insertdb(coll_name, docs):
    #db = mongodb_connect(id='root', pw='keti1234', ip='13.125.67.198', port='27017')
    tmp = 0
    while tmp < 100:
        try:
            tmp += 1
            # Access to AWS MongoDB (Replication Set)
            db = aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME, username=USERNAME, password=PASSWORD)
            results = db[coll_name].insert_many(docs)
            #logging.info("inserted {} ... ".format(str(results.inserted_ids)))
            break
        except Exception as err:
            logging.error("insertdb error: {}".format(str(err)))
            time.sleep(10)

# file2db: Storing file data to collection MongoDB if the file extension is log
def file2db(dirpath, filename, fp):
    if os.path.splitext(filename)[-1] == '.log':
        # To store raw data per month
        coll_name = dirpath[27:31]+dirpath[32:34] # year+month: ex) 201905
        full_filepath = os.path.join(dirpath, filename)
        with open(full_filepath, 'r') as log_fp:
            lines = log_fp.readlines()
            docs = raw2dict(lines)
            insertdb(coll_name=coll_name, docs=docs)
        fp.write(full_filepath)
        fp.write('|')
        logging.info("Inserted {} ...".format(str(filename)))
 
class Watcher():
    def __init__(self):
        self.observer = Observer()
    def run(self, monitoring_path):
        event_handler = Handler()
        self.observer.schedule(event_handler, monitoring_path, recursive=True)
        self.observer.start()
        try:
            event_handler.scanning(logfile_path=monitoring_path)
            #while True:
            #    time.sleep(10) # iteration period: 10sec
        except KeyboardInterrupt, e:
            self.observer.stop()
            logging.warning("Terminated by User") 
        self.observer.join()
    
class Handler(FileSystemEventHandler):
    # on_created: Storing a flie if the file is not in the file list of historyfile
    def on_created(self, event):
        if (event.event_type == 'created') and (event.is_directory == False):
            time.sleep(610) # Wait until the log file is fully stored
            with open(TCP_HISTORYFILE_PATH, 'a+t') as fp:
                exist_list = fp.readline().split('|')
                if not event.key[1] in exist_list:
                    file_dir = event.key[1].split('/M')[0]
                    file_name = event.key[1].split('/')[-1]
                    file2db(dirpath=file_dir, filename=file_name, fp=fp)

    # scanning: Scannning for missing files when process runs
    def scanning(self, logfile_path):
        tmp1 = []
        workDir = os.path.abspath(logfile_path)
        for dirpath, dirnames, filenames in os.walk(workDir):
            for filename in filenames:
                tmp1.append(os.path.join(dirpath, filename))
        with open(TCP_HISTORYFILE_PATH, 'a+t') as fp:
            tmp2 = fp.readline().split('|')
            tmp_list = list(set(tmp1) - set(tmp2))
            if len(tmp_list) != 0:
                tmp_list.sort()
                for full_filepath in tmp_list:
                    file_dir = full_filepath.split('/M')[0]
                    file_name = full_filepath.split('/')[-1]
                    file2db(dirpath=file_dir, filename=file_name, fp=fp)

# monitoring: Checking new files created periodically 
def monitoring(logfile_path):
    watch = Watcher()
    watch.run(monitoring_path=logfile_path)

if __name__ == '__main__':
    logging.basicConfig(filename='f2db_elex_tcpdaemon.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    LOG_MAX_BYTES = 10*1024*1024    # 10 MB
    handlers.RotatingFileHandler(filename='f2db_elex_tcpdaemon.log', maxBytes=LOG_MAX_BYTES, backupCount=5)

    TCP_LOGFILE_PATH = '/svc/connected/drd/logData/'
    TCP_HISTORYFILE_PATH = './stored_tcp_filelist.txt'
    root_path = sys.argv[1] if len(sys.argv) > 1 else TCP_LOGFILE_PATH
    with open(TCP_HISTORYFILE_PATH, 'a+t') as fp:
        if len(fp.readline()) == 0:
            # Pasing MDT log files in ELEX/UMC logData folder of TCP deamon server
            try:
                workDir = os.path.abspath(root_path)
                for dirpath, dirnames, filenames in os.walk(workDir):
                    filenames.sort()
                    # Each file is stored in Mongo DB
                    for filename in filenames:
                        file2db(dirpath=dirpath, filename=filename, fp=fp)
            except IOError:
                logging.error("No such file: %s" % workDir)
    monitoring(logfile_path=root_path)

