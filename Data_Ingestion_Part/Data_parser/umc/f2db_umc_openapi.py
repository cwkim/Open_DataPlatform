#-*- coding: utf-8 -*-
""" 
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    f2db_umc_openapi.py: Store UMC log file of Open API server to collection DB 
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

# raw2dict: Converting lines of read logfile to dictionary of list
def raw2dict(rawdata, B2C_type):
    # Predefined fields: OBD 11 items, WBD 15 items (common_data_template_20181203.excel)
    OBD_fields = {'REQ_TIME':str,'RECORD_TIME':str,'ORIGIN_TYPE':int, \
                  'PHONE_NUM':str,'DEVICE_NUM':str,'VIN':str,'RPM':float,'VSPEED':int, \
                  'ECP':int,'TCODE':str,'RAW_DATA':str, \
                  'FUEL_BAFFLE':bool,'FUEL_RAIL':float,'EGR':bool,'HYBRID_BATTERY':bool,'EOP':int,'INECTION_TIMING':int,'DPFP':int
    }
    WBD_fields = {'REQ_TIME':str,'RECORD_TIME':str,'ORIGIN_TYPE':int, \
                  'PHONE_NUM':str,'DEVICE_NUM':str,'PRG':int, \
                  'ALTTIUDE':int,'TOTAL_STEP':int, \
                  'TOTAL_DISTANCE':int,'TOTAL_CALORIES':int, \
                  'TOTAL_SLEEP_TIME':int,'TOTAL_DEEP_SLEEP_TIME':int, \
                  'TOTAL_LIGHT_SLEEP_TIME':int,'RECOVER':int, \
                  'CONDITION':int,
    }
    if B2C_type == 'OBD':
        fields = OBD_fields
    else:
        fields = WBD_fields
    
    docs = list()
    for line in rawdata:
        tmp_doc = dict()
        dics = line.split('|')
        if '\n' in dics: dics.remove('\n')
        if len(dics) > 2:
            for dic in dics:
                try:
                    # Parsing base64 encoded data to decode string
                    if 'raw_data' in dic:
                        key = 'raw_data'
                        value = base64.b64decode(dic.split('\n')[0].split('raw_data=')[-1])
                    else:
                        key, value = dic.split('=')
                except Exception as err:
                    logging.error("[Exception] Failed to parsing key and value due to {}".format(err))
                    continue
                key = key.upper()
                if key in fields.keys():
                    try:
                        if not ((len(value) == 0) or value.isspace()):
                            if (key == 'REQ_TIME') or (key == 'RECORD_TIME'):
                                # Sample value: '20181122132258'
                                tmp_doc[key] = KST.localize(datetime.strptime(str(value)[:14], "%Y%m%d%H%M%S")) 
                            else:
                                tmp_doc[key] = fields[key](value)
                    except UnicodeEncodeError:
                        if isinstance(value, unicode):
                            tmp_doc[key] = str(value.encode('utf8'))
                    except ValueError as ve:    # For float datatype
                        if ve.message.split(':')[0] == 'invalid literal for int() with base 10':
                            if isinstance(value, fields[key]):
                                tmp_doc[key] = fields[key](value)
                            else:
                                if isinstance(value, str):
                                    if isinstance(fields[key], type(int)):
                                        try:
                                            tmp_doc[key] = fields[key](float(value))
                                        except ValueError as e:
                                            logging.error("[Exception] Value error due to {}".format(e)) 
                                else: 
                                    logging.error("[Exception] Failed to parsing key and value due to {}".format(ve))
                                    pass
                        else:
                            logging.error("[Exception] Failed to parsing key and value due to {}".format(ve))
                            pass
            docs.append(tmp_doc)
    return docs

# mongodb_connect: Remote access to collection MongoDB
def mongodb_connect(id, pw, ip, port):
    try:
        # Access to Collection MongoDB (AWS instance)
        server_config = "mongodb://" + id + ':' + pw + '@' + ip + ':' + port + '/'
        client = MongoClient(server_config)
        db = client.umc
        return db
    except Exception as err:
        logging.error("Failed to connect to mongoDB server {}".format(err))

# insertdb: Inserting documents to 'ALL' collection of 'UMC' database
def insertdb(coll_name, docs):
    db = mongodb_connect(id='', pw='', ip='', port='')
    results = db[coll_name].insert_many(docs)
    #logging.info(results.inserted_ids)

# file2db: Storing file data to collection MongoDB if the file extension is log
def file2db(dirpath, filename, fp):
    if os.path.splitext(filename)[-1] == '.log':
        # To store raw data per month
        coll_name = dirpath[31:35]+dirpath[36:38] # year+month: ex) 201905
        full_filepath = os.path.join(dirpath, filename)
        if 'OBD' in filename:
            B2C_type = 'OBD'
        else:
            B2C_type = 'WBD'
        with open(full_filepath, 'r') as log_fp:
            lines = log_fp.readlines()
            docs = raw2dict(lines, B2C_type)
            insertdb(coll_name=B2C_type+'_'+coll_name, docs=docs)
        fp.write(full_filepath)
        fp.write('|')
        logging.info("Inserted {} ...".format(str(filename)))
 
# openapi_file2db: Pasing OBD/WBD log files in UMC logData folder of Open API server
def openapi_file2db(logfile_path, fp):
    try:
        workDir = os.path.abspath(logfile_path) # root path
        for dirpath, dirnames, filenames in os.walk(workDir):
            filenames.sort()
            # Each file is stored in the mongoDB
            for filename in filenames:
                file2db(dirpath=dirpath, filename=filename, fp=fp)
    except IOError:
        logging.error("No such file: %s" % workDir)

class Watcher():
    def __init__(self):
        self.observer = Observer()
    def run(self, monitoring_path):
        event_handler = Handler()
        self.observer.schedule(event_handler, monitoring_path, recursive=True)
        self.observer.start()
        try:
            event_handler.scanning(logfile_path=monitoring_path)
            while True:
                time.sleep(10) # iteration period: 10sec
        except KeyboardInterrupt, e:
            self.observer.stop()
            logging.warning("Terminated by User") 
        self.observer.join()
    
class Handler(FileSystemEventHandler):
    # on_created: Storing a flie if the file is not in the file list of historyfile
    def on_created(self, event):
        if (event.event_type == 'created') and (event.is_directory == False):
            time.sleep(610) # Wait until the log file is fully stored
            with open(OPENAPI_HISTORYFILE_PATH, 'a+t') as fp:
                exist_list = fp.readline().split('|')
                if not event.key[1] in exist_list:
                    if 'OBD' in event.key[1]:   # For OBD
                        file_dir = event.key[1].split('/O')[0]
                    else:                       # For WBD
                        file_dir = event.key[1].split('/W')[0]
                    file_name = event.key[1].split('/')[-1]
                    file2db(dirpath=file_dir, filename=file_name, fp=fp)

    # scanning: Scannning for missing files when process runs
    def scanning(self, logfile_path):
        tmp1 = []
        workDir = os.path.abspath(logfile_path)
        for dirpath, dirnames, filenames in os.walk(workDir):
            for filename in filenames:
                tmp1.append(os.path.join(dirpath, filename))
        with open(OPENAPI_HISTORYFILE_PATH, 'a+t') as fp:
            tmp2 = fp.readline().split('|')
            tmp_list = list(set(tmp1) - set(tmp2))
            if len(tmp_list) != 0:
                tmp_list.sort()
                for full_filepath in tmp_list:
                    if 'OBD' in full_filepath:  # For OBD
                        file_dir = full_filepath.split('/O')[0]
                    else:                       # For WBD
                        file_dir = full_filepath.split('/W')[0]
                    file_name = full_filepath.split('/')[-1]
                    file2db(dirpath=file_dir, filename=file_name, fp=fp)

# monitoring: Checking new files created periodically 
def monitoring(logfile_path):
    watch = Watcher()
    watch.run(monitoring_path=logfile_path)

if __name__ == '__main__':
    logging.basicConfig(filename='f2db_umc_openapi.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    LOG_MAX_BYTES = 10*1024*1024    # 10 MB
    handlers.RotatingFileHandler(filename='f2db_umc_openapi.log', maxBytes=LOG_MAX_BYTES, backupCount=5)

    OPENAPI_LOGFILE_PATH = '/svc/connected/openapi/logData/'
    OPENAPI_HISTORYFILE_PATH = './stored_openapi_filelist.txt'
    root_path = sys.argv[1] if len(sys.argv) > 1 else OPENAPI_LOGFILE_PATH
    with open(OPENAPI_HISTORYFILE_PATH, 'a+t') as fp:
        if len(fp.readline()) == 0:
            openapi_file2db(logfile_path=root_path, fp=fp)
    monitoring(logfile_path=root_path)

