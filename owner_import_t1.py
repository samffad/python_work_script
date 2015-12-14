# -*- coding: UTF-8 -*-
import time
import os
import datetime
import sys
from frame.lib.lianjia.oracle import Oracle
from frame.lib.lianjia.mysql import Mysql
import threading
import logging
import pdb

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
#used to store oracle pkid and mysql pkid mapping relation
ORACLE_MYSQL_DICT = {}

class Owner_import( threading.Thread ):
    
    def __init__(self, list, name):
        threading.Thread.__init__(self)
        self.owner_list = list
        self.nams = name
        self.oracle = Oracle( user = "hl_q", passwd = "hl_q" , host = "172.16.6.19:1521/HLSALES" )
        #self.oracle = Oracle( user = "hldba", passwd = "WZCXK20AKGD0P0OL" , host = "172.16.6.114:1521/PROCINST" )
        #self.mysql = Mysql( user = "biz_admin", passwd = "5664922ed6404022b4b192499dc56399" , host = "10.10.6.21", port = 6623, db = "biz_huadong", charset = 'utf-8' )
        self.mysql = Mysql( user = "se", passwd = "123456" , host = "172.30.11.240", port = 3306, db = "biz_huadong", charset = 'utf8' )

#        self.mysql = Mysql( user = "biz_admin", passwd = "5664922ed6404022b4b192499dc56399" , host = "10.10.6.21", port = 6623, db = "biz_huadong", charset = 'utf8' )
        # new_table_field_name,  old_field_name, transfer by function or null skip
        self.tup_list = [( "standard_house_id", "HSTANDARD_ID"), \
                     ( "name", "OWNER_NAME"), \
                     ( "phone", "MOBILE_PHONE"), \
                     ( "home_phone", "HOME_PHONE"), \
                     ( "office_phone", "OFFICE_PHONE" ), \
                     ( "owner_type", "OWNER_TYPE",  "transfer_owner_type"  ), \
                     ( "qq", "QQ" ), \
                     ( "STATUS", "IS_VALID"), \
                     ( "create_time", "CREATED_TIME", "date_time_transfer"), \
                     ( "creator_id", "CREATED_BY",  "gen_create_id"  ) \
                    ]

        self.track_tup_list = [( "owner_id", "OWNERINFO_MYSQL_PKID"), \
                     ( "phone", "", "select_phone"), \
                     ( "description", "TRACK_DESCRIPTION"), \
                     ( "creator_id", "creator_id", "gen_create_id"), \
                     ( "creator_name", "DESCRIPTION"  ), \
                     ( "create_time", "CREATED_TIME", "timestamp_convert"), \
                     ( "org_code", "ORG_CODE"), \
                     ( "org_name", "ORG_SHORT_NAME"), \
                    ]

    def select_phone(self, owner):
        if owner['MOBILE_PHONE'] is not None:
            return owner['MOBILE_PHONE']
        elif owner['HOME_PHONE'] is not None:
            return owner['HOME_PHONE']
        elif owner['OFFICE_PHONE'] is not None:
            return owner['OFFICE_PHONE']
        else:
            return None

    def timestamp_convert(self, owner):
        if owner['CREATED_TIME'] is not None:
            #return time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(owner['CREATED_TIME'], "%Y%m%d%H%M%S"))
            #print(time.strptime(owner['CREATED_TIME'], "%Y%m%d%H%M%S"))
            #print(time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(owner['CREATED_TIME'], "%Y%m%d%H%M%S")))
            return time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(owner['CREATED_TIME'], "%Y%m%d%H%M%S"))
            #return owner['CREATED_TIME'].strftime("%Y%m%d%H%M%S")
        return None

    def date_time_transfer(self, owner):
        if owner["CREATED_TIME"] is not None:
            return  owner["CREATED_TIME"].strftime("%Y-%m-%d %H:%M:%S")
        return None

    def transfer_owner_type(self, owner):
        if owner["OWNER_TYPE"] == 0:
            return 0
        else:
           return 3

    def gen_create_id(self, owner):
        user_code = owner["USER_CODE"]
        if user_code == None or not user_code.isdigit():
            return None
        
        return str( 1000000000000000 + int(user_code) )

    def gen_sql_dict(self, owner, dict_mapping):
        sql_dict = {}
        for field_tup in dict_mapping:
            value = None
            if len(field_tup) <= 2:
                value = owner[  field_tup[1]  ]
            elif len( field_tup ) > 2:
                fun = field_tup[2]
                value = getattr(self, fun)( owner )
            if value is None:
                continue
            sql_dict[ field_tup[0] ] = value

        return sql_dict

    def check_standard_id( self, standard_id, oracle_pkid ):
        #check mysql exist
        sql = "select * from owner_info where standard_house_id = %d" %standard_id
        res = self.mysql.query_sql( sql ) 
        if len(res) != 0:
            if len(res) > 1 :
                print('unexpected two tuples containing same standard_house_id' + standard_id)
            ORACLE_MYSQL_DICT[oracle_pkid]=res[0]['id']
            #print(ORACLE_MYSQL_DICT[oracle_pkid])
            return False
 
        #check is valud in hdic
        return True
    def check_ownerinfo_freshness(self, oracle_ownerinfo_pkid, logging) :
        logging.info("checking ownerinfo pkid, oracle pkid %s" % oracle_ownerinfo_pkid)
        sql = "select * from dm_temp where oracle_ownerinfo_pkid = %s" % oracle_ownerinfo_pkid
        res = self.mysql.query_sql(sql)
        if len(res) != 0:
            return False
        return True


    def run(self):
        self.batch( self.owner_list, self.name )

    def batch_track(self, owner_track_list, i, logging) :
        error_fp = open("error/error.log." + str(i), "w")
        normal_fp = open("error/normal.log." + str(i), "w")
        for track in owner_track_list :
            dict = {}
            try:
                #pdb.set_trace()
                logging.info("begin to migration track, info %s" % track)
                #0. check if has migrated
                sql = "select * from dm_temp where oracle_track_pkid = %d" % track['TRACK_PKID']
                res = self.mysql.query_sql( sql )

                if len(res) != 0 :
                    logging.warning("track pkid %s has been handled, skip." % track['TRACK_PKID'])
                    continue

                #1. get oracle owner info pkid to new mysql owner info pkid
                sql = "select * from dm_temp where oracle_ownerinfo_pkid = %s and oracle_track_pkid is null" % track['OWNER_PKID']
                res = self.mysql.query_sql( sql ) 

                if len(res) == 0:
                    logging.warning("HEAVY_ERROR. no mysql ownerinfo mapping relation find, ownerinfo oracle id is %s" % track['OWNER_PKID'])
                    continue

                logging.info("begin to insert into mysql follow up track, info %s" % track)
                track['OWNERINFO_MYSQL_PKID']=res[0]['mysql_ownerinfo_pkid']#mysql_ownerinfo_pkid#ownerinfo_mysql_pkid
                dict = self.gen_sql_dict(track, self.track_tup_list)
                dict['status']='1'
                logging.info(dict)
                self.mysql.insert_dict(table = "owner_follow_up", argv=dict)
                logging.info("SUCCESS for new mysql follow up insert. oracle track pkid %s, data %s" % (track['TRACK_PKID'], dict))

                #get mysql new pkid
                sql = "select max(id) as id from owner_follow_up where owner_follow_up.owner_id = %s" % track['OWNERINFO_MYSQL_PKID']
                res = self.mysql.query_sql(sql)

                mysql_track_pkid = res[0]['id']
                logging.info(res)
                #store relation in temp table
                temp_dict = {}
                temp_dict['oracle_ownerinfo_pkid']=track['OWNER_PKID']
                temp_dict['oracle_track_pkid']=track['TRACK_PKID']
                temp_dict['mysql_ownerinfo_pkid']=track['OWNERINFO_MYSQL_PKID']
                temp_dict['mysql_followup_sh_id']=track['STANDARD_ID']
                temp_dict['mysql_followup_pkid']=mysql_track_pkid
                #pdb.set_trace()
                self.mysql.insert_dict(table = "dm_temp", argv = temp_dict)

                logging.info("SUCCESS for ownerinfo follow up migration, oracle pkid %s, mysql pkid %s" % (track['TRACK_PKID'], mysql_track_pkid))

            except Exception, e:
                pdb.set_trace()
                logging.warning("exception happens for ownerinfo follow up migration. oracle pkid %s. ERROR %s" % (track['TRACK_PKID'], e))

    def batch( self, owner_list, i, logging):
        error_fp = open( "error/error.log." + str(i), "w" )
        normal_fp = open( "error/normal.log." + str(i), "w" )
        count = 0
        for owner in owner_list:
            dict = {}
            # count=count+1
            # if count>110 :
            #     return
            try:
                logging.info('begin to insert owner info data %s' % owner)
                standard_id = owner["HSTANDARD_ID"]
                # if not self.check_standard_id( standard_id, owner['PKID'] ):
                #    continue
                if not self.check_ownerinfo_freshness(owner['ORACLE_PKID'], logging) :
                    logging.warning("dup ownerinfo pkid find, oracle pkid %s" % (owner['ORACLE_PKID']))
                    continue
                dict = self.gen_sql_dict(owner, self.tup_list)
                logging.info(dict)
                self.mysql.insert_dict( table = "owner_info", argv = dict )
                logging.info("SUCCESS for new mysql ownerinfo insert. oracle pkid %s, data %s" % (owner['ORACLE_PKID'], dict))

                #get mysql new pkid
                sql = "select max(id) as id from owner_info where owner_info.standard_house_id = %s" % standard_id
                res = self.mysql.query_sql(sql)

                mysql_pkid = res[0]['id']
                #store relation in temp table
                temp_dict = {}
                temp_dict['oracle_ownerinfo_pkid']=owner['ORACLE_PKID']
                temp_dict['mysql_ownerinfo_pkid']=mysql_pkid
                #pdb.set_trace()
                self.mysql.insert_dict(table = "dm_temp", argv = temp_dict)

                logging.info("SUCCESS for ownerinfo migration, oracle pkid %s, mysql pkid %s" % (owner['ORACLE_PKID'], mysql_pkid))

            except Exception,e:
                logging.warning("exception happens for ownerinfo. oracle pkid %s. ERROR %s" % (owner['ORACLE_PKID'], e))
                error_fp.write( str( standard_id  ) + "\n" )
        error_fp.close()
        normal_fp.close() 

def getOwnerTrackData(logging):
    oracle = Oracle( user = "hl_q", passwd = "hl_q" , host = "172.16.6.19:1521/HLSALES" )

    oracle_sql_owner_track = "SELECT \
                    U.user_code as USER_CODE, \
                    U.DESCRIPTION, \
                    i.PKID as OWNER_PKID, \
                    i.OWNER_NAME as OWNER_NAME, \
                    i.MOBILE_PHONE as MOBILE_PHONE, \
                    i.HOME_PHONE as HOME_PHONE, \
                    i.OFFICE_PHONE as OFFICE_PHONE, \
                    i.OWNER_TYPE, \
                    i.QQ as QQ, \
                    i.IS_VALID, \
                    i.STANDARD_ID as STANDARD_ID, \
                    t.pkid as track_pkid, \
                    t.TRACK_DESCRIPTION, \
                    t.CREATE_USER_NAME, \
                    t.TRACK_DATE as CREATED_TIME, \
                    o.ORG_CODE AS ORG_CODE, \
                    o.ORG_SHORT_NAME AS ORG_SHORT_NAME \
                FROM \
                    owners.t_owner_info i \
                INNER JOIN OWNERS.T_OWNER_TRACK t ON  i.PKID = t.OWNER_ID \
                INNER JOIN owners.t_owner_house H ON i.house_id = H.pkid \
                LEFT JOIN usermgr.ts_user U ON t.created_by = U.user_id \
                LEFT JOIN USERMGR.TS_ORG o ON t.STOREGROUP_ID = o.ORG_ID \
                INNER JOIN owners.t_owner_resblock r ON H.resblock_id = r.pkid \
                WHERE \
                    r.city_code = '440300' \
                AND ( \
                    i.MOBILE_PHONE IS NOT NULL \
                    OR i.HOME_PHONE IS NOT NULL \
                    OR i.OFFICE_PHONE IS NOT NULL \
                ) \
                And t.CREATED_BY IS NOT NULL \
                AND i.owner_name IS NOT NULL \
                AND i.is_valid = 1"

    logging.info("begin to get owner_track_info")
    all_owner_tracks = oracle.query_sql(oracle_sql_owner_track)
    logging.info("end to get owner_track_info, rows " + str(len(all_owner_tracks)))

    tracks = Owner_import( all_owner_tracks, 0 )
    tracks.batch_track( all_owner_tracks, 0, logging) 


def process(logging):

    #step 1, grab oracle ownerinfo data. city sensitive
    oracle = Oracle( user = "hl_q", passwd = "hl_q" , host = "172.16.6.19:1521/HLSALES" )

    oracle_sql  = "select h.standard_id AS HSTANDARD_ID, h.building_no, h.room_no, u.user_code, \
                   i.PKID AS oracle_pkid, i.OWNER_NAME, i.MOBILE_PHONE, i.HOME_PHONE, i.OFFICE_PHONE, i.OWNER_TYPE, i.QQ, i.IS_VALID, i.CREATED_TIME, i.CREATED_BY, i.PKID as PKID \
                   from owners.t_owner_info i inner join owners.t_owner_house h on i.house_id = h.pkid   \
                           left join usermgr.ts_user u on i.created_by = u.user_id \
                           inner join owners.t_owner_resblock r \
                               on h.resblock_id = r.pkid \
                   where r.city_code = '440300'  \
                   and ( i.MOBILE_PHONE is not null or  i.HOME_PHONE is not null or  i.OFFICE_PHONE is not null   ) and i.owner_name is not null and i.is_valid = 1"# AND ROWNUM< 100"
    logging.info("begin get all owner")
    all_owners = oracle.query_sql( oracle_sql )
    logging.info("end get all owner" + str( len( all_owners ) ))
#    i = 0
#    ever = 10000
#    thread_num = len( all_owners ) / ever
#    threads = []    
#
#    while i < thread_num:
#        threads.append(  Owner_import( all_owners[ i*ever: (i+1)*ever ] , i  )  )
#        i = i + 1
#
#    threads.append(  Owner_import( all_owners[ i*ever:  ] , i  )  )
#
#    for th in threads:
#        th.start()
#    ret = False
#    while True:
#        for th in threads:
#            if not th.isAlive():
#                print "has thread exist"
#                ret = True
#                break
#        if ret :
#            break
#
#    for th in threads:
#        th.join()

    
#
    #step 2, insert oracle ownerinfo into mysql ownerinfo. And, fill in <oracle_pkid, mysql_pkid> dict
    owner = Owner_import( all_owners, 0 )
    owner.batch( all_owners, 0, logging) 

    #step 3, get oracle ownerinfo_track data. note that there may be dirty data that owner don't leave any phone info!
    

    #step 4, according to previous dict, insert into mysql ownerinfo_followup table

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='dm_ownerinfo.log',
                filemode='w')

    #################################################################################################
    #定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logging.info("********************************OWNER INFO MIGRATION****************************************")

    #begin to work
    process(logging)
    getOwnerTrackData(logging)

