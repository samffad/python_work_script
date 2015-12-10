# -*- coding: UTF-8 -*-
import time
import os
import datetime
import sys
from frame.lib.lianjia.oracle import Oracle
from frame.lib.lianjia.mysql import Mysql
import threading

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

    def gen_sql_dict(self, owner):
        sql_dict = {}
        for field_tup in self.tup_list:
            value = owner[  field_tup[1]  ]
            if len( field_tup ) > 2:
                fun = field_tup[2]
                value = getattr(self, fun)( owner )
            if value is None:
                continue
            sql_dict[ field_tup[0] ] = value

        return sql_dict

    def check_standard_id( self, standard_id, oracle_pkid ):
        global ORACLE_MYSQL_DICT
        #check mysql exist
        sql = "select * from owner_info where standard_house_id = %d" %standard_id
        res = self.mysql.query_sql( sql ) 
        if len(res) != 0:
            if len(res) > 1 :
                print('unexpected two tuples containing same standard_house_id' + standard_id)
            ORACLE_MYSQL_DICT[oracle_pkid]=res[0]['id']
            return False
 
        #check is valud in hdic
        
        return True


    def run(self):
        self.batch( self.owner_list, self.name )

    def batch( self, owner_list, i):
        error_fp = open( "error/error.log." + str(i), "w" )
        normal_fp = open( "error/normal.log." + str(i), "w" )
        for owner in owner_list:
            dict = {}
            try:
                standard_id = owner["HSTANDARD_ID"]
                if not self.check_standard_id( standard_id, owner['PKID'] ):
                   continue
                dict = self.gen_sql_dict( owner )
                self.mysql.insert_dict( table = "owner_info", argv = dict )
                normal_fp.write( str(dict["standard_house_id"]) + "\n" )

                #check again to get newly inserted tuple id
                self.check_standard_id( standard_id, owner['PKID'] )
            except Exception,e:
                error_fp.write( str( standard_id  ) + "\n" )
        error_fp.close()
        normal_fp.close() 

def getOwnerTrackData(city_code):
    

def process():

    #step 1, grab oracle ownerinfo data. city sensitive
    oracle = Oracle( user = "hl_q", passwd = "hl_q" , host = "172.16.6.19:1521/HLSALES" )

    oracle_sql  = "select h.standard_id AS HSTANDARD_ID, h.building_no, h.room_no, u.user_code, \
                   i.OWNER_NAME, i.MOBILE_PHONE, i.HOME_PHONE, i.OFFICE_PHONE, i.OWNER_TYPE, i.QQ, i.IS_VALID, i.CREATED_TIME, i.CREATED_BY, i.PKID as PKID \
                   from owners.t_owner_info i inner join owners.t_owner_house h on i.house_id = h.pkid   \
                           left join usermgr.ts_user u on i.created_by = u.user_id \
                           inner join owners.t_owner_resblock r \
                               on h.resblock_id = r.pkid \
                   where r.city_code = '320100'  \
                   and ( i.MOBILE_PHONE is not null or  i.HOME_PHONE is not null or  i.OFFICE_PHONE is not null   ) and i.owner_name is not null and i.is_valid = 1 "
    print "begin get all owner"
    all_owners = oracle.query_sql( oracle_sql )
    print "end get all owner" + str( len( all_owners ) )
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
    owner.batch( all_owners, 0 ) 

    #step 3, get oracle ownerinfo_track data.
    global ORACLE_MYSQL_DICT
    print(ORACLE_MYSQL_DICT)

    #step 4, according to previous dict, insert into mysql ownerinfo_followup table

if __name__ == "__main__":
    process()

