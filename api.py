#-*- coding: utf-8 -*-
#ceshi

import os
import sys

_dir = os.path.dirname(os.path.abspath(__file__))
_updir = os.path.abspath(os.path.join(_dir, '..'))
if _updir not in sys.path:
    sys.path.insert(0, _updir)

import re
import json
import random
import time
import hashlib
from flask import Blueprint, request, redirect, abort
from flask import make_response, render_template
from utils.wraps import jview
import json
from utils import wraps as u
from utils.wraps import login_validate
from utils.wraps import super_validate
import services
from user.User import User
from utils.dbcon import DataBase
from utils import table_columns_map
from services  import API_Validate 
api_bp = Blueprint('api', __name__, template_folder='templates')




#验证接口,人员信息
@api_bp.route('/user_info/',methods=['GET','POST'])
def get_person():
    if request.method == 'POST':
       account = request.form.get('account')
    if request.method == 'GET':
       account = request.args.get('account')
    result = {'flag':False}
    if not account: 
        result['msg'] = 'account invalid.'
        return json.dumps(result)
    user_info = User.get_user_info(account)
    return json.dumps(user_info)



#司徒写回数据
@api_bp.route('/test_api_url/', methods=['GET', 'POST'])
def test_write_api_url():
    _type,_sys_name,_sys_id,_user_id,_passw,_oa='','','','','',''

    _type=request.form['_type']
    _sys_name=request.form['_sys_name']
    _sys_id=request.form['_sys_id']
    _user_id=request.form['_user_id']
    _passw=request.form['_passw']
    _oa=request.form['_oa']

    #身份验证

    #数据操作

    print _type,_sys_name,_sys_id,_user_id,_passw,_oa
    print "__________________________________"
    dict={'deal_msg':u'处理成功','time':'2015-12-01'}
    return json.dumps(dict,ensure_ascii=False)



#司徒读取数据
@api_bp.route('/test_get_api_url/', methods=['GET', 'POST'])
def test_read_api_url():

    #身份验证

    #数据操作

    test_table_name=['OA_POSITION','OA_STATION']

    conn=DataBase.connect()
    cursor=conn.cursor()
    
    for table_name in test_table_name:
        
        columns=services.get_table_col(table_name,cursor,conn)
        v=table_columns_map.yydztz_table_info[table_name]
        map_table=v['table_name']
        map_attr_id=v['columns_map'] 
        attr_ids=[]
        for col in columns:
            col_name=col['col_name']
            if map_attr_id.has_key(col_name):
                col_id=map_attr_id[''+col_name+'']
                attr_ids.append(col_id)
            #查询数据


            #构建插入数据

        
        print attr_ids    
    print "-------"
    DataBase.close(cursor,conn)
    dict={'deal_msg':u'处理成功','time':'2015-12-01'}
    return json.dumps(dict,ensure_ascii=False)




"""
获取部门岗位
"""
@api_bp.route('/get_dep_positions', methods=['GET', 'POST'])
def get_dep_positions():
    conn=DataBase.connect()
    cursor=conn.cursor()
    api = API_Validate(1,1,2)
    r=api.validate_sys()
    if r['flag']:
        rs = services.get_org_positions(conn,cursor)
        return json.dumps(rs,ensure_ascii=False)
    else:
        return json.dumps(r,ensure_ascii=False)
    




"""
获取岗位人员
"""
@api_bp.route('/get_position_users', methods=['GET', 'POST'])
def get_position_users():
    conn=DataBase.connect()
    cursor=conn.cursor()

    api = API_Validate(1,1,2)
    r=api.validate_sys()
    if r['flag']:
        print "diao yong"
        print "9909090"
        rs={}
        #业务逻辑
        position_id = request.args.get('position_id')   #岗位id
        print position_id
        dep_type = request.args.get('dep_type')    #组织类型 1:部门 2:科室/中心 3:岗位
        print dep_type
        dep_id = request.args.get('dep_id')   #部门或者科室id
        print dep_id
        rs = services.get_position_users(position_id,dep_type,dep_id,conn,cursor)
        
        return json.dumps(rs,ensure_ascii=False)
    else:
        return json.dumps(r,ensure_ascii=False)







@api_bp.route('/get_org_position_users', methods=['GET', 'POST'])
def get_org_position_users():
    conn=DataBase.connect()
    cursor=conn.cursor()
    api = API_Validate(1,1,2)
    r=api.validate_sys()
    if r['flag']:
        print "获取数据"
    else:
        return json.dumps(r,ensure_ascii=False)





def get_org_users():
    conn=DataBase.connect()
    cursor=conn.cursor()
    r = services.get_org_tree_users(conn,cursor)
    print r




def get_orgs():
    conn=DataBase.connect()
    cursor=conn.cursor()
    r = services.get_org_tree(conn,cursor)
    print r








if __name__ == '__main__':
    #longfang()
    api = API_Validate(1,1,2)
    r=api.validate_sys()
    print r['flag']
    if r['flag']:
        print u"准备获取数据"
        get_org_position_users()
    else:
        print u"返回空数据"

    #redis 数据库
    import redis
    r = redis.Redis(host='localhost', port=6379, db=0)   #如果设置了密码，就加上password=密码

    rs = r.get('test')   
    print rs
