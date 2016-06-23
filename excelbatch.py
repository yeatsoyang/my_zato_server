#-*- coding: utf-8 -*-

import os
import sys
from utils.excel import Excel
from utils.dbcon import DataBase

_dir = os.path.dirname(os.path.abspath(__file__))
_updir = os.path.abspath(os.path.join(_dir, '..'))
if _updir not in sys.path:
    sys.path.insert(0, _updir)



"""
EXCEL文件批量导入
"""
class BatchDeal(object):
    def __init__(self,_file_path,_entityid,_head_attrs):
        #文件路径
        self._file_path = _file_path
        #实体id
        self._entityid = _entityid
        #excel表头
        self._head_attrs = _head_attrs
        #获取数据库连接
        self.conn = DataBase.connect()



    def data_batch_deal(self):
        #文件上传的目录
        filedir = os.environ['_UPLOAD_FOLDER_']
        #获取excel对象
        print self._file_path
        excel = Excel(filedir,self._file_path['file_name'])
        #读取EXCEL数据
        rs=excel.read_excel_model_data()
        #获取游标
        cursor = self.conn.cursor()
        try:
            #限制一次导入记录数
            if len(rs)>3001:
                return {'res':[],'batch_num':'',
                        'table_name':'','entity_id':'',
                        'status':4,'msg':u'记录数超过3000条!'}

            #获取该实体所有属性
            entity_attrs = self.get_entity_attrs()
            
            #找出名称属性
            name_attr = filter(self.filter_nameAttr,entity_attrs)
            
            #找出类型为实体的属性
            entity_attr = filter(self.filter_entityAttr,entity_attrs)
            
            #获取excel表头属性字段
            header_info = self.get_excel_heads(rs)
            
            #验证EXCEL
            validata_result = self.excel_validata(header_info,entity_attrs)
            if validata_result[0] == False:
                return {'res':[],'batch_num':'',
                        'table_name':'','entity_id':'',
                        'status':validata_result[1],'msg':validata_result[2]}
            
            #正式开始写临时表
            self.import_to_tmpTable(rs)


        except Exception:
            print "exception"
        finally:
            print "finally"



    """
    获取所有实体属性
    """
    def get_entity_attrs(self):
        #获取游标
        cursor = self.conn.cursor()
        sql = "select attr_id,attr_name,entity_id,show_name,"\
              "value_source_type,value_source_code from "\
              "r_attr t  where t.entity_id=:entity_id"
        row = DataBase.query_params(sql,{'entity_id':self._entityid},
                                    cursor,"Many")
        rs = None
        rs = row[1] if row is not None else None
        return rs


        
    """
    找出名称属性记录
    """
    def filter_nameAttr(self,dict_):
        if dict_['attr_name'] ==\
            os.environ['name_attr']:
            return True
        else:
            return False


    """
    找出类型为实体的属性
    """
    def filter_entityAttr(self,dict_):
        if dict_['value_source_type'] == "ENTITY":
            return True
        else:
            return False



    """
    获取excel表头信息
    """
    def get_excel_heads(self,rs):
        #EXCEL所有属性字段,除去字母的字段
        headers_,header_attrs,sheet_num = [],[],1
        for index,item in enumerate(rs):
            for i in item:
                if i[0] == '':
                    return {'flag':False}
                headers_.append(i[0])
                if i[0]!='b' and i[0]!='m' and i[0]!='t'\
                   and int(i[0]) not in header_attrs:
                       header_attrs.append(i[0])
                #判断EXCEL文件中是否有两个SHEET
                if i[0] == 't' and i[0] == 2:
                    sheet_num = 2

        return {'header':headers_,
                'header_attrs':header_attrs,
                'sheet_num':sheet_num,
                'flag':True}



    """
    EXCEL表格验证
    """
    def excel_validata(self,header_info,entity_attrs):

        if header_info['flag']:
            #验证是否为新增模板
            if "b" not in  header_info['header']:
                return False,2,u'新增!'

            #验证是否有多个sheet
            if header_info['sheet_num']>1:
                return False,6,u'当前导入的EXCEL仅支持一个工作簿(sheet)!'

            #验证EXCEL中的所有字段是否存在于选择的实体中
            header_attrs = list(set(header_info['header_attrs']))
            entity_attrs_ = [r['attr_id'] for r in entity_attrs] 
            flag = True
            for h in header_attrs:
               if int(h) not in entity_attrs_:
                   flag = False
                   break
            if flag == False:
                return False,0,u'非所有属性在所选的实体中，请检查EXCEL表格!'
            
            return True
        else:
            return False,5,u'当前导入的EXCEL缺少部分表头信息!'
       






        """
        导入到临时表中
        """
        def import_to_tmpTable(self,rs):

            print "&&&&&"





"""
        r={}

        entity=en_s.query_entity_by_param_cursor({'entity_id':entity_id},"One",cursor)
        attr=attr_s.query_attrs_by_param_cursor({'entity_id':entity['entity_id'],
                                                 'attr_name':os.environ['name_attr'].encode("utf-8")},
                                                 "One",cursor)

        #查询该实体的所有属性
        attrs=attr_s.query_attrTrees_cursor(entity['entity_id'],cursor)

        entity_attrs=[a['attr_id'] for a in attrs]

        #属于实体的属性
        tmp_entity_sub_attrs=[{'attr_id':tmp_a['attr_id'],'value_source_code':tmp_a['value_source_code']}\
                              for tmp_a in attrs if tmp_a['value_source_type']=='ENTITY']
        entity_sub_attrs=[]

        rs_attrs=[]
        head_attr=[]
     
        if len(rs)>3001:
            r['res']=[]
            r['batch_num']=''
            r['table_name']=''
            r['entity_id']=''
            r['status']=4
            r['msg']=u"记录数超过3000条"
            return r

        for us in rs:
            for u in us:
                head_attr.append(u[0])
                if u[0]!='b' and u[0]!='m' and u[0]!='t' and int(u[0]) not in rs_attrs:
                    rs_attrs.append(int(u[0]))

        if "b" not in head_attr:
            r['res']=[]
            r['batch_num']=''
            r['table_name']=''
            r['entity_id']=''
            r['status']=2
            r['msg']=u"新增"
            return r

        #判断导入的字段是否在都在所选实体中
        r_flag=True
        for ra in rs_attrs:
            #不在里面
            if ra not in entity_attrs:
                r_flag=False
                break
            #在里面
            else:
                for t in tmp_entity_sub_attrs:
                    if ra==t['attr_id']:
                        entity_sub_attrs.append(t)
                        break

        if r_flag==False:
            r['res']=[]
            r['batch_num']=''
            r['table_name']=''
            r['entity_id']=''
            r['status']=0
            return r


        #查询所有的属性取值范围
        params={}
        params['owner_id']=request.environ.get('owner_id','') 

        value_source_code_items=vs.query_valuesources_by_params_cursor(params,"Many",cursor)

        c_v=du.list_toUTF8(value_source_code_items)

        table_name=entity['entity_value_table']
        name_attr_id=attr['attr_id']

        insert_l=[]
        single_insert=[]
        s_insert_list=[]

        #查询批次号
        b_num=services.search_batch_num(cursor)
        b_num=b_num['nextval']


        num=len(rs)
        if len(rs)==0:
            num=1
        #查询分组编码
        r_b_num={}
        r_b_num=services.search_group_num_batch(num,cursor)
        num_s=range(r_b_num['current'],r_b_num['next'])

        #查询instance_ids
        i_b_num={}
        i_b_num=services.search_instance_id_batch(num,cursor)
        num_is=range(i_b_num['current'],i_b_num['next'])


        user_id=session['user_id']
        user_type=session['user_type']
        if user_type=="admin":
            user_id=(-1)*int(user_id)
        else:
            user_id=user_id


        #同一个tab重复出现的编号
        dupl_instance_id=[]

        for tmp_r in rs:
            tmp=tmp_r[0]
            if tmp[1]=='':
                continue
            if tmp[1] not in single_insert:
                single_insert.append(tmp[1])
                #r=services.search_instance_id(cursor)
                #tmp_dict={'instance_id':r['nextval'],'b':tmp[1]}
                tmp_dict={'instance_id':num_is[0],'b':tmp[1]}
                del num_is[0]
                s_insert_list.append(tmp_dict)

        instance_ids=[]
        name_list,name_v=[],[]

        #excel定位---行
        row=4
        _tab=1
        tbs=[]
        print u"处理开始..."
        for tmp_r in rs:
            row_tmp={}
            if tmp_r[0][1]=='':
                continue
            t=du.is_valueofdict_in_dicts(s_insert_list,arg='b',arg_v=tmp_r[0][1])
            instance_id=''
            instance_id=t[0]['instance_id']
            
            if instance_id not in instance_ids:
                instance_ids.append(instance_id)

            #g_num=services.search_group_num(cursor)
            #v_g_num=g_num['nextval']

            v_g_num=num_s[0]
            del num_s[0]

            import datetime
            now = datetime.datetime.now()
            
            #excel定位---列
            col=2
            for r in tmp_r: 
                
                attr_id,attr_name='',''
                value_source_id,tmp_store_value='',''
                msg,deal_state=u'待处理','0'
             
                if r[0]=='b':
                    continue

                #elif r[0]=='m':
                    #attr_id=name_attr_id
                    #attr_name=u"名称"
                    #tmp_store_value=r[1]
                    #if r[1]=='':
                         #msg,deal_state=u'未填写','3'

                elif r[0]=='t':
                    if r[1] not in tbs:
                        row=3
                        tbs.append(r[1])
                    _tab=r[1]
                    continue

                else:
                    attr_id=int(r[0])

                    tp=du.is_valueofdict_in_dicts(attrs,arg='attr_id',arg_v=attr_id)  

                    if len(tp)!=0:
                        attr_name=tp[0]['attr_name']
                        value_source_type=tp[0]['value_source_type']
                        
                        #空值处理
                        if r[1]=='':
                            msg,deal_state=u'未填写','3'



                        #取值处理(数据库取值)
                        if value_source_type=='ORASQL':
                            v=du.is_valueofdict_in_dicts(c_v,arg='value',arg_v=r[1])
                            if len(v)!=0:
                                if v[0]['value'] is not None:
                                    value_source_id=(v[0]['value_source_code_id'])
                                else:
                                    msg='填写有误'
                                    deal_state='1'
                            else:
                                msg='填写有误'
                                deal_state='1'

                        #时间处理
                        value_store_type=tp[0]['value_store_type']
                        if value_store_type=="date":
                            compare_date=ut._date('1899-12-31')
                            tmp_store_value=''
                            if r[1]!='':
                                tmp=''
                                flag=r[1].isdigit()
                                if flag==False:
                                    msg='时间格式错误'
                                    deal_state='6'
                                    tmp_store_value=str(r[1])
                                else:
                                    tmp=compare_date+datetime.timedelta(days=(int(r[1])-1))
                                    tmp_store_value=str(tmp)

                        #正则验证            
                        if value_source_type=="INPUT":
                            flag=True 
                            if tp[0]['value_limit']!=None:
                                flag=rex_v(r[1],tp[0]['value_limit'])
                            #如果验证不通过
                            if flag==False:
                                msg='正则验证不通过'
                                deal_state='10'
                                tmp_store_value=str(r[1])
                            else:
                                tmp_store_value=str(r[1])

                        else:
                            flag=True 
                            if tp[0]['value_limit']!=None:
                                flag=rex_v(r[1],tp[0]['value_limit'])
                            
                            if flag==False:
                                msg='正则验证不通过'
                                deal_state='10'
                                tmp_store_value=str(r[1])
                            else:
                                tmp_store_value=r[1]    

                    else:
                        attr_name=tp[0]['attr_name']
                        msg,deal_state=u'该属性不属于当前实体','2'

                if attr_name == os.environ['name_attr'].encode("utf-8"):
                    if tmp_store_value not in name_v:
                        name_v.append(tmp_store_value)
                        name_list.append({'attr_id':attr_id,'entity_id':entity['entity_id'],'value':tmp_store_value}) 

                row_tmp={'instance_id':instance_id,'entity_id':entity['entity_id'],'attr_id':attr_id,'attr_name':attr_name,\
                         'multiple_group_number':str(v_g_num),'multiple_sort_number':'','value_source_id':str(value_source_id),\
                         'value':str(tmp_store_value),'value_file_name':'','value_clob':'','update_user_id':user_id,\
                         'update_time':now,'operate_type':'B','batch_number':b_num,'msg':msg,'deal_state':deal_state,\
                         'excel_row':str(row),'excel_col':str(col),'excel_tab':str(_tab)}
                
                col=col+1
                insert_l.append(row_tmp)
            row=row+1
        print u"批处理完成.."

        add_parent_attr(insert_l,instance_ids,b_num,cursor)
        print u"保存到临时表中..."
        services.save_v(insert_l,table_name,conn,b_num,cursor)
        

        
        print u"检查数据库是否已存在记录..."
        #重复记录检查数据库中已经存在
        check_duplicate(table_name,name_list,name_attr_id,cursor,conn)

        print u"检查数据关联实体..."
        #如果属性为实体，检查实体是否存在(当前已名称做唯一标识)
        check_no_exits_entity(table_name,entity_sub_attrs,b_num,cursor,conn)
         
        print u"权限检查"
        #权限验证 begin
        check_permission(entity_id,table_name,b_num,attrs,cursor,conn)
        #权限验证 end

        tp_r={}
        print u"检查其他信息..."
        res=services.query_batch_info(entity_id,table_name,b_num,cursor)
        conn.commit()

    finally:
        DataBase.close(cursor,conn)
    tp_r['res']=res
    tp_r['batch_num']=b_num
    tp_r['table_name']=table_name
    tp_r['entity_id']=entity['entity_id']
    tp_r['status']='1'
    return tp_r

"""
