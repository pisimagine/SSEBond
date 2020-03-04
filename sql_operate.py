import pymysql
import sys
import re
import traceback
from financialmath import Bond_Discount_Rate
import datetime

class DB:
	def __init__(self):
		self.conn = pymysql.connect(host='127.0.0.1',port = 3306,user='root',passwd='Yjs3.1415926',charset='utf8',cursorclass=pymysql.cursors.DictCursor)
		self.cursor = self.conn.cursor()
		
	def Initialize_BondDB(self):			#初始化债券信息数据库，若不存在该数据库，则创建
		#===================债券数据库构成：===========================
		#===============债券基本信息表、债券每日价格表、债券实时价格表===========
		conn = self.conn
		cursor = self.cursor
		dbName = "bond"
		basicInf = "basicInf (ID int primary key auto_increment,UPDATE_TIME DATETIME not null,BOND_CODE VarChar(45))"		#保存国债债券基本信息的数据表
		financialBond = "financialBond (ID int primary key auto_increment,UPDATE_TIME DATETIME not null,BOND_CODE VarChar(45))"	#保存金融债券基本信息
		dayPrice = "dayprice (ID int primary key auto_increment,UPDATE_TIME DATETIME not null)"		#保存债券每日价格的数据表
		timePrice = "timeprice (ID int primary key auto_increment,UPDATE_TIME DATETIME not null)"		#保存债券实时价格的数据表
		try:
			cursor.execute("create database if not exists %s;" % dbName)		#注意，mysql中【数据库名】和【表名】不能加''，故此处dbName不能作为cursor.execute的参数传入
			cursor.execute("use %s;" % dbName)			#选中债券信息数据表
			cursor.execute("create table if not exists %s;" % basicInf)	
			cursor.execute("create table if not exists %s;" % financialBond)
			cursor.execute("create table if not exists %s;" % dayPrice)
			cursor.execute("create table if not exists %s;" % timePrice)
			conn.commit()
		except:
			conn.rollback
			traceback.print_exc()
			return False
		else:
			return True
	
	def Update_BondInf(sel,bondInfList):		#作废：直接更新bond数据库里的整个basicinf数据表
		#参数：bondInfList,为由dict组成的list
		conn = self.conn
		cursor = self.cursor
		
		if(isinstance(bondInfList,list) == True):
			for bondInf in bondInfList:
				if(isinstance(bondInf,dict) == True):
					print()
				else:
					errorInf = "错误：参数中含有非dict变量---" + str(bondInf)
					return errorInf
		else:
			return "错误：传入函数[Update_BondInf]的参数必须为list[{dict},{dict},...]变量！"
	
	def Update_BondInf_Record(self,bondInf,tableName):		#更新单条债券信息，本地数据库存在该债券时有效	
		conn = self.conn
		cursor = self.cursor
		
		if(isinstance(bondInf,dict) ==True):
			sql = "select * from " +tableName+ " where %s = '%s';" % ("BOND_CODE",bondInf['BOND_CODE'])
			#sql = "select * from basicInf where %s = '%s';" % ("BOND_CODE","14322")
			cursor.execute(sql)
			result = cursor.fetchone()
			if(result == None):		#若本地不存在该债券记录，函数返回
				errorInf = "不存在记录"
				return errorInf
			else:				#若本地数据库存在该债券记录，则更新
				for key in bondInf:		#更新网络获取的数据
					if(bondInf[key] != result[key]):
						try:
							cursor.execute("SET SQL_SAFE_UPDATES = 0;")		#关闭sql更新安全模式
							sql = "Update " +tableName+ " set %s = '%s' where %s = '%s';" % (key,bondInf[key],"BOND_CODE",bondInf['BOND_CODE'])		
							cursor.execute(sql)
							#更新该条记录不同字段
							sql = "Update " +tableName+ " set UPDATE_TIME = CURRENT_TIMESTAMP where %s = '%s';" % ("BOND_CODE",bondInf['BOND_CODE'])
							#更新该条记录的更新时间
							cursor.execute(sql)
							cursor.execute("SET SQL_SAFE_UPDATES = 0;")		#打开sql更新安全模式
							conn.commit()
						except:
							conn.rollback()
							errorInf = "错误：债券代码为[" + bondInf['BOND_CODE'] + "]的记录[" + bondInf[key] + "]未更新，因为" + str(traceback.format_exc())
							return errorInf		#返回异常消息
				return True
		else:
			return "错误：传入函数【Insert_BondInf】的参数必须为字典变量！"
	
	def Insert_BondInf(self,bondInf,tableName):		#插入债券信息，本地数据库不存在该债券时有效
		conn = self.conn 
		cursor = self.cursor

		if(isinstance(bondInf,dict) == True):		#判断传入的参数是否为字典
			#计算最新利润率
			# todate = datetime.datetime.now().strftime('%Y-%m-%d')
			# dr = Bond_Discount_Rate(bondInf["ISSUE_PRICE"],bondInf["FACE_RATE"],bondInf["FACE_VALUE"],bondInf["PAY_TYPE"],bondInf["START_DATE"],todate,bondInf["PAY_DATE"],bondInf["END_DATE"])
			sqlColumns = "(UPDATE_TIME,"
			sqlValues = "(CURRENT_TIMESTAMP,"
			for key in bondInf.keys():
				# if(self.Is_Column_Exists("bond","basicInf",key) == False):		#若数据表中不存在该字段，则增加字段
					# self.Add_Column("bond","basicInf",key)
				sqlColumns = sqlColumns + key + ","
				sqlValues = sqlValues + "\"" +  bondInf[key] + "\","
			sqlColumns = sqlColumns + "))"
			sqlColumns = sqlColumns.replace(",))",")")
			sqlValues = sqlValues + "))"
			sqlValues = sqlValues.replace(",))",")")
			sql = "insert into %s %s VALUES %s;" % (tableName,sqlColumns,sqlValues)
			try:
				cursor.execute(sql)	
				conn.commit()
			except:
				conn.rollback()
				info = str(sys.exc_info()) 
				loseColumn = re.search("1054.*?column\s'(.*?)'.*?",info,re.S)		#当出现无字段1054错误时，增加该字段，然后回滚操作
				if(loseColumn != None):
					if self.Add_Column("bond",tableName,loseColumn.group(1)):		
						self.Insert_BondInf(bondInf,tableName)				#若增加字段成功，则重新执行该插入程序
					else:
						print("发生数据库内部错误（1054：无相应字段）时，尝试用Add_Column()函数向数据库增加此字段，但未成功！")
				else:
					return 	traceback.format_exc()		#返回异常消息
		else:
			return "错误：传入函数【Insert_BondInf】的参数必须为字典变量！"
			
	def Is_Column_Exists(self,databaseName,tableName,queryColumnName):		#判断本地数据库databaseName的数据表tableName中是够存在queryColumnName字段
		cursor = self.cursor
		sql = "SELECT 1 FROM  information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' AND table_name='%s' AND COLUMN_NAME='%s';" % (databaseName,tableName,queryColumnName)	
		try:
			cursor.execute(sql)
			data = cursor.fetchone()
		except:
			return "Error:Unknow"
		else:
			if(data == None):
				return False
			else:
				return True		
			
	def Add_Column(self,databaseName,tableName,newColumnName):			#向本地数据库databaseName的数据表tableName中增加queryColumnName字段
		cursor = self.cursor
		conn = self.conn 
		cursor.execute("use %s;" % databaseName)
		try:
			sql = "ALTER TABLE %s add column %s varchar(45);" % (tableName,newColumnName)
			cursor.execute(sql)
			conn.commit()
		except:
			print("错误：向数据库中写入新字段时失败！")
			conn.rollback()
			return False
		else:
			return True
		
	def close(self):			#关闭数据库
		conn = self.conn
		conn.close
	
	def Query_ActiveBond(self):
		#返回结果格式：returnData = {"活跃国债"：[国债信息列表],"活跃金融债":[金融债信息列表]}
		
		conn = self.conn
		cursor = self.cursor
		
		try:
			cursor.execute("use bond;")
			sql = "select BOND_CODE,FACE_RATE,PAY_TYPE,FACE_VALUE,BOND_ABBR,END_DATE,CLOSE_TIME,CLOSE_PRICE,CLOSE_AMT,PROFIT_RATE,ALREADY_ALERT from BasicInf WHERE (PROFIT_RATE !='-' AND CLOSE_AMT != '0' ) ORDER BY PROFIT_RATE DESC;"
			cursor.execute(sql)
			result = cursor.fetchall()
			key = "BasicInf"
			returnData = {key:result}
			sql = "select BOND_CODE,FACE_RATE,PAY_TYPE,FACE_VALUE,BOND_ABBR,END_DATE,CLOSE_TIME,CLOSE_PRICE,CLOSE_AMT,PROFIT_RATE,ALREADY_ALERT from FinancialBond WHERE (PROFIT_RATE !='-' AND CLOSE_AMT != '0' ) ORDER BY PROFIT_RATE DESC;"
			cursor.execute(sql)
			result = cursor.fetchall()
			key = "FinancialBond"
			returnData[key] = result
		except:
			return traceback.format_exc()		#返回异常消息
		else:
			return returnData
	
	def Query_NonActiveBond(self):		#获取交易数量为0的国债 暂时只返回国债，不返回金融债
		conn =self.conn
		cursor = self.cursor
		
		cursor.execute("use bond;")
		sql = "select * from BasicInf WHERE (PROFIT_RATE !='-' AND CLOSE_AMT = '0' ) ORDER BY PROFIT_RATE DESC;"
		cursor.execute(sql)
		result = cursor.fetchall()
		return result   #返回为国债列表		


def test_DB():
	db = DB()
	d = db.Query_ActiveBond()
	
	for r in d:
		print("=="*60)
		print(r)
		print("--"*60)
		for f in d[r]:
			print(f)
			print("--"*60)
	
	db.close()
#test_DB()
