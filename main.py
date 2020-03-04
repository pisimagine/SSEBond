import spider
import json
import sql_operate
import sendmessage
from financialmath import Bond_Discount_Rate
import datetime
import threading
import time
import os
import random
import sys

flagPath =os.path.join(os.getcwd(),"support","flag.txt")
class Thread_Control (threading.Thread):
	def __init__(self,threadName):
		threading.Thread.__init__(self)
		self.threadName =threadName
	def run(self):
		threadName = self.threadName
		if(threadName == "Update_Bond"):
			UP= Update_Bond()
			UP.Commander()
		elif(threadName == "UserService"):
			u= UserService()
			u.Commander()
	
class Update_Bond:
	
	def __init__(self):
		self.sse = spider.SSESpider()
		self.db = sql_operate.DB()
	
	def Commander(self):
		strNow =datetime.datetime.strftime(datetime.datetime.now(),"%H:%M:%S")	#获取当前时间并转化为str，输出格式：HH:MM:SS
		print(strNow + ":数据库同步系统已启用")
		self.Update_financialBond()
		self.Update_NationalBond()		#每天启动数据库同步系统时更新数据库
		
		while(1):		#持续更新活跃债券
			strNow =datetime.datetime.strftime(datetime.datetime.now(),"%H:%M:%S")	#获取当前时间并转化为str，输出格式：HH:MM:SS
			if(strNow > "09:30:00" and strNow < "15:00:00"):		#此处改时间
				print(strNow + ":债券持续更新中")
				self.Update_ActiveSingleBond()
				self.Update_NonActiveSingleBond()
				time.sleep(10)
			else:								#非交易时间自动退出
				print(strNow + ":不在交易时间，债券列表已更新")
				return "不在交易时间"
	
	def Update_NonActiveSingleBond(self):		#更新非活跃状态的债券，随机抽取一个非活跃债券更新		
		sse = self.sse
		db = self.db
		todate = datetime.datetime.now().strftime('%Y-%m-%d')		#获取今日时间
		sqlData = db.Query_NonActiveBond()
		r = random.randint(0,len(sqlData)-1)		#获取随机数
		bond = sqlData[r]		#在所有非活跃债券中随机抽取一个债券
		closeValue = sse.Close_TimePriceAmt(bond["BOND_CODE"])		#调用爬虫获取最新价格
		dr = Bond_Discount_Rate(closeValue[1],bond["FACE_RATE"],bond["FACE_VALUE"],bond["PAY_TYPE"],todate,bond["END_DATE"])
		bond["CLOSE_TIME"] = str(closeValue[0])
		bond["CLOSE_PRICE"] = str(closeValue[1])
		bond["CLOSE_AMT"] =str(closeValue[2])		#更新最新交易量
		bond["PROFIT_RATE"] = str(dr)
		db.Update_BondInf_Record(bond,'basicInf')
	
	def Update_ActiveSingleBond(self): 
		sse = self.sse
		db = self.db
		todate = datetime.datetime.now().strftime('%Y-%m-%d')		#获取今日时间
		sqlData = db.Query_ActiveBond()
		for key in sqlData:
			for bond in sqlData[key]:
				closeValue = sse.Close_TimePriceAmt(bond["BOND_CODE"])		#调用爬虫获取最新价格
				dr = Bond_Discount_Rate(closeValue[1],bond["FACE_RATE"],bond["FACE_VALUE"],bond["PAY_TYPE"],todate,bond["END_DATE"])
				bond["CLOSE_TIME"] = str(closeValue[0])
				bond["CLOSE_PRICE"] = str(closeValue[1])
				bond["CLOSE_AMT"] =str(closeValue[2])		#根据爬虫的获取方式，最新交易量为最新的非零交易量，所以一个债券在一天的某个时候交易量不为零，过后的期间交易量也不会为零。
				bond["PROFIT_RATE"] = str(dr)
				db.Update_BondInf_Record(bond,key)
				time.sleep(1)
		
	def Update_NationalBond(self):
		sse = self.sse
		bondList = sse.NationalBond_List()
		self.Update_BondList(bondList,"basicInf")
		
	def Update_financialBond(self):
		sse = self.sse
		bondList = sse.FinancialBond_list()
		self.Update_BondList(bondList,"financialBond")
	
	def Update_BondList(self,bondList,tableName):
		sse = self.sse
		todate = datetime.datetime.now().strftime('%Y-%m-%d')		#获取今日时间
		db = self.db
		if(db.Initialize_BondDB()):
			
			for bondInf in bondList:
				faceRate = bondInf["FACE_RATE"] + "%"
				#print(bondInf["BOND_CODE"])
				closeValue = sse.Close_TimePriceAmt(bondInf["BOND_CODE"])
				dr = Bond_Discount_Rate(closeValue[1],faceRate,bondInf["FACE_VALUE"],bondInf["PAY_TYPE"],todate,bondInf["END_DATE"])
				#price,faceRate,faceValue,payType,todate,endDate
				bondInf["CLOSE_TIME"] = str(closeValue[0])
				bondInf["CLOSE_PRICE"] = str(closeValue[1])
				bondInf["CLOSE_AMT"] =str(closeValue[2])
				bondInf["PROFIT_RATE"] = str(dr)
				bondInf["ALREADY_ALERT"] = "否"
				
				result = db.Update_BondInf_Record(bondInf,tableName)
				
				if(result == "不存在记录"):
					result = db.Insert_BondInf(bondInf,tableName)
				elif(result != True):
					print(result)
					print("=="*60)
		
		else:
			print("数据库初始化失败！")
			db.close()

class UserService:
	
	def __init__(self):
		self.sendDailyTime = "10:00:00"		#日报时间
		self.alertRate = "3.5%"	#警戒利率
		self.db = sql_operate.DB()
	
	def Commander(self):
		strNow =datetime.datetime.strftime(datetime.datetime.now(),"%H:%M:%S")	#获取当前时间并转化为str，输出格式：HH:MM:SS
		dailySendFlag = "未发送"
		print(strNow + ":邮件预警系统已启用")
		while(1):
			strNow =datetime.datetime.strftime(datetime.datetime.now(),"%H:%M:%S")	#获取当前时间并转化为str，输出格式：HH:MM:SS
			print(strNow + "：邮件预警系统持续运行中")
			if(strNow > "09:30:00" and strNow < "15:00:00"):		#开市时间
				if(strNow > "10:00:00"):
					if(dailySendFlag == "未发送"):
						self.Send_BondDailyReport()
						dailySendFlag = "已发送"
				else:		#当strNow 小于10:00:00，大于09:30:00时有效
					dailySendFlag = "未发送"		#保持dailySendFlag
				self.Send_HighProfit_Bond()		#检测活跃债券利息
			else:
				print(strNow + ":非交易时间，邮件预警系统自动退出")
				return "不在交易时间"
			time.sleep(10)
				
	def Create_Msg(self,data):
		msg = "<!DOCTYPE html><html><body>"
		for key in data:
			msg = msg + "<table style='width:100%'>"
			msg =msg+"<tr style='font-size:200%'>"
			msg =msg + "<th style='text-align:left' colspan='3'>" + key +"</th>"
			msg =msg+"</tr>"
			msg = msg + "<tr>"
			msg = msg + "<th style='text-align:left'>债券代码</th>"
			msg = msg + "<th style='text-align:left'>最新价</th>"
			msg = msg + "<th style='text-align:left'>到期收益率</th>"
			msg = msg + "<th style='text-align:left'>到期日</th>"
			msg = msg + "</tr>"
			for bond in data[key]:
				msg =msg + "<tr>"
				msg =msg +  "<td>" + bond["BOND_CODE"] + "</td>"
				msg =msg +  "<td>" + bond["CLOSE_PRICE"] + "</td>"
				msg =msg +  "<td>" + bond["PROFIT_RATE"] + "</td>"
				msg =msg +  "<td>" + bond["END_DATE"] + "</td>"
				msg =msg + "</tr>"
			msg = msg + "</table>"
		msg = msg+"</body></html>"	#合成邮件内容
		return msg
		
	def Send_BondDailyReport(self):
		sendDailyTime = self.sendDailyTime
		db = self.db
		data = db.Query_ActiveBond()		#获取活跃债券信息
		#strNow =datetime.datetime.strftime(datetime.datetime.now(),"%H:%M:%S")	#获取当前时间并转化为str，输出格式：HH:MM:SS
		
		msgData = {}
		for key in data:
			if(key == "BasicInf"):
				msgData["活跃国债"]=data["BasicInf"]
			elif(key == "FinancialBond"):
				msgData["活跃金融债"]=data["FinancialBond"]
		
		
		msg = self.Create_Msg(msgData)		#合成html版邮件
		
		sub = datetime.datetime.strftime(datetime.datetime.now(),"%Y/%m/%d")#生成邮件标题
		sub = sub + "活跃债券日报"
		
		s = sendmessage.Mail()
		s.Send_Mail(sub,msg)	#发送邮件
	
	def Send_HighProfit_Bond(self):
		alertRate = self.alertRate
		db = self.db
		data = db.Query_ActiveBond()	#获取活跃债券
		
		dataValue = []
		for key in data:
			for bond in data[key]:				
				if((bond["PROFIT_RATE"]>alertRate or bond["PROFIT_RATE"]==alertRate) and bond["ALREADY_ALERT"]=="否"):		#当前到期收益率大于预警收益率，且未预警
					record = {"BOND_CODE":bond["BOND_CODE"],"CLOSE_PRICE":bond["CLOSE_PRICE"],"PROFIT_RATE":bond["PROFIT_RATE"],"END_DATE":bond["END_DATE"]}
					dataValue = dataValue + [record]
					bond["ALREADY_ALERT"] = "是"		#更新预警状态
					result = db.Update_BondInf_Record(bond,key)
					if(result != True):
						return "翻转预警状态时失败！"
								
		if(len(dataValue) != 0):
			data = {"达到预警利率的债券":dataValue}
			msg = self.Create_Msg(data)		#合成html版邮件
			s = sendmessage.Mail()
			s.Send_Mail("高利率债券预警报告",msg)	#发送邮件


print("欢迎进入债券预警系统！")	
s = input("是否启动进程？y/n >>")

if(s=='y' or s == 'Y' or s == "是"):
	while(1):
		strNow =datetime.datetime.strftime(datetime.datetime.now(),"%H:%M:%S")	#获取当前时间并转化为str，输出格式：HH:MM:SS
		print(strNow + ":主进程持续运行中")
		if(strNow > "09:30:00" and strNow < "15:00:00"):		#在交易时间启动线程
			thread1 = Thread_Control("Update_Bond")
			thread2 = Thread_Control("UserService")
			thread1.start()
			thread2.start()
			thread1.join()
			thread2.join()
			print ("退出主线程")
		time.sleep(600)
else:
	sys.exit()











