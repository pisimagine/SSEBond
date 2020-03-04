from scipy.optimize import fsolve
import math
import numpy
import datetime

def Bond_Discount_Rate(price,faceRate,faceValue,payType,todate,endDate):			#计算债券贴现率
#输入参数====================================================
			# price: 	 当前价格（数字）
			# faceRate:  票面利率（字符：百分数）
			# faceValue: 票面价值（数字）
			# payType:   支付方式（字符串：半年付息、一年付息、到期一次还本付息）
			# todate:	 今日日期（字符串：格式YYYY-MM-DD）
			# endDate：	 还本日期（字符串：格式YYYY-MM-DD）
	if(price == "-" or faceRate=="-" or faceValue =="-"):
		discountRate = "-"			#价格为“-”时说明没有交易量,无法计算贴现率，将其赋值为“-”
		return discountRate

	
	price = float(price)		#将价格、利息转化为float形
	faceValue = float(faceValue)
	faceRate = float(faceRate.strip('%'))
	faceRate = faceRate/100.0
	
	endDate = datetime.datetime.strptime(endDate,'%Y-%m-%d').date()
	todate	= datetime.datetime.strptime(todate,'%Y-%m-%d').date()
	
	if(endDate<todate or endDate==todate):
		discountRate = "-"		#已到期债券无法交易，不计算贴现率
		return discountRate
	
	if(payType == "按半年付息"):
		actualFaceRate = math.pow((1 + faceRate/2),2)-1
	elif(payType == "按年付息"):
		actualFaceRate = faceRate
	elif(payType == "到期一次还本付息"):
		actualFaceRate = faceRate
	
	tradeFee = 1		#交易费用，债券为买卖收取1元，到期不收费用	
	diffRate = math.exp((365/(endDate-todate).days)*math.log(faceValue/(price+tradeFee)))-1		#计算债券价差产生的收益率，结果已折算成按年付息的利率
	# print("价差利率为：")
	# print(diffRate)
	# print("票面实际利率为：")
	# print(actualFaceRate)	
	discountRate = actualFaceRate+diffRate
	discountRate = "%.2f%%" % (discountRate * 100)		#转化为百分数
	
	return discountRate

def test():
	todate = datetime.datetime.now().strftime('%Y-%m-%d')
	# print("今天的日期是：" )
	# print(type(todate),todate)
	dr = Bond_Discount_Rate("100",'5.31%','100',"按半年付息",todate,"2063-11-18")
	#price,faceRate,faceValue,payType,startDate,todate,payDate,endDate
	print("贴现利率：")
	print(dr)
	print("=="*60)
	
# test()
