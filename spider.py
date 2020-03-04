import urllib.request
import json
import time
import re

class SSESpider:
	def __init__(self):
		self.nationalBondListUrl ="http://query.sse.com.cn/commonQuery.do?sqlId=COMMON_BOND_XXPL_ZQXX_L&BONDTYPE=%E8%AE%B0%E8%B4%A6%E5%BC%8F%E5%9B%BD%E5%80%BA"
			#该url返回国债列表基本信息
		self.BondUrl = "http://query.sse.com.cn/commonQuery.do?sqlId=COMMON_BOND_XXPL_ZQXX_L&BONDCODE=%s"
			#返回单个债券基本信息，该url为单个债券的前缀url，查询时需添加债券代码 格式：queryUrl = BondUrl % 债券代码
		self.BondTradeInfUrl = "http://query.sse.com.cn/security/fund/queryAllQuatAbelNew.do?FUNDID=%s&searchDate=%s"
			#返回债券成交概况。查询时需添加债券代码、查询日期 格式：queryUrl = BondTradeInfUrl % (债券代码,查询日期YYYY-MM-DD)
		self.BondPriceLine = "http://yunhq.sse.com.cn:32041//v1/sh1/line/%s?begin=0&end=-1&select=time%2Cprice%2Cvolume"
			#此url返回债券的【时间、实时价格、成交量】。查询时需添加债券代码 格式：queryUrl = BondPriceLine % 债券代码
	def FinancialBond_list(self):
		url = "http://query.sse.com.cn/commonQuery.do?sqlId=COMMON_BOND_XXPL_ZQXX_L&BONDTYPE=%E6%99%AE%E9%80%9A%E9%87%91%E8%9E%8D%E5%80%BA"
		pageSource = Get_Resource(url)
		items = json.loads(pageSource)
		return items["result"]
		
	def NationalBond_List(self):		#该函数返回国债列表
		url = self.nationalBondListUrl
		pageSource = Get_Resource(url)
		items = json.loads(pageSource)
		return items["result"]
	
	def Day_Price(self,queryCode,queryDate=""):	#该函数返回当个证券每日成交量、价格等信息
		url =self.BondTradeInfUrl % (queryCode,queryDate)
		pageSource = Get_Resource(url)
		items = json.loads(pageSource)
		for item in items["result"]:
			print(item)
			print("=="*60)
	
	def Close_TimePriceAmt(self,queryCode):		#返回该只证券最新交易价格、交易时间、交易数量
		url =self.BondPriceLine.replace(r"line/%s?","line/" + queryCode + "?")
		#print("=="*60)
		#print(url)
		try:
			pageSource = Get_Resource(url)
		except:
			return ["-","-","-"]
		else:
			# print(pageSource)
			items = json.loads(pageSource)
			i = len(items["line"])-1
			while(i):
				i = i-1
				if(items["line"][i][2] != 0):
					return items["line"][i]
			return items["line"][len(items["line"])-1]
	
def Get_Resource(queryUrl):
	Cookie = ""			#添加cache
	headers = {			#构造请求头
		'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
		'Cookie': Cookie,
		'Connection': 'keep-alive',
		'Accept': '*/*',
		'Accept-Encoding': 'gzip, deflate, sdch',
		'Accept-Language': 'zh-CN,zh;q=0.8',
		'Host': 'query.sse.com.cn',
		'Referer': 'http://www.sse.com.cn/assortment/bonds/list/'
	}
	
	req = urllib.request.Request(queryUrl,None,headers)
	response = urllib.request.urlopen(req,timeout=3)
	pageResource = response.read().decode('utf8')
	return pageResource

def test():
	sse =SSESpider()
	#daypriceList = sse.Day_Price('019315','2020-2-16')
	print(sse.Close_TimePriceAmt('019315'))
	#sse.FinancialBond_list()
	
#test()
