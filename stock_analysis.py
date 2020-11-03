# -*- coding: utf-8 -*-
# from __future__ import division
import pandas as pd
import numpy as np
import tushare as ts
import datetime
import time
import os
import csv
import pymysql
import warnings
import random
warnings.filterwarnings("ignore")

class Demo():
	def get_today_all_info(self):
		all_info = ts.get_today_all()
		# all_info.to_csv("F:\\Project\\Demo\\data\\all_info.csv")
		for stock in all_info.itertuples():
			code = stock[1]
			name = stock[2]
			changepercent = stock[3]
			trade = stock[4]
			sql = "select count(1) from zhijia.stock_today_detail where code = \'" + str(code) + "\' ;"
			cursor.execute(sql)
			results = cursor.fetchone()
			isexist = results[0]
			if isexist == 0:
				sql = "insert into zhijia.stock_today_detail values(null, " + str(code) + ", \'" + str(name) + "\', " + str(changepercent) + ", " + str(trade) + ", 1, 0, now()) ;"
				cursor.execute(sql)
			else:
				sql = "update zhijia.stock_today_detail set activate = 1, changepercent = " + str(changepercent) + ", trade = " + str(trade) + ", validdate = now() where code = \'" + str(code) + "\' ;"
				cursor.execute(sql)
			# sql = "INSERT INTO zhijia.stock_history_detail VALUES (NULL, CODE, NAME, changepercent, trade, OPEN, high, low, settlement, validdate)"
			# cursor.execute(sql)
		sql = "select code from zhijia.stock_today_detail order by changepercent desc limit 10 ;"
		cursor.execute(sql)
		results = cursor.fetchall()
		code_list = []
		for row in results:
			code = row[0]
			code_list.append(code)
		return code_list

	def queryDetail(self, code):
		DetailINFO = ts.get_hist_data(code, '2019-01-01', '2020-12-31')
		DetailINFO = DetailINFO.iloc[:,:5].sort_index()
		DetailINFO.index = pd.to_datetime(DetailINFO.index, format='%Y-%m-%d')
		df1 = DetailINFO
		ndate = len(df1)
		periodHigh = pd.Series(np.zeros(ndate - 8), index = df1.index[8:])
		periodLow = pd.Series(np.zeros(ndate - 8), index=df1.index[8:])
		RSV = pd.Series(np.zeros(ndate - 8), index=df1.index[8:])
		close = df1.close
		high = df1.high
		low = df1.low
		date = close.index.to_series()
		date[date.index.duplicated()]
		for j in range(3, ndate):
			period = date[j-3:j+1]
			i = date[j]
			periodHigh[i] = high[period].max()
			periodLow[i] = low[period].min()
			RSV[i] = 100*(close[i] - periodLow[i]) / (periodHigh[i] - periodLow[i])
			periodHigh.name = 'periodHigh'
			periodLow.name = 'periodLow'
			RSV.name = 'RSV'
		RSV1 = pd.Series([50, 50], index = date[1:3]).append(RSV)
		RSV1.name = 'RSV'
		# for i, v in RSV1.items():
		# 	print('index: ', i, 'value: ', v)
		KValue = pd.Series(0.0, index = RSV1.index)
		KValue[0] = 50
		for i in range(1, len(RSV1)):
			KValue[i] = 2/3*KValue[i-1] + RSV1[i]/3
		KValue.name = 'KValue'
		DValue = pd.Series(0.0, index = RSV1.index)
		DValue[0] = 50
		for i in range(1, len(RSV1)):
			DValue[i] = 2/3*DValue[i-1] + KValue[i]/3
		KValue = KValue[1:]
		DValue.name = 'DValue'
		DValue = DValue[1:]
		date[date.index.duplicated()]
		closedf = close.to_frame()
		KValuedf = KValue.to_frame()
		DValuedf = DValue.to_frame()
		data = pd.DataFrame()
		data['close'] = closedf['close']
		data['k'] = KValuedf['KValue']
		data['d'] = DValuedf['DValue']
		for tup in data.itertuples():
			index = tup[0]
			k = tup[2]
			d = tup[3]
			j = (3 * k) - (2 * d)
			data.loc[index, 'j'] = j
		detailDF = data.tail(100)
		# for i in data.itertuples():
		# 	print (i)
		self.Analysis(code, detailDF)

	def Analysis(self, code, detailDF):
		# flag = -1
		param = random.randint(0, 99)
		print(param)
		index = detailDF.index[param]
		print (detailDF.iloc[param])
		close = detailDF.iloc[param, 0]
		k = detailDF.iloc[param, 1]
		d = detailDF.iloc[param, 2]
		j = detailDF.iloc[param, 3]
		# 止损指标
		param2 = 1.15
		# db = pymysql.connect(host="172.17.0.3", port=3306, user="root", passwd="root", db="zhijia", charset='utf8')
		db = pymysql.connect(host="192.168.146.128", port=50001, user="root", passwd="root", db="zhijia", charset='utf8')
		cursor = db.cursor()
		sql = "select count(1) from zhijia.stock_buy_sell where code = \'" + code + "\' ;"
		cursor.execute(sql)
		results = cursor.fetchone()
		isexist = results[0]
		if isexist == 0:
			sql = "insert into zhijia.stock_buy_sell values(null, " + str(code) + " , " + str(close) + ", " + str(k) + ", " + str(d) + ", " + str(j) + ", 10000, 0, 0, null);"
			print (sql)
			cursor.execute(sql)
			db.commit()
		sql = "select flag,money,num from zhijia.stock_buy_sell where code = \'" + str(code) + "\' ;"
		cursor.execute(sql)
		results = cursor.fetchone()
		flag = results[0]
		money = results[1]
		num = results[2]
		if k * param2 > d:
			if flag != 1:
				num = float(money) / float(close)
				money = 0
				sql = "update zhijia.stock_buy_sell set close = " + str(close) + ", k = " + str(k) + ", d = " + str(d) + ", j = " + str(j) + ", money = " + str(money) + ", num = " + str(num) + ", flag = 1 where code = \'" + code + "\' ;"
				print (sql)
				print ("到达买入时间")
			else : 
				print ("买入正常无操作")
		elif k * param2 < d:
			if flag != -1:
				money = float(num) * float(close)
				num = 0
				sql = "update zhijia.stock_buy_sell set close = " + str(close) + ", k = " + str(k) + ", d = " + str(d) + ", j = " + str(j) + ", money = " + str(money) + ", num = " + str(num) + ", flag = -1 where code = \'" + code + "\' ;"
				print (sql)
				print ("到达卖出时间")
			else :
				print ("卖出正常无操作")
		else :
			print ("正常无操作")
		cursor.execute(sql)
		db.commit()
		db.close()
	
if __name__ == '__main__':
	db = pymysql.connect(host="192.168.146.128", port=50001, user="root", passwd="root", db="zhijia", charset='utf8')
	cursor = db.cursor()
	code_list = Demo().get_today_all_info()
	for stock in code_list:
		Demo().queryDetail(stock)
	print (code_list)
	db.commit()
	db.close()

