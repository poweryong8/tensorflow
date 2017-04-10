#-*- coding: utf-8 -*-
#------------------------기본 설정 및 모듈 import-----------------------------------------------------------------------
import datetime

class DauCount:

    def __init__(self,year,month,day,startnum,endnum):
        self.year = year
        self.month = month
        self.day = day
        self.startnum = startnum
        self.endnum =endnum

    def makeDate(self):
        startdate = datetime.date(self.year, self.month, self.day)
        dateData = []
        for i in range(self.startnum, self.endnum,1):
            date1 = startdate + datetime.timedelta(days=i)
            date2 = date1.strftime('%Y%m%d')
            dateData.insert(i, date2)
        return (dateData)

    def makeDateX(self):
        startdate = datetime.date(self.year, self.month, self.day)
        dateData1 = []
        for i in range(self.startnum, self.endnum, 1):
            date1 = startdate + datetime.timedelta(days=i)
            date2 = date1.strftime('%Y-%m-%d')
            dateData1.insert(i, date2)
        return (dateData1)
