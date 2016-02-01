# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 03:09:21 2015

@author: lowellmarinas
"""

'''
'X', 'prn.coverage', 'cust.class.client', 'cust.class',
       'Principal.Code', 'Principal.Name', 'Item.Code', 'Material.Description',
       'Ims.Item.1', 'Ims.Item.2', 'Ims.Item.3', 'Ims.Item.4', 'Prod.Group',
       'Item.Brand', 'Customer.Code', 'Customer.Name', 'Cust..Class..1',
       'Cust..Class..1.Description', 'Cust..Class..2',
       'Cust..Class..2.Description', 'State', 'Transaction.Type', 'Doc.No.',
       'Seq.No.', 'Date.error', 'Reason.Code', 'Ref.Invoice', 'Ship.To.No.',
       'Ship.To.Name', 'Ship.To.Address.1', 'Ship.To.Address.2',
       'Ship.To.Address.3', 'Approval.No.', 'Branch.Code', 'Cust..Channel',
       'Cust..Channel.Name', 'Salesman.Code', 'Salesman.Name', 'Com.Qty',
       'Fg.Qty', 'Sales.Cost.Value', 'Return.Qty.', 'Ret.Fg',
       'Return.Cost.value', 'X...Unit.Selling.Price', 'X...Unit.List.Price',
       'Unit.overrided.price', 'X...Gross.Sales', 'X...Discount.Value',
       'X...Invoice.Sales', 'X...Net.Sales', 'X...Net.Sales.W.Tax',
       'Local.Group.1', 'Local.Group.1.Description', 'Local.Group.2',
       'Local.Group.2.Description', 'Local.Group.3',
       'Local.Group.3.Description', 'year', 'month', 'Ims.Item.2.Full',
       'Channels', 'month_year'
'''

'''
'X', 'prn.coverage', 'cust.class.client', 'cust.class',
       'Principal.Code', 'Principal.Name', 'Item.Code', 'Material.Description',
       'Ims.Item.1', 'Ims.Item.2', 'Ims.Item.3', 'Prod.Group',
       'Item.Brand', 'Customer.Code', 'Customer.Name','Transaction.Type',
       'Ship.To.Name', 'Ship.To.Address.1', 'Ship.To.Address.2',
       'Ship.To.Address.3','Com.Qty',
       'Fg.Qty', 'Sales.Cost.Value', 'Return.Qty.', 'Ret.Fg',
       'Return.Cost.value', 'X...Unit.Selling.Price', 'X...Unit.List.Price',
       'Unit.overrided.price','X...Net.Sales', 'X...Net.Sales.W.Tax',
       'Local.Group.1', 'Local.Group.1.Description', 'Local.Group.2',
       'Local.Group.3','year', 'month', 'month_year'
'''


import os
import pandas as pd
import numpy as np

os.chdir('/Users/lowellmarinas/Google Drive/Zuellig Pharma/Solutions/Data Analytics/Data/ZPTH')
df = pd.read_csv('zpth_msd_final4.csv',
                 dtype={'Item.Code': 'object',
                        'Postal.Code':'object',
                        'Date.full':'str',
                        'Customer.Code':'object'},
                encoding='latin-1')
#df['Date.full'].astype('string_')
df['month_year']=df['Date.full'].astype('|S6')





TS_zpth = pd.pivot_table(df, index=['Postal.Code', 'Province', 'District', 'prn.coverage',
       'cust.class.client', 'cust.class', 'Customer.Code', 'Principal.Code',
       'Principal.Name', 'Item.Code', 'Material.Description', 'Ims.Item.1',
       'Ims.Item.2', 'Ims.Item.3', 'Ims.Item.4', 'Prod.Group', 'Item.Brand',
       'Customer.Name', 'Transaction.Type', 
       'Ship.To.Name', 'Ship.To.Address.1', 'Ship.To.Address.2',
       'Ship.To.Address.3', 'Cust..Channel', 'Salesman.Code', 'Salesman.Name','Local.Group.1', 'Local.Group.1.Description', 'Local.Group.2',
       'Local.Group.3','Ims.Item.2.Full', 'Channels','State.Country'],
                        values=['Com.Qty', 'Fg.Qty', 'Sales.Cost.Value', 'Return.Qty.', 'Ret.Fg',
       'Return.Cost.value','X...Gross.Sales', 'X...Discount.Value',
       'X...Invoice.Sales', 'X...Net.Sales', 'X...Net.Sales.W.Tax',
                        'X...Unit.Selling.Price','X...Unit.List.Price', 'Unit.overrided.price'],
                        columns='month_year',aggfunc=[np.sum,np.mean],fill_value=0)
stacked = TS_zpth.stack()

stacked.to_csv('TimeSeriesZPTHv2.csv')
stacked = pd.read_csv('TimeSeriesZPTHv2.csv',
                      dtype={'Principal.Code': 'object',
                      'Item.Code': 'object',
                      'month_year': 'string_',
                      'Customer.Code': 'object'},
                      skiprows=2)                      
stacked = stacked.drop(stacked.columns[[45,46,47,48,49,50,51,52,53,54,55,56,57,58]],axis=1)                      
stacked.columns=['Postal.Code', 'Province', 'District', 'prn.coverage',
       'cust.class.client', 'cust.class', 'Customer.Code', 'Principal.Code',
       'Principal.Name', 'Item.Code', 'Material.Description', 'Ims.Item.1',
       'Ims.Item.2', 'Ims.Item.3', 'Ims.Item.4', 'Prod.Group', 'Item.Brand',
       'Customer.Name', 'Transaction.Type', 
       'Ship.To.Name', 'Ship.To.Address.1', 'Ship.To.Address.2',
       'Ship.To.Address.3', 'Cust..Channel', 'Salesman.Code', 'Salesman.Name','Local.Group.1', 'Local.Group.1.Description', 'Local.Group.2',
       'Local.Group.3','Ims.Item.2.Full', 'Channels','State.Country',
       'month_year',
       'Com.Qty', 'Fg.Qty', 'Sales.Cost.Value', 'Return.Qty.', 'Ret.Fg',
       'Return.Cost.value','X...Gross.Sales', 'X...Discount.Value',
       'X...Invoice.Sales', 'X...Net.Sales', 'X...Net.Sales.W.Tax',
                        'X...Unit.Selling.Price','X...Unit.List.Price', 'Unit.overrided.price']                      
stacked['Year']=stacked['month_year'].str[2:6]
stacked['Month']=stacked['month_year'].str[6:8]
stacked['Date']=stacked['Year']+"-"+stacked['Month']+"-01"
stacked['Date']=pd.to_datetime(stacked['Date'])
stacked.drop('month_year',axis=1,inplace=True)
stacked.drop('Year',axis=1,inplace=True)
stacked.drop('Month',axis=1,inplace=True)

cols = stacked.columns.tolist()
cols = cols[-1:]+cols[:-1]
stacked = stacked[cols]

stacked.to_csv('TimeSeriesZPTHv2.csv',index=False)


##Backup
TS_zpth = pd.pivot_table(df, index=['Item.Code','Customer.Code'],
                        values=['Com.Qty.','Fg.Qty.','Return.Qty.','Return.Fg',
                        'Sales.Cost.Value', 'Return.Cost.value','X...Net.Sales', 'X...Net.Sales.W.Tax',
                        'X...Unit.Selling.Price','X...Unit.List.Price', 'Unit.overrided.price'],
                        columns='month_year',
                        aggfunc=[np.sum,np.mean],fill_value=0)
stacked = TS_zpth.stack()

stacked.to_csv('TimeSeriesZPTHv1.csv')
stacked = pd.read_csv('TimeSeriesZPTHv1.csv',
                      dtype={'Item.Code': 'object',
                             'Customer.Code': 'object'},
                             skiprows=2)
stacked = stacked.drop(stacked.columns[[8,9,10,11,12,13,14,15,16,17,18]],axis=1)

stacked.columns=['Item.Code','Cust..Code','month_year','Sales.Qty.','Sales.Bonus.Qty.','Return.Qty.','Return.Bonus.Qty.',
                        'Sales.Cost.Value', 'Return.Cost.value','Gross.Sales..Ex.Tax.','Gross.Sales..Inc.Tax.',
                        'Unit.Sell.Price','Unit.List.Price', 'Unit.overrided.price']
stacked['Year']=stacked['month_year'].str[2:6]
stacked['Month']=stacked['month_year'].str[6:8]
stacked['Date']=stacked['Year']+"-"+stacked['Month']+"-01"
stacked['Date']=pd.to_datetime(stacked['Date'])
stacked.drop('month_year',axis=1,inplace=True)
stacked.drop('Year',axis=1,inplace=True)
stacked.drop('Month',axis=1,inplace=True)

cols = stacked.columns.tolist()
cols = cols[0:2]+cols[-1:]+cols[2:-1]
stacked = stacked[cols]

stacked.to_csv('TimeSeriesZPTHv1.csv',index=False)