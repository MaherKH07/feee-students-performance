#!/usr/bin/env python
# coding: utf-8

# In[13]:


#import the libraries
import pandas as pd
import sqlite3


# In[14]:


#Establish a Connection to the databse file
conn = sqlite3.connect(r'C:\Users\Shift\Desktop\work\Data Analysis\students_marks\FEEE_V2.db')


# In[32]:


#Execute SQL Queries and Retrieve Data into DataFrames
query1 = "SELECT * FROM ONE"
query2 = "SELECT * FROM TWO"
query3 = "SELECT * FROM THREE"
query4 = "SELECT * FROM FOUR"
query5 = "SELECT * FROM FIVE"

df_one = pd.read_sql_query(query1, conn)
df_two = pd.read_sql_query(query2, conn)
df_three = pd.read_sql_query(query3, conn)
df_four = pd.read_sql_query(query4, conn)
df_five = pd.read_sql_query(query5, conn)


# In[43]:


# Create a clean copy of df
df_clean = df_three.copy()

# Drop the 'Rate' column
df_clean = df_clean.drop('Rate', axis=1)

# Define a mapping dictionary for status values to be cleaned
text_mapping = {
    'ناجح سابقا': 'Exempt',
    'ناجح  سابق':'Exempt',
    'ناجحة سابقا':'Exempt',
    'ناجح سابقاً':'Exempt',
    'ناجح بالمقرر':'Exempt',
    'ناجح(تنزيل) ':'Exempt',
    'تنزيل ':'Exempt',
    'تنزيل  سابقا':'Exempt',
    'تنزيل عام سايق':'Exempt',
    'راسبة':'delete',
    'تنزيل(ناجح)':'Exempt',
    'ناجح': 'Pass',
    'خطــــــأ': 'Pass',
    'تنزيل عام سابق':'Exempt',
    'تنزيل مسبق':'Exempt',
    'تنزيل سابق': 'Exempt',
    'نظم':'delete',
    'رابع نظم الكترونية':'delete',
    'نظم الكترون':'delete',
    'تنزيل': 'Exempt',
    'محروم ': 'Debarred',
    ' محروم':'Debarred',
    ' مرسوم 125-محروم':'Debarred',
    'محروم': 'Debarred',
    '.محروم':'Debarred',
    'حرمان':'Debarred',
    'رابع':'delete',
    'سنة ثالثة':'delete',
    'لا يوجد كنية':'delete',
    'رايع':'delete',
    'ملغى':'delete',
    'أول راسب':'delete',
    'راسب ثالث':'delete',
    'ثالث راسب':'delete',
    'ثاني راسب':'delete',
    'راسب ثاني(ايقاف)':'Withdrawal',
    'راسب أول':'delete',
    'راسب ثاني':'delete',
    'راسبة ثاني':'delete',
    'راسب': 'Fail',
    '45`':'Fail',
    'صفر':'Fail',
    'غائب ':'Absent',
    'غساب':'Absent',
    'غائب': 'Absent',
    'غايب': 'Absent',
    'إعادة': 'Absent',
    'غياب': 'Absent',
    'غا': 'Absent',
    'غ': 'Absent',
    'p': 'Fail',
    'إيقاف فصل أول':'Withdrawal',
    'ايقاف فصل ثاني':'Withdrawal',
    'ايقاف تسجيل':'Withdrawal',
    'تحويل متماثل':'Transfer',
    'إيقاف ف1':'Withdrawal',
    'إيقاف ف2 ':'Withdrawal',
    'إيقاف عام ':'Withdrawal',
    'إيقاف عام': 'Withdrawal',
    'تغير قيد':'Withdrawal',
    'تغيير قيد':'Withdrawal',
    'ترقين قيد': 'Withdrawal',
    'ايقاف عام': 'Withdrawal',
    'إيقاف ف2':'Withdrawal',
    'إيقااف ف2': 'Withdrawal',
    'إيقاف تسجيل': 'Withdrawal',
    'إيقاف ثاني': 'Withdrawal',
    'ايقاف تسجيل ف2':'Withdrawal',
    'ايقاف فصل أول':'Withdrawal',
    'إيقاف فصل اول':'Withdrawal',
    'إيقاف فصل 1': 'Withdrawal',
    'إيقاف فصل ثاني': 'Withdrawal',
    'إيقاف تسجيل فصل ثاني':'Withdrawal',
    'إيقاف ':'Withdrawal',
    'ايقاف فصل':'Withdrawal',
    'ايقاف': 'Withdrawal',
    'إيقاف': 'Withdrawal',
    'اوائل معاهد': 'Exempt',
    'ناجح سابقاً': 'Exempt',
    'سنة ثانية': 'delete',
    'حجب ':'Withhold',
    'حجب': 'Withhold',
    'مراجعة' : 'Withhold',
    'تحويل مماثل':'Transfer',
    'تحويل الى تشرين':'Transfer',
    'تحويل إلى البعث':'Transfer',
    'تحويل إلى تشرين': 'Transfer',
    'تحويل مماثل سنة2': 'Transfer',
    'تحويل':'Transfer',
    'الى دمشق':'Transfer',
    'نقل لدمشق':'Transfer',
    'نقل  دمشق':'Transfer',
    'نقل افتراضية':'Transfer',
    'نقل البعث':'Transfer',
    'نقل الى دمشق':'Transfer',
    'نقل إلى تشرين':'Transfer',
    'نقل إلى دمشق':'Transfer',
    'نقل بعث':'Transfer',
    'نقل تشربن':'Transfer',
    'نقل تشرين':'Transfer',
    'نقل حمص':'Transfer',
    'نقل دمشق':'Transfer',
    'نقل ':'Transfer',
    'نقل':'Transfer',
    'منقول':'Transfer',
    'حرمان دورتين':'Suspended',
    'حرمان ثلاث دورات':'Suspended',
    'حرمان 3 دورات':'Suspended',
    'لم يسجل - محروم':'Withdrawal',
    'لم يسجل -محروم':'Withdrawal',
    'لم يسجل-محروم':'Withdrawal',
    'غير مسجل':'Withdrawal',
    'لم تسجل':'Withdrawal',
    'لم يسجل':'Withdrawal',
    'منقطعة':'Withdrawal',
    'سيق الى الخدمة الالزامية':'delete',
    'قسم تحكم':'delete',
    'قسم تحكم ':'delete',
}

# Define a mapping dictionary for numeric Columns to be cleaned
numeric_mapping = {
    'ناجح سابقا': '0',
    'ناجح  سابق':'0',
    'ناجحة سابقا':'0',
    'ناجح سابقاً':'0',
    'ناجح بالمقرر':'0',
    'ناجح(تنزيل) ':'0',
    'تنزيل ':'0',
    'تنزيل  سابقا':'0',
    'تنزيل عام سايق':'0',
    'راسبة':'0',
    'تنزيل(ناجح)':'0',
    'ناجح': '0',
    'خطــــــأ': '0',
    'تنزيل عام سابق':'0',
    'تنزيل مسبق':'0',
    'تنزيل سابق': '0',
    'نظم':'0',
    'رابع نظم الكترونية':'0',
    'نظم الكترون':'0',
    'تنزيل': '0',
    'محروم ': '0',
    ' محروم':'0',
    ' مرسوم 125-محروم':'0',
    'محروم': '0',
    '.محروم':'0',
    'حرمان':'0',
    'رابع':'0',
    'سنة ثالثة':'0',
    'لا يوجد كنية':'0',
    'رايع':'0',
    'ملغى':'0',
    'أول راسب':'0',
    'راسب ثالث':'0',
    'ثالث راسب':'0',
    'ثاني راسب':'0',
    'راسب ثاني(ايقاف)':'0',
    'راسب أول':'0',
    'راسب ثاني':'0',
    'راسبة ثاني':'0',
    'راسب': '0',
    '45`':'0',
    'صفر':'0',
    'غائب ':'0',
    'غساب':'0',
    'غائب': '0',
    'غايب': '0',
    'إعادة': '0',
    'غياب': '0',
    'غا': '0',
    'غ': '0',
    'p': '0',
    'إيقاف فصل أول':'0',
    'ايقاف فصل ثاني':'0',
    'ايقاف تسجيل':'0',
    'تحويل متماثل':'0',
    'إيقاف ف1':'0',
    'إيقاف ف2 ':'0',
    'إيقاف عام ':'0',
    'إيقاف عام': '0',
    'تغير قيد':'0',
    'تغيير قيد':'0',
    'ترقين قيد': '0',
    'ايقاف عام': '0',
    'إيقاف ف2':'0',
    'إيقااف ف2': '0',
    'إيقاف تسجيل': '0',
    'إيقاف ثاني': '0',
    'ايقاف تسجيل ف2':'0',
    'ايقاف فصل أول':'0',
    'إيقاف فصل اول':'0',
    'إيقاف فصل 1': '0',
    'إيقاف فصل ثاني': '0',
    'إيقاف تسجيل فصل ثاني':'0',
    'إيقاف ':'0',
    'ايقاف فصل':'0',
    'ايقاف': '0',
    'إيقاف': '0',
    'اوائل معاهد': '0',
    'ناجح سابقاً': '0',
    'سنة ثانية': '0',
    'حجب ':'0',
    'حجب': '0',
    'مراجعة' : '0',
    'تحويل مماثل':'0',
    'تحويل الى تشرين':'0',
    'تحويل إلى البعث':'0',
    'تحويل إلى تشرين': '0',
    'تحويل مماثل سنة2': '0',
    'تحويل':'0',
    'الى دمشق':'0',
    'نقل لدمشق':'0',
    'نقل  دمشق':'0',
    'نقل افتراضية':'0',
    'نقل البعث':'0',
    'نقل الى دمشق':'0',
    'نقل إلى تشرين':'0',
    'نقل إلى دمشق':'0',
    'نقل بعث':'0',
    'نقل تشربن':'0',
    'نقل تشرين':'0',
    'نقل حمص':'0',
    'نقل دمشق':'0',
    'نقل ':'0',
    'نقل':'0',
    'منقول':'0',
    'حرمان دورتين':'0',
    'حرمان ثلاث دورات':'0',
    'حرمان 3 دورات':'0',
    'لم يسجل - محروم':'0',
    'لم يسجل -محروم':'0',
    'لم يسجل-محروم':'0',
    'غير مسجل':'0',
    'لم تسجل':'0',
    'لم يسجل':'0',
    'منقطعة':'0',
    'سيق الى الخدمة الالزامية':'0',
    'قسم تحكم':'0',
    'قسم تحكم ':'0',
}

# Replace the values in 'Status' column
df_clean['Status'] = df_clean['Status'].replace(text_mapping)

# Replace specific values in 'OfficalYear' column
df_clean['OfficalYear'] = df_clean['OfficalYear'].str.replace('2022-2021', '2021-2022')
df_clean['OfficalYear'] = df_clean['OfficalYear'].str.replace('2021-2020', '2020-2021')

# Replace values in 'TotalInt' column
df_clean['TotalInt'] = df_clean['TotalInt'].replace(numeric_mapping)

# Replace missing values in 'TotalInt' column with '0'
df_clean['TotalInt'] = df_clean['TotalInt'].fillna('0')

# Replace values in 'Theoretical' column 
df_clean['Theoretical'] = df_clean['Theoretical'].replace(numeric_mapping)

# Replace missing values in 'Theoretical' column with '0'
df_clean['Theoretical'] = df_clean['Theoretical'].fillna('0')

# Replace values in 'Practical' column 
df_clean['Practical'] = df_clean['Practical'].replace(numeric_mapping)

# Replace missing values in 'Practical' column with '0'
df_clean['Practical'] = df_clean['Practical'].fillna('0')


# In[44]:


#sort the dataframe in order to drop the empty rows
df_clean = df_clean.sort_values(by = 'TotalInt', ascending = True)

#reset the index in order to know what rows to drop
df_clean = df_clean.reset_index(drop=True)

#define a criteria to drop rows
criteria = df_clean['TotalInt'] == 'delete'

#drop wrong rows
df_clean = df_clean.drop(df_clean[criteria].index)

#drop empty rows
df_clean = df_clean.drop(df_clean.index[0:2099])

#reset the index to be normal again
df_clean.reset_index(drop=True)
    
#([a-zA-Z]{1,2}): captures two or two consecutive alphabetic characters (department prefix)
#(\d+): captures two or more consecutive digits (ID)
df_clean[['Department', 'ID']] = df_clean['ID'].str.extract(r'([a-zA-Z]{1,2})(\d+)')

#create a new list contains the new order of the columns
new_order = ['ID','Department','Subject','Semester','Practical','Theoretical','TotalInt','Status','OfficalYear']

#reorgnizing the dataframe
df_clean = df_clean.reindex(columns=new_order)

#reset the index becuase it is not reseted
df_clean = df_clean.reset_index(drop=True)

#make department prefixes all upper case 
df_clean['Department']=df_clean['Department'].str.upper()

#rename TotalInt column
df_clean.rename(columns={'TotalInt': 'Total'}, inplace=True)

df_clean.to_csv('C:/Users/Shift/Desktop/work/Data Analysis/students_marks/CSVs/two_clean.csv', encoding='utf-8-sig', index=True)

