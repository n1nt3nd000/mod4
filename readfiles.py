#!/usr/bin/env python
# coding: utf-8

# In[3]:


import boto3
import pandas as pd
from io import StringIO

# Ініціалізація клієнта S3
s3 = boto3.client('s3')

# Заміна на ваш S3 bucket name та ключ файлу
bucket_name = 'awwsbucket228335'
csv_file_key = 'currency_rate_2022.csv'

# Функція для зчитування файлу з S3 та обчислення середнього курсу за місяць
def read_csv_and_compute_monthly_average_from_s3(bucket_name, file_key):
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv('currency_rate_2022.csv', usecols=['date', 'rate', 'cc'])
    
    # Перетворюємо стовпець Date в тип datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Розділення даних за тип валюти (USD та EUR)
    usd_df = df[df['cc'] == 'USD']
    eur_df = df[df['cc'] == 'EUR']
    
    # Групуєємо за місяцями та обчислюємо середній курс
    usd_df['Month'] = usd_df['date'].dt.to_period('M')
    eur_df['Month'] = eur_df['date'].dt.to_period('M')
    
    monthly_average_usd = usd_df.groupby('Month')['rate'].mean().reset_index()
    monthly_average_eur = eur_df.groupby('Month')['rate'].mean().reset_index()
    
    return monthly_average_usd, monthly_average_eur

# Зчитування та обчислення середнього курсу за місяць з CSV файлу на S3
monthly_average_usd_df, monthly_average_eur_df = read_csv_and_compute_monthly_average_from_s3(bucket_name, csv_file_key)

# Виведення вмісту DataFrame на консоль
print("Monthly average USD:\n", monthly_average_usd_df)
print("\nMonthly average EUR:\n", monthly_average_eur_df)


# In[ ]:




