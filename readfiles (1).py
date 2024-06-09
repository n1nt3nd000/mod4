#!/usr/bin/env python
# coding: utf-8

# In[2]:


import boto3
import pandas as pd
from io import StringIO

# Вказуємо ім'я вашого S3 bucket
bucket_name = 'awwsbucket228335'

# Вказуємо ім'я файлу, який потрібно прочитати
file_key = 'currency_rate.csv'

# Створюємо клієнт для S3
s3 = boto3.client('s3')

# Завантажуємо файл з S3
obj = s3.get_object(Bucket=bucket_name, Key=file_key)
data = obj['Body'].read().decode('utf-8')

# Читаємо CSV файл у DataFrame
df = pd.read_csv(StringIO(data))

# Виводимо перші кілька рядків DataFrame
print(df.head())


# In[ ]:




