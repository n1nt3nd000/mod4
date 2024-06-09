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
    df = pd.read_csv(StringIO(data))
    
    # Перетворюємо стовпець Date в тип datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Групуєємо за місяцями та обчислюємо середній курс
    df['Month'] = df['date'].dt.to_period('M')
    monthly_average = df.groupby('Month')['rate'].mean().reset_index()
    
    return monthly_average

# Зчитування та обчислення середнього курсу за місяць з CSV файлу на S3
monthly_average_df = read_csv_and_compute_monthly_average_from_s3(bucket_name, csv_file_key)

# Виведення вмісту DataFrame на консоль
print(monthly_average_df)
