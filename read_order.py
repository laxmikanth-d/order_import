import psycopg2 as ps
import json
import pandas as pd
from config import Config
import os
import boto3


conn = None
params = Config()
conn = ps.connect(**params)


def read_order():

    try:

        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')

        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

        s3 = boto3.client(
            service_name='s3',
            region_name='us-west-2',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        
        files = s3.list_objects_v2(Bucket='download-testing-10202020', Prefix='sample')

        for file in files['Contents']:
            # print(file['Key'])
            if file['Key'].endswith('json'):
                file_name = file['Key']
                with open(file_name,'wb') as f:
                    s3.download_fileobj('download-testing-10202020',file_name,f)

                with open(file_name, 'r') as f:
                    json_data = json.load(f)     

                for order in json_data['orders']:
                    data = order['order']['priceInfo']
                    priceInfo_insert(data)

                copy_source = {
                        'Bucket': 'download-testing-10202020',
                        'Key': file_name
                    }

                s3.copy(CopySource=copy_source,Bucket='archive-orders',Key=file_name)

                s3.delete_object(Bucket='download-testing-10202020',Key='sample_order.json')

        conn.commit()
        conn.close()

    except (Exception, ps.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def priceInfo_insert(data):
    try:
        crsr = conn.cursor()
        crsr.execute("""INSERT INTO priceInfo(amount,total,shipping,currencycode,tax,amountisfinal
                        ,discounted,manualadjustmenttotal,rawsubtotal,discountamount) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                     (data['amount'], data['total'], data['shipping'], data['currencyCode']
                     , data['tax'], data['amountIsFinal'], data['discounted']
                     , data['manualAdjustmentTotal'], data['rawSubtotal']
                     , data['discountAmount']))

    except(Exception, ps.DatabaseError) as error:
        print(error)





if __name__ == "__main__":
    read_order()
