import os
import PyPDF2
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
"""pd的to_sql不能使用pymysql的连接，否则就会直接报错"""

np.set_printoptions(suppress=True)  # 关闭科学计数法
pd.set_option('display.max_columns', None)  # 完整显示控制台打印的列
pd.set_option('display.max_rows', 50)  # 设置控制台打印的行数
pd.set_option('display.width', 1000)  # 设置控制台打印的宽度


def insert_tenpay(filename):
    """filename use full path

    -- 使用mysql workbench自带的import功能会有字符转码的问题，暂时无法克服
    load data infile 'bill_tenpay.csv' into table family.bill_tenpay
    fields terminated by ',' IGNORE 1 LINES;

    -- 获取列名的查询语句
    select COLUMN_NAME
    from information_schema.columns
    where table_name = 'bill_tenpay'
    order by ORDINAL_POSITION asc;
    """
    try:
        df = pd.read_csv(filename, sep=',', skiprows=17, header=None, #skipinitialspace=True,
                         names=['trans_time'
                             , 'trans_type'
                             , 'counterparty'
                             , 'commodity'
                             , 'receipt_expend'
                             , 'amt'
                             , 'pay_method'
                             , 'current_status'
                             , 'trans_id'
                             , 'order_no'
                             , 'remark'])
        df['amt'] = df['amt'].map(lambda x: float(str(x)[1:])) # remove ￥
        df['trans_id'] = df['trans_id'].map(lambda x: str.strip(x)) # remove \t
        df['order_no'] = df['order_no'].map(lambda x: str.strip(x)) # remove \t

        conn = create_engine('mysql://root:root@localhost:3306/family?charset=utf8mb4')
        df.to_sql("bill_tenpay", con=conn, if_exists='append', index=False)
        print("Successfully imported", filename)
    except Exception as e:
        print(filename, e)
    finally:
        conn.dispose()


def insert_alipay(filename):
    """filename use full path"""
    try:
        df = pd.read_csv(filename, sep=',', skiprows=5, skipfooter=7,
                         header=None, encoding='gbk', engine='python',
                         names=['trans_id'
                                , 'order_no'
                                , 'create_time'
                                , 'pay_time'
                                , 'modify_time'
                                , 'origin'
                                , 'trans_type'
                                , 'counterparty'
                                , 'commodity'
                                , 'amt'
                                , 'receipt_expend'
                                , 'trans_status'
                                , 'service_fee'
                                , 'success_refund'
                                , 'remark'
                                , 'fund_status'
                                , 'NaN' ])
        df['trans_id'] = df['trans_id'].map(lambda x: str.strip(x)) # remove \t
        df['order_no'] = df['order_no'].map(lambda x: str.strip(x)) # remove \t
        df.drop(columns='NaN', inplace=True)

        conn = create_engine('mysql://root:root@localhost:3306/family?charset=utf8mb4')
        df.to_sql("bill_alipay", con=conn, if_exists='append', index=False)
        print("Successfully imported", filename)
    except Exception as e:
        print(filename, e)
    finally:
        conn.dispose()


def analysis_pdf(filename):
    """filename use full path"""

    pdf_file = open(filename,'rb')
    pdf_reader = PyPDF2.PdfReader(pdf_file)

    content = ''
    for i in range(len(pdf_reader.pages)):
        content = content + pdf_reader.pages[i].extract_text() + '\n'
        content = content.replace('招商银行信用卡对账单', '\n招商银行信用卡对账单')

    def re_filter(dt):
        return[val for val in dt if re.search(r'^ \d', val)]

    return re_filter(content.split('\n')) # string list


def insert_credit(filename):
    """filename use full path"""
    try:
        bill_list = analysis_pdf(filename)
        print('pdf file read complete')

        temp_list = []
        for i in range(len(bill_list)):

            if len(bill_list[i].split()) == 5: # 利息
                print('处理利息：', bill_list[i])
                trade_date = bill_list[i].split()[0]
                description = bill_list[i].split()[2]
                original_amt = bill_list[i].split()[-2]
                card_no = bill_list[i].split()[-1]
                amt = bill_list[i].split()[-2]
            else:
                trade_date = bill_list[i].split()[0]
                description = ' '.join(bill_list[i].split()[2:-3]) # 描述包含多个空格
                original_amt = bill_list[i].split()[-1]
                card_no = bill_list[i].split()[-2]
                amt = bill_list[i].split()[-3]

            temp_list.append([trade_date,description,amt,card_no,original_amt])

        df = pd.DataFrame(temp_list, columns=['trade_date','description','amt','card_no','original_amt'])
        df.sort_values(by='trade_date', inplace=True, ascending=True)

        conn = create_engine('mysql://root:root@localhost:3306/family?charset=utf8mb4')
        df.to_sql("bill_credit", con=conn, if_exists='append', index=False)
        print("Successfully imported", filename)
    except Exception as e:
        print(filename, e)
    finally:
        conn.dispose()


def test():

    filedir = "F:/bill"

    # bill_tenpay
    insert_tenpay(os.path.join(filedir,"微信支付账单(20150118-20150331).csv"))

    # bill_alipay
    insert_alipay(os.path.join(filedir,"alipay_record_20220822_1906_1.csv"))

    # bill_credit
    insert_credit(os.path.join(filedir,"CreditCardReckoning2023-07.pdf"))
