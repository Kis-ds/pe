from stqdm import stqdm
from streamlit_option_menu import option_menu
import streamlit as st
import pandas as pd
import numpy as np
import OpenDartReader
import warnings
import dart_fss
import time, datetime
import re
import os

warnings.filterwarnings('ignore')

## 01. functions
def get_data(dart, code, year, quarter):
    select_cols = ['corp_cls', 'corp_name', 'inv_prm', 'frst_acqs_de', 'invstmnt_purps', 'frst_acqs_amount', 'trmend_blce_qy', 'trmend_blce_qota_rt', 'trmend_blce_acntbk_amount']
    change_cols = ['법인구분', '회사명', '법인명', '최초취득일자', '출자목적', '최초취득금액', '기말잔액수량', '기말잔액지분율', '기말잔액장부가액']
    change_cls = {"Y":"유가", "K":"코스닥", "N":"코넥스", "E":"기타"}
    
    change_dict = {"1분기보고서": 11013, "반기보고서": 11012, "3분기보고서": 11014, "사업보고서":11011}
    r_code = change_dict[quarter]
    
    invst_df = dart.report(code, '타법인출자', year, r_code)
    
    if invst_df.shape[0] == 0:
        return invst_df
    else:
        invst_df = invst_df.loc[:, select_cols]
        invst_df.corp_cls = invst_df.corp_cls.map(change_cls)
        invst_df.columns = change_cols
        return invst_df

@st.cache_data
def convert_df(df, encode_opt = False):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    if encode_opt:
        return df.to_csv().encode('CP949')
    else:
        return df.to_csv()

def main(year, r_code):
    
    ## SOURCE CODE
    api_key = 'f54ffa528f132c33a392456a76438073353fa2b5'
    dart = OpenDartReader(api_key)
    dart_fss.set_api_key(api_key=api_key)

    corp_dict = dart_fss.api.filings.get_corp_code()
    corp_df = pd.DataFrame(corp_dict)
    corp_df = corp_df.loc[corp_df.stock_code.notnull()]
    corp_df.index = [x for x in range(corp_df.shape[0])]
    
    # C=발행공시, D=지분공시
    market_dict = {"Y": "코스피","K": "코스닥", "N": "코넥스", "E": "기타"}
    q_dict = {"1분기": ['{}-01-01', '{}-03-31'],
              "2분기": ['{}-04-01', '{}-06-30'],
              "3분기": ['{}-07-01', '{}-09-30'],
              "4분기": ['{}-10-01', '{}-12-31']}

    change_dict = {"1분기보고서": 11013, "반기보고서": 11012, "3분기보고서": 11014, "사업보고서":11011}

    loop_list = ['{}분기'.format(x) for x in range(1,5)]
    
    for idx, q in enumerate(loop_list):
        start_dt, end_dt = [x.format(year) for x in q_dict[q]]

        if r_code == "사업보고서":
            temp_df = dart.list(start=start_dt, end=end_dt, kind_detail = 'A001') # 사업
        elif r_code == "반기보고서":
            temp_df = dart.list(start=start_dt, end=end_dt, kind_detail = 'A002') # 반기
        else:
            temp_df = dart.list(start=start_dt, end=end_dt, kind_detail = 'A003') # 분기

        if idx == 0:
            info_df = temp_df
        else:
            info_df = pd.concat([info_df, temp_df])

    corp_list = info_df.loc[info_df.corp_cls.isin(['Y', 'K', 'N'])].corp_name.unique()

    cnt = 0
    p_cnt = 0

    for corp in stqdm(corp_list):
        temp_df = dart.report(corp, '타법인출자', year, change_dict[r_code])
        p_cnt += 1

        if temp_df.shape[0] == 0:
            continue

        elif cnt == 0 and temp_df.shape[0] != 1:
            output = temp_df
            cnt += 1

        else:
            output = pd.concat([output, temp_df])

    output.invstmnt_purps = [x.replace(" ", "").replace("\n", "") for x in output.invstmnt_purps]
    final_df = output.loc[[True if "단순" == x or "단순투자" in x else False for x in output.invstmnt_purps], :]

    select_cols = ['corp_cls', 'corp_name', 'inv_prm', 'frst_acqs_de', 'invstmnt_purps', 'frst_acqs_amount', 'trmend_blce_qy', 'trmend_blce_qota_rt', 'trmend_blce_acntbk_amount']
    change_cols = ['법인구분', '회사명', '법인명', '최초취득일자', '출자목적', '최초취득금액', '기말잔액수량', '기말잔액지분율', '기말잔액장부가액']

    save_df = final_df.loc[:, select_cols]
    save_df.columns = change_cols

    save_df['법인구분'] = save_df['법인구분'].map(market_dict)

    clean_txt = [re.sub(r'\([^)]*\)', '', x) for x in save_df['법인명']]
    clean_txt = [re.sub("㈜", "", x) for x in clean_txt]
    clean_txt = [re.sub("\s", "", x) for x in clean_txt]
    save_df['법인명'] = clean_txt
    check = save_df.loc[save_df['법인명'].isin(list(corp_df.corp_name.unique()))]

    try:
        save_df = check
        save_df.index = [x for x in range(1, save_df.shape[0]+1)]
        st.dataframe(save_df)

        save_df1 = convert_df(save_df)
        save_df1.to_csv('./datasets/ECM_타법인출자-단순투자-{}-{}.csv'.format(year, r_code))
        
        save_df2 = convert_df(save_df, True)
        st.download_button(label="Download", data=save_df2, file_name='ECM_타법인출자-단순투자-{}-{}.csv'.format(year, r_code), mime='text/csv')
        
    except:
        st.write("수집 데이터 없음")