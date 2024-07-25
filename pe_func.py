import streamlit as st
import warnings
import pandas as pd
import numpy as np
import json
import pickle
import requests
from bs4 import BeautifulSoup
import zipfile
from io import BytesIO
import xmltodict
import time

warnings.filterwarnings('ignore')
API_KEY = 'd7d1be298b9cac1558eab570011f2bb40e2a6825'
headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
          'Accept-Encoding': '*', 'Connection': 'keep-alive'}

# 고유번호-회사명 매칭 리스트
def get_corp_dict():
    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    response = requests.get(url, params={'crtfc_key': API_KEY}, verify=False)
    zf = zipfile.ZipFile(BytesIO(response.content))
    file = zf.read('CORPCODE.xml').decode('utf-8')
    data_odict = xmltodict.parse(file)
    data_dict = json.loads(json.dumps(data_odict))
    data = data_dict.get('result', {}).get('list')
    corp_dict = {}
    for i in range(len(data)):
        corp_dict[data[i]['corp_name']] = data[i]['corp_code']
        # corp_nm_list.append(data[i]['corp_name'])
    return corp_dict

# 보고서명, 일자로 검색해서 보고서접수번호 추출(최대 호출 가능기간: 3개월)
def get_rcept_no(report_nm, bgn_de, end_de):
    rcept_no_list = []
    url = 'https://opendart.fss.or.kr/api/list.xml'
    params = {'crtfc_key': API_KEY
        , 'bgn_de': bgn_de
        , 'end_de': end_de
        , 'pblntf_detail_ty': 'B001'
        , 'last_reprt_at': 'Y'}
    try:
        response = requests.get(url, params=params, headers=headers, verify=False)
        soup = BeautifulSoup(response.content, features='html.parser')
        total_page = soup.find('total_page').get_text()

        for i in range(1, int(total_page) + 1):
            params = {'crtfc_key': API_KEY
                , 'bgn_de': bgn_de
                , 'end_de': end_de
                , 'pblntf_detail_ty': 'B001'
                , 'page_no': str(i)
                , 'last_reprt_at': 'Y'}
            response = requests.get(url, params=params, headers=headers, verify=False)
            soup = BeautifulSoup(response.content, features='html.parser')
            for c in soup.find_all('list'):
                if report_nm in c.report_nm.get_text():
                    rcept_no_list.append(c.rcept_no.get_text())
    except Exception as e:
        print(e)
        rcept_no_list = []

    print('보고서명:', report_nm, ', 보고서수: ', len(rcept_no_list))

    return rcept_no_list

# 고유번호, 보고서명, 일자로 검색해서 보고서접수번호 추출
def get_rcept_no_by_corp(corp_code, report_nm, bgn_de, end_de):
    rcept_no_list = []
    url = 'https://opendart.fss.or.kr/api/list.xml'
    params = {'crtfc_key': API_KEY
        , 'corp_code': corp_code
        , 'bgn_de': bgn_de
        , 'end_de': end_de
        , 'pblntf_detail_ty': 'B001'
        , 'last_reprt_at': 'Y'}
    try:
        response = requests.get(url, params=params, headers=headers, verify=False)
        soup = BeautifulSoup(response.content, features='html.parser')
        total_page = soup.find('total_page').get_text()

        for i in range(1, int(total_page) + 1):
            params = {'crtfc_key': API_KEY
                , 'corp_code': corp_code
                , 'bgn_de': bgn_de
                , 'end_de': end_de
                , 'pblntf_detail_ty': 'B001'
                , 'page_no': str(i)
                , 'last_reprt_at': 'Y'}
            response = requests.get(url, params=params, headers=headers, verify=False)
            soup = BeautifulSoup(response.content, features='html.parser')
            for c in soup.find_all('list'):
                if report_nm in c.report_nm.get_text():
                    rcept_no_list.append(c.rcept_no.get_text())
    except Exception as e:
        print(e)
        rcept_no_list = []

    print('보고서명:', report_nm, ', 보고서수: ', len(rcept_no_list))

    return rcept_no_list

# 주요사항보고서(전환,신주인수권, 교환채권) 데이터 호출
def get_mezn_data(knd, corp_nm, start_dt, end_dt, intr_ex_min, intr_ex_max, intr_sf_min, intr_sf_max):
    with open('./pickle/Mezzanine_new.pkl', 'rb') as f:
        df = pickle.load(f)
        df['발행사'] = df['발행사'].str.replace('주식회사', '').str.replace('(주)', '').str.replace('㈜', '').str.replace('(',
                                                                                                              '').str.replace(
            ')', '').str.strip()
        df = df[df['종류'].isin(knd)]
        df['표면이자율(%)'] = df['표면이자율(%)'].str.strip()
        df['만기이자율(%)'] = df['만기이자율(%)'].str.strip()
        df.loc[df['표면이자율(%)'] == '-', '표면이자율(%)'] = -1000
        df.loc[df['만기이자율(%)'] == '-', '만기이자율(%)'] = -1000
        df = df[(((df['표면이자율(%)'].astype(float) >= intr_ex_min) & (df['표면이자율(%)'].astype(float) <= intr_ex_max)) | (
                    df['표면이자율(%)'].astype(float) == -1000))
                & (((df['만기이자율(%)'].astype(float) >= intr_sf_min) & (df['만기이자율(%)'].astype(float) <= intr_sf_max)) | (
                    df['만기이자율(%)'].astype(float) == -1000))]
        if corp_nm == '':
            df = df[(df['공시일'] >= start_dt.strftime('%Y%m%d')) & (df['공시일'] <= end_dt.strftime('%Y%m%d'))]
        else:
            df = df[(df['공시일'] >= start_dt.strftime('%Y%m%d')) & (df['공시일'] <= end_dt.strftime('%Y%m%d'))
                    & (df['발행사'] == corp_nm)]
        df.loc[df['표면이자율(%)'] == -1000, '표면이자율(%)'] = '-'
        df.loc[df['만기이자율(%)'] == -1000, '만기이자율(%)'] = '-'
    return df

# 주요사항보고서(전환,신주인수권, 교환채권) 상세정보 추출
def get_mezn_docu(rcept_no):
    url = 'https://opendart.fss.or.kr/api/document.xml'
    params = {'crtfc_key': API_KEY, 'rcept_no': rcept_no}
    response = requests.get(url, params=params, verify=False)
    time.sleep(1)
    try:
        zf = zipfile.ZipFile(BytesIO(response.content))
        fp = zf.read('{}.xml'.format(rcept_no))
        try:
            xml_str = fp.decode('cp949')
            xml_str = xml_str.replace('<=', '')
            xml = xml_str.encode('cp949')
        except:
            xml_str = fp.decode('utf-8')
            xml_str = xml_str.replace('<=', '')
            xml = xml_str.encode('utf-8')

        soup = BeautifulSoup(xml, features='html.parser')
        doc_nm = '전환사채권' if '전환사채권' in soup.find('document-name').get_text() else (
            '신주인수권부사채권' if '신주인수권' in soup.find('document-name').get_text() else '교환사채권')  # 보고서 종류
        table = soup.find('table-group', attrs={'aclass': 'CB_PUB'}) if doc_nm == '전환사채권' else (
            soup.find('table-group', attrs={'aclass': 'BW_PUB'}) if doc_nm == '신주인수권부사채권' else soup.find('table-group', attrs={'aclass': 'EB_PUB'}))
        company_nm = soup.find('company-name').get_text()  # 발행사
        rcept_dt = rcept_no[:8]  # 공시일
        pym_dt = table.find('tu', attrs={'aunit': 'PYM_DT'}).get('aunitvalue')  # 발행일
        seq_no = table.find('te', attrs={'acode': 'SEQ_NO'}).get_text()  # 회차
        dnm_sum = table.find('te', attrs={'acode': 'DNM_SUM'}).get_text()  # 권면총액
        prft_rate = table.find('te', attrs={'acode': 'PRFT_RATE'}).get_text()  # 표면이자율
        lst_rtn_rt = table.find('te', attrs={'acode': 'LST_RTN_RT'}).get_text()  # 만기이자율
        exp_dt = table.find('tu', attrs={'aunit': 'EXP_DT'}).get('aunitvalue')  # 사채만기일
        exe_rt = table.find('te', attrs={'acode': 'EXE_RT'}).get_text()  # 전환비율
        exe_prc = table.find('te', attrs={'acode': 'EXE_PRC'}).get_text()  # 전환가액
        exe_func = table.find('te', attrs={'acode': 'EXE_FUNC'}).get_text()  # 할증발행
        stk_knd = table.find('te', attrs={'acode': 'STK_KND'}).get_text()  # 대상주식
        stk_cnt = table.find('te', attrs={'acode': 'STK_CNT'}).get_text()  # 주식수
        stk_rt = table.find('te', attrs={'acode': 'STK_RT'}).get_text()  # 주식총수대비비율
        sb_bgn_dt = table.find('tu', attrs={'aunit': 'SB_BGN_DT'}).get('aunitvalue')  # 시작일
        sb_end_dt = table.find('tu', attrs={'aunit': 'SB_END_DT'}).get('aunitvalue')  # 종료일
        try:
            min_rsn = table.find('te', attrs={'acode': 'MIN_RSN'}).get_text()  # 리픽싱조항
            min_prc = table.find('te', attrs={'acode': 'MIN_PRC'}).get_text()  # 최저조정가액한도
        except:
            min_rsn = '-'
            min_prc = '-'

        issu_table_group = soup.find('table-group', attrs={'aclass': 'CRP_ISSU'})  # 인수인 부분(특정인에 대한 대상자별 사채발행내역)
        issu_table = issu_table_group.find('table', attrs={'aclass': 'EXTRACTION'})
        issu_nms = issu_table.tbody.find_all('tr')
        issu_nm = ""
        for i in issu_nms:
            issu_nm = issu_nm + i.find('te', attrs={'acode': "ISSU_NM"}).get_text() + ","
        issu_nm = issu_nm[:-1]

        row = {'종류': doc_nm, '발행사': company_nm, '공시일': rcept_dt, '발행일': pym_dt, '회차': seq_no, '권면총액': dnm_sum,
               '표면이자율(%)': prft_rate, '만기이자율(%)': lst_rtn_rt,
               '사채만기일': exp_dt, '전환/행사/교환 비율': exe_rt, '전환/행사/교환 가액': exe_prc, '대상주식': stk_knd, '주식수': stk_cnt,
               '주식총수대비비율(%)': stk_rt, '청구/행사 시작일':sb_bgn_dt, '청구/행사 종료일':sb_end_dt, '할증발행':exe_func, '리픽싱조항': min_rsn, '최저조정가액한도': min_prc, '인수인': issu_nm}

        return row

    except Exception as e:
        print(rcept_no + " Error!")
        print(e)

        return {}

# 주요사항보고서(자본으로인정되는채무증권발행결정) 추출
def get_perp_data(start_dt, end_dt, corp_code=None):
    rcept_name = '주요사항보고서(자본으로인정되는채무증권발행결정)'
    rcept_no_list = []
    start_dt = start_dt.strftime('%Y%m%d')
    end_dt = end_dt.strftime('%Y%m%d')
    if corp_code == '':
        rcept_no_list.extend(get_rcept_no(rcept_name, start_dt, end_dt))
    else:
        rcept_no_list.extend(get_rcept_no_by_corp(corp_code, rcept_name, start_dt, end_dt))
    rows = []
    for rcept in rcept_no_list:
        row = get_perp_docu(rcept)
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty == False :
        df = df.dropna(subset=['발행사'])
        df = df.sort_values('공시일', ascending=False)
    return df

# 주요사항보고서(자본으로인정되는채무증권발행결정) 상세정보 추출
def get_perp_docu(rcept_no):
    url = 'https://opendart.fss.or.kr/api/document.xml'
    params = {'crtfc_key': API_KEY, 'rcept_no': rcept_no}
    response = requests.get(url, params=params, headers=headers)
    try:
        zf = zipfile.ZipFile(BytesIO(response.content))
        fp = zf.read('{}.xml'.format(rcept_no))
        try:
            xml_str = fp.decode('cp949')
            xml_str = xml_str.replace('<=', '')
            xml = xml_str.encode('cp949')
        except:
            xml_str = fp.decode('utf-8')
            xml_str = xml_str.replace('<=', '')
            xml = xml_str.encode('utf-8')

        soup = BeautifulSoup(xml, features='html.parser')
        table = soup.find('table-group', attrs={'aclass': 'CB_PUB'})
        pl_knd = table.find('te', attrs={'acode': 'PL_KND'}).get_text()  # 사채의 종류
        if '신종자본증권' in pl_knd:
            company_nm = soup.find('company-name').get_text()  # 발행사
            rcept_dt = rcept_no[:8]  # 공시일
            dnm_sum = table.find('te', attrs={'acode': 'DNM_SUM'}).get_text()  # 권면총액
            fnd_use1 = table.find('te', attrs={'acode': 'FND_USE1'}).get_text().strip()  # 시설자금
            fnd_use_sq = table.find('te', attrs={'acode': 'FND_USE_SQ'}).get_text().strip()  # 영업양수자금
            fnd_use2 = table.find('te', attrs={'acode': 'FND_USE2'}).get_text().strip()  # 운영자금
            fnd_use_rd = table.find('te', attrs={'acode': 'FND_USE_RD'}).get_text().strip()  # 채무상환자금
            anc_acq_prc = table.find('te', attrs={'acode': 'ANC_ACQ_PRC'}).get_text().strip()  # 타법인증권취득자금
            fnd_use3 = table.find('te', attrs={'acode': 'FND_USE3'}).get_text().strip()  # 기타자금
            fnd = '' + (("시설자금(원): " + fnd_use1 + "\n") if fnd_use1 != '-' else '') \
                  + (("영업양수자금(원): " + fnd_use_sq + "\n") if fnd_use_sq != '-' else '') \
                  + (("운영자금(원): " + fnd_use2 + "\n") if fnd_use2 != '-' else '') \
                  + (("채무상환자금(원): " + fnd_use_rd + "\n") if fnd_use_rd != '-' else '') \
                  + (("타법인증권취득자금(원): " + anc_acq_prc + "\n") if anc_acq_prc != '-' else '') \
                  + (("기타자금(원): " + fnd_use3 + "\n") if fnd_use3 != '-' else '')
            prft_rate = table.find('te', attrs={'acode':'PRFT_RATE'}).get_text() # 표면이자율
            lst_rtn_rt = table.find('te', attrs={'acode':'LST_RTN_RT'}).get_text() # 만기이자율
            exp_dt = table.find('tu', attrs={'aunit': 'EXP_DT'}).get_text()  # 사채만기일
            exp_dt_dur = table.find('te', attrs={'acode': 'EXP_DT_DUR'}).get_text()  # 사채만기기간
            int_gv_mth = table.find('te', attrs={'acode': 'INT_GV_MTH'}).get_text().replace('\n', '').replace('-', '')  # 이자지급방법
            int_stp = table.find('te', attrs={'acode': 'INT_STP'}).get_text().replace('\n', '').replace('-', '').replace('&cr', '')  # 이자지급 정지(유예) 가능여부 및 조건
            int_stp_acm = table.find('te', attrs={'acode': 'INT_STP_ACM'}).get_text().replace('\n', '').replace('-', '')  # 유예이자 누적 여부
            int_st_up = table.find('te', attrs={'acode': 'INT_ST_UP'}).get_text().replace('\n',
                                                                                          '').replace('-', '').replace('&cr', '')  # 금리상향조정 등 이자율 조정 조건
            rtn_mth = table.find('te', attrs={'acode': 'RTN_MTH'}).get_text().replace('\n', '').replace('-', '').replace('&cr', '')  # 원금 만기상환방법
            erl_rtn_mth = table.find('te', attrs={'acode': 'ERL_RTN_MTH'}).get_text().replace('\n',
                                                                                              '').replace('-', '').replace('&cr', '')  # 원금 조기상환 가능시점 및 조건
            exp_rnw_mth = table.find('te', attrs={'acode': 'EXP_RNW_MTH'}).get_text().replace('\n',
                                                                                              '').replace('-', '').replace('&cr', '')  # 원금 상환 만기연장 조건 및 방법
            opt_fct = table.find('te', attrs={'acode': 'OPT_FCT'}).get_text().replace('\n', '').replace('-', '').replace('&cr', '')  # 옵션에 관한 사항
            chf_agn = table.find('te', attrs={'acode': 'CHF_AGN'}).get_text()  # 대표주관회사
            issu_table_group = soup.find('table-group', attrs={'aclass': 'CRP_ISSU'})  # 인수인(특정인에 대한 대상자별 사채발행내역)
            issu_table = issu_table_group.find('table', attrs={'aclass': 'EXTRACTION'})
            issu_nms = issu_table.tbody.find_all('tr')
            issu_nm = ""
            for i in issu_nms:
                issu_temp = i.find('te', attrs={'acode': "ISSU_NM"}).get_text()
                if issu_temp != '-':
                    issu_nm = issu_nm + issu_temp + ","
            issu_nm = issu_nm[:-1]

            row = {'발행사': company_nm, '종류': pl_knd, '공시일': rcept_dt, '권면총액': dnm_sum, '자금조달의 목적': fnd,
                   '표면이자율(%)': prft_rate, '만기이자율(%)': lst_rtn_rt, '사채만기일': exp_dt,  "사채만기기간": exp_dt_dur,
                   '이자지급방법': int_gv_mth, '이자지급 정지(유예) 가능여부 및 조건': int_stp, '유예이자 누적여부': int_stp_acm, '이자율 조정 조건': int_st_up,
                   '원금 만기상환방법': rtn_mth, '원금 조기상환 조건': erl_rtn_mth, '원금 만기연장 조건 및 방법': exp_rnw_mth,
                   '옵션': opt_fct, '대표주관회사': chf_agn, '인수인': issu_nm}
        else:
            row = {}
            pass

    except Exception as e:
        print(rcept_no + " Error!")
        print(e)
        row = {}

    return row

# 주요사항보고서(유상증자결정) 데이터 호출
def get_cps_data(start_dt, end_dt, corp_nm):
    with open('./pickle/Cprs_new.pkl', 'rb') as f:
        df = pickle.load(f)
    if corp_nm == '':
        df = df[(df['공시일'] >= start_dt.strftime('%Y%m%d')) & (df['공시일'] <= end_dt.strftime('%Y%m%d'))]
    else:
        df = df[(df['공시일'] >= start_dt.strftime('%Y%m%d')) & (df['공시일'] <= end_dt.strftime('%Y%m%d'))
                & (df['발행사'] == corp_nm)]
    if df.empty == False :
        df = df.dropna(subset=['발행사'])
        df.loc[df['전환청구기간']=='-', '전환청구기간'] = '-~-'
        df[['전환청구시작일', '전환청구종료일']] = df['전환청구기간'].str.split('~', expand=True)
        df['전환청구시작일'] = df['전환청구시작일'].str.replace(pat=r'[ㄱ-ㅣ가-힣]+', repl=r'', regex=True).str.replace(' ', '').str.replace('.', '')
        df['전환청구종료일'] = df['전환청구종료일'].str.replace(pat=r'[ㄱ-ㅣ가-힣]+', repl=r'', regex=True).str.replace(' ',
                                                                                                      '').str.replace(
            '.', '')
        df = df.rename(columns={'전환조건':'전환가액 조정에 관한 사항'})
        df = df[['발행사', '공시일', '신주의 종류와 수', '1주당 액면가액', '증자방식', '전환비율', '전환가액', '전환가액결정방법',
                  '전환주식종류', '전환주식수', '주식총수대비비율', '전환청구시작일', '전환청구종료일',
                 '전환가액 조정에 관한 사항', '최저조정가액', '최저조정가액근거', '전환가액의 70%미만으로 조정가능한 잔여발행한도', '의결권',
                 '옵션', '이익배당', '신주발행가액', '할인율 또는 할증율(%)']]
    return df

# 주요사항보고서(유상증자결정) 상세정보 추출
def get_cps_docu(rcept_no):
    url = 'https://opendart.fss.or.kr/api/document.xml'
    params = {'crtfc_key': API_KEY, 'rcept_no': rcept_no}
    response = requests.get(url, params=params)
    try:
        zf = zipfile.ZipFile(BytesIO(response.content))
        fp = zf.read('{}.xml'.format(rcept_no))
        try:
            xml_str = fp.decode('cp949')
            xml_str = xml_str.replace('<=', '')
            xml = xml_str.encode('cp949')
        except:
            xml_str = fp.decode('utf-8')
            xml_str = xml_str.replace('<=', '')
            xml = xml_str.encode('utf-8')

        soup = BeautifulSoup(xml, features='html.parser')
        table = soup.find('table-group', attrs={'aclass': 'CST_CNT'})
        pst_cnt = table.find('te', attrs={'acode': 'PST_CNT'}).get_text()  # 기타주식(주)
        if pst_cnt.strip() in ('-', '0'):
            row = {}
        else:
            company_nm = soup.find('company-name').get_text()  # 발행사
            rcept_dt = rcept_no[:8]  # 공시일
            fval = table.find('te', attrs={'acode': 'FVAL'}).get_text()  # 1주당 액면가액
            ci_mth = table.find('tu', attrs={'aunit': 'CI_MTH'}).get_text()  # 증자방식

            table = soup.find('table-group', attrs={'aclass': 'TG_RDT_CVT'})
            if table is None:
                table = soup.find('table-group', attrs={'aclass': 'TG_CVT_RIT'})
            cvt_knd = table.find('te', attrs={'acode': 'CVT_KND'}).get_text()  # 전환주식종류
            cvt_cnt = table.find('te', attrs={'acode': 'CVT_CNT'}).get_text()  # 전환주식수
            exe_rt = table.find('te', attrs={'acode': 'EXE_RT'}).get_text()  # 전환비율
            exe_prc = table.find('te', attrs={'acode': 'EXE_PRC'}).get_text()  # 전환가액
            exe_func = table.find('te', attrs={'acode': 'EXE_FUNC'}).get_text()  # 전환가액결정방법
            cvt_rt = table.find('te', attrs={'acode': 'CVT_RT'}).get_text()  # 주식총수대비 비율
            cvt_bgn_dt = table.find('tu', attrs={'aunit': 'CVT_BGN_DT'}).get_text()  # 시작일
            cvt_end_dt = table.find('tu', attrs={'aunit': 'CVT_END_DT'}).get_text()  # 종료일
            cvt_prd = cvt_bgn_dt + "~" + cvt_end_dt
            cvt_cdt = table.find('te', attrs={'acode': 'EXE_REG'}).get_text()  # 전환조건
            try:
                min_prc = table.find('te', attrs={'acode': 'MIN_PRC'}).get_text() # 최저 조정가액
                min_rsn = table.find('te', attrs={'acode': 'MIN_RSN'}).get_text() # 최저 조정가액 근거
                ctr_lmt = table.find('te', attrs={'acode': 'CTR_LMT'}).get_text() # 발행당시 전환가액의 70%미만으로 조정가능한 잔여발행한도
            except:
                min_prc = '-'
                min_rsn = '-'
                ctr_lmt = '-'
            opt_fct = table.find('te', attrs={'acode': 'OPT_FCT'}).get_text() # 옵션에 관한 사항
            vtr_info = table.find('te', attrs={'acode': 'VTR_INFO'}).get_text()  # 의결권
            dvd_info = table.find('te', attrs={'acode': 'DVD_INFO'}).get_text()  # 이익배당

            table = soup.find('table-group', attrs={'aclass': 'THD_ASN_INC'})
            pst_iss_val = table.find('te', attrs={'acode': 'PST_ISS_VAL'}).get_text()  # 신주발행가액
            dc_rate = table.find('te', attrs={'acode': 'DC_RATE'}).get_text()  # 할인율, 할증율

            row = {'발행사': company_nm, '공시일': rcept_dt, '신주의 종류와 수': pst_cnt, '1주당 액면가액': fval, '증자방식': ci_mth,
                   '전환비율': exe_rt, '전환가액': exe_prc, '전환가액결정방법': exe_func, '전환주식종류': cvt_knd, '전환주식수': cvt_cnt,
                   '주식총수대비비율': cvt_rt, '전환청구기간': cvt_prd, '전환조건': cvt_cdt, '최저조정가액': min_prc, '최저조정가액근거': min_rsn,
                   '전환가액의 70%미만으로 조정가능한 잔여발행한도': ctr_lmt, '의결권': vtr_info, '옵션':opt_fct,
                   '이익배당': dvd_info, '신주발행가액': pst_iss_val, '할인율 또는 할증율(%)': dc_rate}

    except Exception as e:
        print(rcept_no + " Error!")
        print(e)
        row = {}

    return row

# 메자닌채권 Data Cleansing
def cleansing_mzn_df(df):
    df['발행사'] = df['발행사'].str.replace('주식회사', '').str.replace('(주)', '').str.replace('㈜', '').str.replace('(',
                                                                                                          '').str.replace(
        ')', '').str.strip()
    df['발행일'] = df['발행일'].str.replace(pat=r'[ㄱ-ㅣ가-힣]+', repl=r'', regex=True).str.replace(' ', '')
    df['사채만기일'] = df['사채만기일'].str.replace(pat=r'[ㄱ-ㅣ가-힣]+', repl=r'', regex=True).str.replace(' ', '').str.replace('.','')
    df = df[(df['발행일'] != '-') & (df['사채만기일'] != '-')]
    df['만기기간'] = round((pd.to_datetime(df['사채만기일']) - pd.to_datetime(df['발행일']))/np.timedelta64(1, 'Y'), 0)
    df['발행연도'] = df['발행일'].str[:4]
    df['발행월'] = df['발행일'].str[4:6]
    df = df.astype({'발행연도': int, '발행월': int})
    df['권면총액'] = df['권면총액'].str.replace(',', '').replace('-', '0').str.replace('\n', '').astype('float')
    df['표면이자율(%)'] = df['표면이자율(%)'].str.strip()
    df['만기이자율(%)'] = df['만기이자율(%)'].str.strip()
    df['표면이자율(%)'] = df['표면이자율(%)'].str.replace('\n', '').str.replace('&cr', '')
    df['만기이자율(%)'] = df['만기이자율(%)'].str.replace('\n', '').str.replace('&cr', '')
    df = df[(df['표면이자율(%)'] != '-') & (df['만기이자율(%)'] != '-')]
    df['표면이자율(%)'] = df['표면이자율(%)'].astype('float')
    df['만기이자율(%)'] = df['만기이자율(%)'].astype('float')
    df['주식수'] = df['주식수'].str.replace(',', '').str.replace('\n', '').str.replace('&cr', '')
    df = df[df['주식수'] != '-']
    df['주식수'] = df['주식수'].astype('float')
    df.loc[df['종류'] == '신주인수권', '종류'] = '신주인수권부사채권'
    df['발행일'] = pd.to_datetime(df['발행일'])
    df['공시일'] = pd.to_datetime(df['공시일'])
    df = df.reset_index(drop=True)
    df = df.loc[df.groupby(['종류', '발행사', '회차'])['공시일'].idxmax()]
    return df

# Dataframe 변환 및 다운로드
def set_df(df, file_nm, start_dt, end_dt):
    # 총 조회 건수
    row_cnt = "총 " + str(df.shape[0]) + "건"
    st.text(row_cnt)

    df = df.reset_index(drop=True)
    df.index += 1
    st.dataframe(df)

    csv = df.to_csv().encode('utf-8-sig')
    st.download_button(
        label="Download",
        data=csv,
        file_name='{}_{}_{}.csv'.format(file_nm, start_dt, end_dt),
        mime='text/csv'
    )
