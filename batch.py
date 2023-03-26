import requests
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
import warnings
import pickle
import datetime
from datetime import timedelta
import time

warnings.filterwarnings(action='ignore')
API_KEY = 'd7d1be298b9cac1558eab570011f2bb40e2a6825'
headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
          'Accept-Encoding': '*', 'Connection': 'keep-alive'}

# target_day = (datetime.datetime.today()-timedelta(days=1)).strftime('%Y%m%d')
# print(target_day)
# bgn_de = target_day # 코드 수행 전일
# end_de = target_day # 코드 수행 전일

bgn_de='20230323'
end_de='20230326'

# 보고서명, 일자로 검색해서 보고서 접수번호 추출
def get_rcept_no(report_nm, bgn_de, end_de):
    rcept_no_list = []
    url = 'https://opendart.fss.or.kr/api/list.xml'
    params = {'crtfc_key': API_KEY
        , 'bgn_de': bgn_de
        , 'end_de': end_de
        , 'pblntf_detail_ty': 'B001'
        , 'last_reprt_at': 'Y'}
    response = requests.get(url, params=params, headers=headers, verify=False)
    soup = BeautifulSoup(response.content, features='xml')

    try:
        total_page = soup.find('total_page').get_text()
        for i in range(1, int(total_page) + 1):
            params = {'crtfc_key': API_KEY
                , 'bgn_de': bgn_de
                , 'end_de': end_de
                , 'pblntf_detail_ty': 'B001'
                , 'page_no': str(i)
                , 'last_reprt_at': 'Y'}
            response = requests.get(url, params=params, headers=headers, verify=False)
            soup = BeautifulSoup(response.content, features='xml')
            for c in soup.find_all('list'):
                if report_nm in c.report_nm.get_text():
                    rcept_no_list.append(c.rcept_no.get_text())
    except:
        rcept_no_list = []

    print('보고서명:', report_nm, ', 보고서수: ', len(rcept_no_list))

    return rcept_no_list


def get_corp_docu(rcept_no):
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

if __name__ == '__main__':
    rcept_names = ['주요사항보고서(전환사채권발행결정)', '주요사항보고서(신주인수권부사채권발행결정)', '주요사항보고서(교환사채권발행결정)']
    rcept_no_list = []
    for r in rcept_names:
        rcept_no_list.extend(get_rcept_no(r, bgn_de, end_de))
        time.sleep(1)

    # 보고서 접수번호별 세부정보 추출
    rows = []
    for rcept in rcept_no_list:
        row = get_corp_docu(rcept)
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty == False:
        df = df[df['대상주식'] != '-']
        print("크롤링 결과 사이즈: ", df.shape)

        with open('./Mezzanine_new.pkl', 'rb') as f:
            df_org = pickle.load(f)
        print("백업 사이즈: ", df_org.shape)

        # 기존파일 백업
        with open('./Mezzanine_bk.pkl', 'wb') as f:
            pickle.dump(df_org, f)

        # 파일 합치기
        df_new = pd.concat([df_org, df])
        df_new = df_new.sort_values('공시일')
        print("최종 사이즈: ", df_new.shape)
        with open('./Mezzanine_new.pkl', 'wb') as f:
            pickle.dump(df_new, f)

    else:
        print("No row added!")
