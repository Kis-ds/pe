import streamlit as st
from streamlit_option_menu import option_menu
from dateutil.relativedelta import relativedelta
import pandas as pd
import graphviz
import warnings
import datetime
import os
import pickle
import ecm2
import pe_func

### API 관련 세팅 ###
warnings.filterwarnings(action='ignore')
API_KEY = 'd7d1be298b9cac1558eab570011f2bb40e2a6825'
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'Accept-Encoding': '*', 'Connection': 'keep-alive'}
st.set_page_config(layout='wide')

### 화면 ###
with st.sidebar:
    selected = option_menu("Menu", ["주식연계채권", "타법인출자현황", "CPS/RCPS", "영구채", "기업지배구조"],
                           icons=['chevron-right', 'chevron-right', 'chevron-right', 'chevron-right', 'chevron-right'],
                           menu_icon='card-list', default_index=0)

if selected == "주식연계채권":
    st.header('주식연계채권 발행내역')
    all_yn = st.radio('검색 유형', ('전체 검색', '회사별 검색'), horizontal=True)

    with st.form(key='form1'):
        if all_yn == '회사별 검색':
            with open('./pickle/Mezzanine_new.pkl', 'rb') as f:
                df_mzn = pickle.load(f)
            corp_nm_list = df_mzn.sort_values('발행사')['발행사'].unique()
            corp_nm = st.selectbox('기업명을 입력하세요', corp_nm_list)
        else:
            corp_nm = ''

        knd = st.multiselect('채권 종류', ('전환사채권', '신주인수권부사채권', '교환사채권'))
        c1, c2 = st.columns(2)
        with c1:
            start_dt = st.date_input('시작일')
        with c2:
            end_dt = st.date_input('종료일')  # , min_value=start_dt)
        c3, c4 = st.columns(2)
        with c3:
            intr_ex_min = st.number_input('표면이자율(%) MIN', min_value=0, max_value=100, value=0)
        with c4:
            intr_ex_max = st.number_input('표면이자율(%) MAX', min_value=0, max_value=100, value=10)
        c5, c6 = st.columns(2)
        with c5:
            intr_sf_min = st.number_input('만기이자율(%) MIN', min_value=0, max_value=100, value=0)
        with c6:
            intr_sf_max = st.number_input('만기이자율(%) MAX', min_value=0, max_value=100, value=10)

        form1_bt = st.form_submit_button('조회')

    if form1_bt:
        df = pe_func.get_mezn_data(knd, corp_nm, start_dt, end_dt, intr_ex_min, intr_ex_max, intr_sf_min, intr_sf_max)
        pe_func.set_df(df, "mezzanine", start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d'))

elif selected == "타법인출자현황":
    st.header('ECM2부 - 타법인출자현황(단순투자)')
    with st.form(key='form2'):
        c1, c2 = st.columns(2)
        with c1:
            year = st.selectbox('연도', [x for x in range(2015, datetime.datetime.now().year + 1)])
            file_list = st.selectbox('저장소 파일', sorted(os.listdir('./datasets/')))
        with c2:
            r_code = st.radio("보고서 선택", ("1분기보고서", "반기보고서", "3분기보고서", "사업보고서"), horizontal=True)
            load = st.radio("재수집 여부", ("아니오", "예"), horizontal=True)
        form2_bt = st.form_submit_button('조회')

    if form2_bt:
        # 파일 존재할 경우
        if os.path.isfile('./datasets/' + "ECM_타법인출자-단순투자-{}-{}.csv".format(year, r_code)):
            st.warning("""ECM_타법인출자-단순투자-{}-{} 파일이 저장소에 존재합니다.""".format(year, r_code), icon="⚠️")

            if load == "예":
                st.warning('사용자 조건에 따라 재수집을 진행합니다.', icon="⚠️")
                ecm2.main(year, r_code)

            else:
                st.warning('사용자 조건에 따라 저장소 파일을 불러옵니다.', icon="⚠️")
                save_df = pd.read_csv('./datasets/' + 'ECM_타법인출자-단순투자-{}-{}.csv'.format(year, r_code))
                save_df.index += 1
                st.dataframe(save_df)
                save_df = ecm2.convert_df(save_df, encode_opt=True)
                st.download_button(label="Download", data=save_df,
                                   file_name='ECM_타법인출자-단순투자-{}-{}.csv'.format(year, r_code), mime='text/csv')
        else:
            ecm2.main(year, r_code)

elif selected == "CPS/RCPS":
    st.header('CPS/RCPS 발행내역')
    all_yn = st.radio('검색 유형', ('전체 검색', '회사별 검색'), horizontal=True)

    with st.form(key='form3'):
        if all_yn == '회사별 검색':
            with open('./pickle/Cprs_new.pkl', 'rb') as f:
                df_cprs = pickle.load(f)
            corp_nm_list = df_cprs.sort_values('발행사')['발행사'].unique()
            corp_nm = st.selectbox('기업명을 입력하세요', corp_nm_list)
        else:
            corp_nm = ''

        c1, c2 = st.columns(2)
        with c1:
            start_dt = st.date_input('시작일')
        with c2:
            end_dt = st.date_input('종료일')
        form3_bt = st.form_submit_button('조회')

    if form3_bt:
        with st.spinner("데이터를 수집 중입니다🤖"):
            df = pe_func.get_cps_data(start_dt, end_dt, corp_nm)
        if df.empty:
            st.info('수집할 데이터가 없습니다', icon="🤔")
        else:
            pe_func.set_df(df, "CPS_RCPS", start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d'))

elif selected == "영구채":
    st.header('신종자본증권(영구채) 발행내역')
    corp_code = ''
    all_yn = st.radio('검색 유형', ('전체 검색', '회사별 검색'), horizontal=True)

    with st.form(key='form4'):
        if all_yn =='회사별 검색':
            if "corp" not in st.session_state:
                with st.spinner("기업정보 최초 로딩 시 시간이 소요됩니다😅"):
                    corp_dict = pe_func.get_corp_dict()
                st.success('기업정보 로딩 완료!', icon="🙌")
                st.session_state.corp = corp_dict
            else:
                corp_dict = st.session_state.corp
            corp_nm_list = []
            for c in corp_dict:
                corp_nm_list.append(c)
            corp_nm = st.selectbox('기업명을 입력하세요', corp_nm_list)
            corp_code = corp_dict.get(corp_nm)

        c1, c2 = st.columns(2)
        with c1:
            start_dt = st.date_input('시작일')
        with c2:
            if all_yn == '전체 검색':
                end_dt = st.date_input('종료일(시작일로부터 3개월까지 조회 가능)')
            else:
                end_dt = st.date_input('종료일')
        form4_bt = st.form_submit_button('조회')

    if form4_bt:
        if corp_code == '':
            if end_dt > start_dt + relativedelta(months=3):
                st.warning('종료일을 확인해주세요', icon="⚠️")
            else:
                with st.spinner("데이터를 수집 중입니다🤖"):
                    df = pe_func.get_perp_data(start_dt, end_dt)
                if df.empty:
                    st.info('수집할 데이터가 없습니다', icon="🤔")
                else:
                    pe_func.set_df(df, "perp_bond", start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d'))
        else:
            with st.spinner("데이터를 수집 중입니다🤖"):
                df = pe_func.get_perp_data(start_dt, end_dt, corp_code)
            if df.empty:
                st.info('수집할 데이터가 없습니다', icon="🤔")
            else:
                pe_func.set_df(df, "perp_bond", start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d'))

else:
    st.header("기업 지배구조")
    uploaded_file = st.file_uploader("지배구조 데이터를 업로드 해주세요(확장자 xlsx)", type='xlsx', key="file")
    # 샘플 파일 다운로드
    with open('./datasets/sample.xlsx', 'rb') as f:
        st.download_button('Sample Input File Download', f, file_name='sample.xlsx')

    if uploaded_file is not None:

        df = pd.read_excel(uploaded_file)
        # st.dataframe(df)

        df = df.fillna(0)
        df = df.rename(columns={'Unnamed: 0': '모회사'})
        df.set_index('모회사', inplace=True)

        df_pivot = df.reset_index().melt(id_vars='모회사')
        df_pivot = df_pivot[df_pivot['value'] > 0]
        df_pivot.rename(columns={'variable': '자회사', 'value': '지분'}, inplace=True)
        df_pivot = df_pivot.astype({'지분': 'string'})

        # 모회사, 자회사 중복 없이 저장
        corp = []
        for index, row in df_pivot.iterrows():
            corp.append(row[0])
            corp.append(row[1])
        corp = set(corp)

        g = graphviz.Digraph('round-table', comment='The Round Table')
        for c in corp:
            g.node(c, c)

        for idx, row in df_pivot.iterrows():
            g.edge(row['모회사'], row['자회사'], label=row['지분'])

        st.graphviz_chart(g)