import pandas as pd
import time
import os
import pickle
import datetime
from datetime import timedelta
import pe_func

dir_path = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(dir_path, 'pickle', 'Cprs_new.pkl'), 'rb') as f:
    df_org = pickle.load(f)
df_org = df_org.dropna(subset=['공시일', '발행사'])

bgn_de = max(df_org['공시일'])
bgn_de = (datetime.datetime.strptime(bgn_de, '%Y%m%d')+timedelta(days=1)).strftime('%Y%m%d')
end_de = (datetime.datetime.today()-timedelta(days=1)).strftime('%Y%m%d')
print('bgn_de: ', bgn_de, 'end_de: ', end_de)

if __name__ == '__main__':
    rcept_name = '주요사항보고서(유상증자결정)'
    rcept_no_list = []
    rcept_no_list.extend(pe_func.get_rcept_no(rcept_name, bgn_de, end_de))
    time.sleep(1)

    # 보고서 접수번호별 세부정보 추출
    rows = []
    for rcept in rcept_no_list:
        row = pe_func.get_cps_docu(rcept)
        rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty == False:
        df = df.dropna(subset=['공시일', '발행사'])
        df['주식총수대비비율'] = df.주식총수대비비율.str.replace('\n', '')
        df['전환조건'] = df.전환조건.str.replace('\n', '')
        df['의결권'] = df.의결권.str.replace('\n', '')
        df['이익배당'] = df.이익배당.str.replace('\n', '')
        df[['전환비율', '전환가액', '전환가액결정방법', '주식총수대비비율']] = df[['전환비율', '전환가액', '전환가액결정방법', '주식총수대비비율']].fillna(
            '-')
        print("크롤링 결과 사이즈: ", df.shape)

        # 기존파일 백업
        with open(os.path.join(dir_path, 'pickle', 'Cprs_bk.pkl'), 'wb') as f:
            pickle.dump(df_org, f)
        print("백업 사이즈: ", df_org.shape)

        # 파일 합치기
        df_new = pd.concat([df_org, df])
        df_new = df_new.sort_values('공시일')
        df_new.reset_index(inplace=True, drop=True)
        df_new = df_new.drop_duplicates(ignore_index=True)
        print("최종 사이즈: ", df_new.shape)
        with open(os.path.join(dir_path, 'pickle', 'Cprs_new.pkl'), 'wb') as f:
            pickle.dump(df_new, f)

    else:
        print("No row added!")