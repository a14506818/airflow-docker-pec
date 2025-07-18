import pandas as pd
import pyodbc

def extract_member_data(conn_str):
    """
    從 MSSQL 資料庫中提取 BPM Member 資料
    """
    # 建立連接
    conn = pyodbc.connect(conn_str)
    
    # SQL 查詢語句
    query = """
        select UserAccount, DisplayName, HRID, LeaderTitle, SupervisorUserAccount, UserCostCenter, PEC_Level
        , CMO_COO_CTO, GM, CEO, Chairman, OU_Code, OU_CostCenter, OUName, Manager
        , FullMemberName, company, UserDefaultRole
        from BPMDB.dbo.v_BPMSysOUMembers 
    """

    # 執行查詢並轉換為 DataFrame
    df = pd.read_sql(query, conn)
    print(df.head())  # 印出前幾筆資料以確認

    conn.close()
    
    return df

def get_UAT_member_data():
    """
    從 UAT 環境取得 BPM Member 資料
    """
    from src.common.common import get_UAT_mssql_conn_str
    
    member_data_df = extract_member_data(get_UAT_mssql_conn_str())
    print(f"✅ 成功從 UAT 環境取得 BPM Member 資料，共 {len(member_data_df)} 筆")
    
    return member_data_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def get_PRD_member_data():
    """
    從 PRD 環境取得 BPM Member 資料
    """
    from src.common.common import get_PRD_mssql_conn_str
    
    member_data_df = extract_member_data(get_PRD_mssql_conn_str())
    print(f"✅ 成功從 PRD 環境取得 BPM Member 資料，共 {len(member_data_df)} 筆")
    
    return member_data_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict