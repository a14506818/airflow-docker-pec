import pyodbc

from src.common.common import get_mssql_conn_str
from src.instrument_notify.config import MASTER_COLUMNS


def _fetch_dicts(sql):
    conn = pyodbc.connect(get_mssql_conn_str(), timeout=5)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        names = [d[0] for d in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_instruments(**context):
    """儀器主檔 + 是否有處理中的校驗紀錄 + 最新一筆 PR 單號"""
    cols = ", ".join(f"m.[{c}]" for c in MASTER_COLUMNS)
    rows = _fetch_dicts(f"""
        SELECT {cols},
               CASE WHEN EXISTS (
                   SELECT 1 FROM dbo.REF_Instrument_Cal_Record r
                   WHERE r.Instrument_No = m.Instrument_No AND r.Cal_Result = N'PROCESSING'
               ) THEN 1 ELSE 0 END AS is_Processing,
               (SELECT TOP 1 r.PR_No FROM dbo.REF_Instrument_Cal_Record r
                WHERE r.Instrument_No = m.Instrument_No AND r.PR_No > N''
                ORDER BY ISNULL(TRY_CONVERT(datetime, r.Cal_Date), '9999-12-31') DESC, r.SN DESC
               ) AS Last_PR_No
        FROM dbo.REF_Instrument_Master m
    """)
    if not rows:
        raise ValueError("❌ 儀器主檔查無資料，請檢查資料庫")
    for r in rows:  # XCom 需可 JSON 序列化
        for k, v in r.items():
            if v is not None and not isinstance(v, (str, int, float, bool)):
                r[k] = str(v)
    print(f"✅ 取得儀器主檔 {len(rows)} 筆")
    return rows


def get_members(**context):
    """BPMSysUsers（帳號狀態）+ v_BPMSysOUMembers.Email"""
    rows = _fetch_dicts("""
        SELECT u.Account, u.DisplayName, u.Disabled, e.Email
        FROM BPMDB.dbo.BPMSysUsers u
        LEFT JOIN (
            SELECT UserAccount, MAX(Email) AS Email
            FROM BPMDB.dbo.v_BPMSysOUMembers
            WHERE Email LIKE N'%_@_%'
            GROUP BY UserAccount
        ) e ON e.UserAccount = u.Account
    """)
    if not rows:
        raise ValueError("❌ BPMSysUsers 查無資料，請檢查資料庫")
    members = [
        {"Account": r["Account"], "DisplayName": r["DisplayName"],
         "Disabled": bool(r["Disabled"]), "Email": r["Email"]}
        for r in rows
    ]
    print(f"✅ 取得 BPMSysUsers {len(members)} 筆，有 email {sum(1 for m in members if m['Email'])} 筆")
    return members
