"""
儀器主檔每日 SAP 資料同步 — 設定（SRS-20261118-001）

只要改這個檔就能調整 RFC 參數與欄位對應；合併規則在 rules.py。
"""
from collections import namedtuple

# SAP RFC -------------------------------------------------------------------------------------------
RFC_NAME = "Z_FI_BPM_013"
RFC_PARAMS = {
    "PI_BUKRS": "1000",  # 公司代碼
    "PI_SPRAS": "M",     # 語言：繁體中文
    "PI_FLAG": "X",      # 排除暫時資產
}
RFC_TABLE = "PT_OUT"     # 回傳結構 ZFIS0010

# 從 PT_OUT 保留的欄位（其餘欄位不需要，避免 XCom 過大）
SAP_FIELDS = [
    "ANLN1", "TXT50", "TXA50", "ANLKL", "TXK50",
    "STORT", "STORT_TXT", "KOSTL", "KTEXT", "KFZKZ", "ORD41",
]
SAP_KEY = "ANLN1"
# 有 ALPHA 轉換的欄位：SAP 內部值有前導零（00161670），畫面顯示時去掉（161670）
ALPHA_FIELDS = {"ANLKL"}
# 必要欄位：空值 → 該筆不同步、列入報表（ANLN1 另有檢核，空值直接中止）
SAP_REQUIRED_FIELDS = ["TXT50"]
SAP_KEEPER_FIELD = "KFZKZ"  # 保管人 SAP 帳號（員工編號）

# SAP 欄位 → 主檔欄位（白名單：只有這些欄位會被資產同步改寫）
FIELD_MAP = {
    "TXT50": "Instrument_Name",
    "TXA50": "Instrument_Spec",
    "ANLKL": "Asset_Type",
    "TXK50": "Asset_Type_Name",
    "STORT": "Location_Code",
    "STORT_TXT": "Location_Name",
    "KOSTL": "Cost_Center_Code",
    "KTEXT": "Cost_Center_Name",
}

# 保管人角色 ------------------------------------------------------------------------------------------
KeeperRole = namedtuple("KeeperRole", ["label", "account_col", "name_col", "sap_col", "from_sap"])
KEEPER_ROLES = [
    # from_sap=True：資產在 SAP 且 KFZKZ 有值時，以 SAP 為準換保管人
    KeeperRole("保管人", "Keeper", "Keeper_Name", "Keeper_SAP_Account", True),
    KeeperRole("各組總保管人", "Sub_Keeper", "Sub_Keeper_Name", "Sub_Keeper_SAP_Account", False),
    KeeperRole("額外通知人", "Additional_Notify_Keeper", "Additional_Notify_Keeper_Name",
               "Additional_Notify_Keeper_SAP_Account", False),
]
# BPMSysUsers.SAP_Account 中的髒值，一律視為空
JUNK_SAP_ACCOUNTS = {"", "null", "na", "n/a", "none", "-"}

# 主檔 ----------------------------------------------------------------------------------------------
MASTER_TABLE = "dbo.REF_Instrument_Master"
SOURCE = "SAP_SYNC"                              # 寫入 Updated_By，trigger 用來當 Changed_By
SESSION_CONTEXT_KEY = "InstrumentChangeSource"   # trigger TR_REF_Instrument_Master_Log 讀這個 key

KEEPER_COLUMNS = [c for r in KEEPER_ROLES for c in (r.account_col, r.name_col, r.sap_col)]
# 同步可以寫入的欄位（Last_Sync_Date、Updated_By 由 loader 另外處理）
WRITABLE_COLUMNS = frozenset(list(FIELD_MAP.values()) + KEEPER_COLUMNS)
# 讀主檔時要取的欄位
MASTER_READ_COLUMNS = ["Instrument_No", "Asset_No"] + list(dict.fromkeys(
    list(FIELD_MAP.values()) + KEEPER_COLUMNS
))

# 報表顯示用的中文欄位名
FIELD_LABELS = {
    "Instrument_Name": "儀器名稱",
    "Instrument_Spec": "儀器規格",
    "Asset_Type": "資產類別",
    "Asset_Type_Name": "資產類別名稱",
    "Location_Code": "座落位置代碼",
    "Location_Name": "座落位置名稱",
    "Cost_Center_Code": "成本中心代碼",
    "Cost_Center_Name": "成本中心名稱",
    "Keeper": "保管人帳號",
    "Keeper_Name": "保管人姓名",
    "Keeper_SAP_Account": "保管人SAP帳號",
    "Sub_Keeper": "各組總保管人帳號",
    "Sub_Keeper_Name": "各組總保管人姓名",
    "Sub_Keeper_SAP_Account": "各組總保管人SAP帳號",
    "Additional_Notify_Keeper": "額外通知人帳號",
    "Additional_Notify_Keeper_Name": "額外通知人姓名",
    "Additional_Notify_Keeper_SAP_Account": "額外通知人SAP帳號",
}
