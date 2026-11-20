"""
儀器管理到期通知（校驗／合約）— 設定

兩種通知共用同一套規則與信件版型，差異都集中在 NotifyKind。
"""
from collections import namedtuple

URGENT_DAYS = 7  # 逾期或幾天內到期算「需立即處理」

# 收件角色：數字越小優先度越高（同一台儀器對同一人只保留最高的角色）
ROLE_KEEPER = "保管人"
ROLE_SUB = "組保管人代收"
ROLE_ADDL = "額外通知"
ROLE_RANK = {ROLE_KEEPER: 0, ROLE_SUB: 1, ROLE_ADDL: 2}

# 主檔保管人欄位：(角色名稱, 帳號欄位)
KEEPER_COL = ("保管人", "Keeper")
SUB_KEEPER_COL = ("組保管人", "Sub_Keeper")
ADDL_KEEPER_COL = ("額外通知人", "Additional_Notify_Keeper")

NotifyKind = namedtuple("NotifyKind", [
    "key",               # calibration / contract
    "heading",           # 信件頁首標題
    "subject_tag",       # 郵件主旨前綴
    "schedule_label",    # 頁首副標
    "send_days",         # 每月寄送日，用來推算上次寄送日（判斷【新】）
    "enable_col",        # = 'Y' 才通知
    "date_col",          # 到期日欄位
    "days_col",          # 提前通知天數欄位
    "no_date_reason",    # 到期日算不出來時，資料異常的說明
    "unit",              # 台 / 份
    "tags",              # 名稱後面要顯示的標記
    "split_urgent",      # 是否把逾期與 7 天內到期另列「需立即處理」
    "columns",           # 完整清單欄位：[(item key, 欄位標題)]
    "date_label",        # 到期日欄位標題
    "intro",             # 對象下方的說明
    "tip",               # 清單上方的提醒（可為 None）
    "not_notified",      # 「以下情況不會寄出通知」
    "footer",            # 頁尾
])

UNDELIVERABLE_NOTE = "保管人與組保管人帳號都已停用或不存在（改通知 IT）"

CALIBRATION = NotifyKind(
    key="calibration",
    heading="儀器校驗到期通知",
    subject_tag="【儀器校驗到期】",
    schedule_label="每月 1、15 號發送",
    send_days=(1, 15),
    enable_col="is_Notify_Calibration",
    date_col="Next_Due_Date",
    days_col="Notify_Days_Before",
    no_date_reason="是否通知校驗 = Y，但沒有上次校驗日（算不出到期日）",
    unit="台",
    tags=("GMP", "PROCESSING", "NEW"),
    split_urgent=True,
    columns=[("Cal_Vendor_Name", "廠商"), ("Last_PR_No", "上次 PR")],
    date_label="到期日",
    intro="以下是你負責或需要知悉的儀器，請安排校驗。",
    tip=None,
    not_notified=[
        "主檔「是否通知校驗」為 N",
        "主檔沒有上次校驗日（算不出到期日）",
        "儀器已停用",
        UNDELIVERABLE_NOTE,
    ],
    footer="校驗完成後請於 BPM 登錄校驗紀錄，主檔到期日會自動更新。本信由系統自動發送，請勿直接回覆。",
)

CONTRACT = NotifyKind(
    key="contract",
    heading="維護合約到期通知",
    subject_tag="【儀器合約到期】",
    schedule_label="每月 1 號發送",
    send_days=(1,),
    enable_col="is_Maint_Contract",
    date_col="Contract_Expiry_Date",
    days_col="Contract_Notify_Days_Before",
    no_date_reason="有維護合約 = Y，但未填合約到期日",
    unit="份",
    tags=("NEW",),
    split_urgent=False,
    columns=[("Ariba_Contract_No", "合約編號"), ("BPM_Contract_FormNo", "BPM 合約申請單號")],
    date_label="合約到期日",
    intro="以下儀器的維護保養合約即將到期或已過期，請評估是否續約。",
    tip=("💡 續約請先在 BPM 送出合約申請，<b>待合約流程簽核完成</b>後，"
         "才能在 BPM 儀器主檔選到新合約並更新「合約到期日」；未更新會持續通知。"),
    not_notified=[
        "主檔「有維護合約」為 N",
        "主檔未填合約到期日",
        "儀器已停用",
        UNDELIVERABLE_NOTE,
    ],
    footer="本信由系統自動發送，請勿直接回覆。",
)

KINDS = {k.key: k for k in (CALIBRATION, CONTRACT)}

# 主檔讀取欄位（兩種通知共用一支 SQL）
MASTER_COLUMNS = [
    "Instrument_No", "Asset_No", "Instrument_Name", "is_Active", "is_GMP",
    "is_Notify_Calibration", "Next_Due_Date", "Notify_Days_Before", "Cal_Vendor_Name",
    "is_Maint_Contract", "Contract_Expiry_Date", "Contract_Notify_Days_Before",
    "Ariba_Contract_No", "BPM_Contract_FormNo",
    "Keeper", "Keeper_Name", "Sub_Keeper", "Sub_Keeper_Name",
    "Additional_Notify_Keeper", "Additional_Notify_Keeper_Name",
]
