"""
儀器主檔每日 SAP 資料同步 — 合併規則（SRS-20261118-001）

純 Python：不 import airflow / pyodbc / pyrfc，方便單元測試。
每條規則都要有對應測試（plugins/tests/instrument_sync/test_rules.py），修改需經 code review 並先跑 dry-run。
"""
from collections import Counter, defaultdict

from src.instrument_sync.config import (
    ALPHA_FIELDS, FIELD_LABELS, FIELD_MAP, JUNK_SAP_ACCOUNTS, KEEPER_ROLES, SAP_FIELDS,
    SAP_KEEPER_FIELD, SAP_KEY, SAP_REQUIRED_FIELDS, WRITABLE_COLUMNS,
)


class SapValidationError(ValueError):
    """SAP 資料檢核不通過：整次同步中止，主檔不做任何異動"""


# 基本工具 ------------------------------------------------------------------------------------------
def clean(value):
    """None → ''，其餘轉字串並去除前後空白"""
    if value is None:
        return ""
    return str(value).strip()


def alpha_out(value):
    """SAP ALPHA 轉換（輸出）：去掉前導零，全為 0 時保留一個 0"""
    s = clean(value)
    if not s:
        return ""
    return s.lstrip("0") or "0"


def _is_changed(old, new):
    """新值有內容且與舊值不同才算異動（空值不覆蓋）"""
    return bool(new) and clean(old) != new


def _same_account(a, b):
    """BPM 帳號比對不分大小寫（與資料庫定序一致）"""
    return clean(a).casefold() == clean(b).casefold()


# F1/F2 SAP 資料 ------------------------------------------------------------------------------------
def normalize_sap_rows(rows):
    """只保留需要的欄位、去空白、套 ALPHA 轉換"""
    out = []
    for row in rows:
        n = {f: clean(row.get(f)) for f in SAP_FIELDS}
        for f in ALPHA_FIELDS:
            n[f] = alpha_out(n[f])
        out.append(n)
    return out


def validate_sap_rows(rows):
    """檢核不通過即 raise SapValidationError（不重試、不寫入）"""
    if not rows:
        raise SapValidationError("SAP 回傳 0 筆資產資料")

    empty_keys = sum(1 for r in rows if not r.get(SAP_KEY))
    if empty_keys:
        raise SapValidationError(f"SAP 資料有 {empty_keys} 筆資產編號（{SAP_KEY}）為空")

    dup = sorted(k for k, c in Counter(r[SAP_KEY] for r in rows).items() if c > 1)
    if dup:
        raise SapValidationError(f"SAP 資料有 {len(dup)} 個重複資產編號：{', '.join(dup[:20])}")


def split_sap_rows(rows):
    """
    分成可同步與略過（必要欄位空值）兩組
    回傳 (usable: {asset_no: row}, skipped: {asset_no: [缺少的欄位]})
    """
    usable, skipped = {}, {}
    for r in rows:
        missing = [f for f in SAP_REQUIRED_FIELDS if not r.get(f)]
        if missing:
            skipped[r[SAP_KEY]] = missing
        else:
            usable[r[SAP_KEY]] = r
    return usable, skipped


# F4 人事資料 ---------------------------------------------------------------------------------------
def clean_sap_account(value):
    s = clean(value)
    return "" if s.casefold() in JUNK_SAP_ACCOUNTS else s


def is_resigned(member):
    # 只看 Disabled；BPMSysUsers.DateResigned 全部是預設值 1900-01-01，不能用來判斷
    return bool(member.get("Disabled"))


class MemberIndex:
    """BPMSysUsers 的查詢索引：by BPM 帳號（不分大小寫）、by SAP 帳號"""

    def __init__(self, members):
        self.by_account = {}
        self.by_sap = defaultdict(list)
        for m in members:
            account = clean(m.get("Account"))
            if not account:
                continue
            mm = {
                "Account": account,
                "DisplayName": clean(m.get("DisplayName")),
                "SAP_Account": clean_sap_account(m.get("SAP_Account")),
                "resigned": is_resigned(m),
            }
            self.by_account[account.casefold()] = mm
            if mm["SAP_Account"]:
                self.by_sap[mm["SAP_Account"]].append(mm)

    def lookup_account(self, account):
        """回傳 (member, None) 或 (None, 原因)"""
        m = self.by_account.get(clean(account).casefold())
        if m is None:
            return None, "人事資料查無此帳號"
        if m["resigned"]:
            return None, "已離職／帳號停用"
        return m, None

    def lookup_sap_account(self, sap_account):
        """回傳 (member, None) 或 (None, 原因)；同一 SAP 帳號只取在職者，在職者多於一人視為無法判斷"""
        candidates = self.by_sap.get(clean(sap_account), [])
        active = [m for m in candidates if not m["resigned"]]
        if len(active) == 1:
            return active[0], None
        if len(active) > 1:
            return None, "SAP 帳號對到多個 BPM 帳號（{}）".format(", ".join(m["Account"] for m in active))
        if candidates:
            return None, "已離職／帳號停用"
        return None, "人事資料查無此 SAP 帳號"


def build_member_index(members):
    return MemberIndex(members)


# F3 差異比對 ---------------------------------------------------------------------------------------
def diff_asset_fields(master, sap):
    """SAP 負責欄位：回傳 {主檔欄位: 新值}，只含真正有變動的欄位"""
    targets = {}
    for sap_field, col in FIELD_MAP.items():
        new = sap.get(sap_field, "")
        if _is_changed(master.get(col), new):
            targets[col] = new
    return targets


def _refresh_by_account(master, role, index):
    """F4：以 BPM 帳號刷新姓名與 SAP 帳號（不換帳號）。回傳 (targets, anomaly_reason)"""
    account = clean(master.get(role.account_col))
    if not account:
        return {}, ("未設定保管人" if role.from_sap else None)
    member, reason = index.lookup_account(account)
    if member is None:
        return {}, reason
    targets = {}
    if _is_changed(master.get(role.name_col), member["DisplayName"]):
        targets[role.name_col] = member["DisplayName"]
    if _is_changed(master.get(role.sap_col), member["SAP_Account"]):
        targets[role.sap_col] = member["SAP_Account"]
    return targets, None


def diff_keepers(master, sap, index):
    """
    保管人相關欄位
    回傳 (targets: {欄位: 新值}, keeper_change: dict | None, anomalies: [dict])
    """
    targets, anomalies, keeper_change = {}, [], None

    for role in KEEPER_ROLES:
        sap_keeper = sap.get(SAP_KEEPER_FIELD, "") if (sap and role.from_sap) else ""

        if sap_keeper:
            # SAP 為準：KFZKZ → BPMSysUsers.SAP_Account → 換保管人三欄
            member, reason = index.lookup_sap_account(sap_keeper)
            if member is None:
                anomalies.append({"Role": role.label, "Value": sap_keeper,
                                  "Reason": f"SAP 保管人 {sap_keeper}：{reason}"})
                continue
            old_account = clean(master.get(role.account_col))
            if not _same_account(old_account, member["Account"]):
                targets[role.account_col] = member["Account"]
                keeper_change = {
                    "Old_Keeper": old_account,
                    "Old_Keeper_Name": clean(master.get(role.name_col)),
                    "New_Keeper": member["Account"],
                    "New_Keeper_Name": member["DisplayName"],
                    "SAP_Keeper": sap_keeper,
                }
            if _is_changed(master.get(role.name_col), member["DisplayName"]):
                targets[role.name_col] = member["DisplayName"]
            if _is_changed(master.get(role.sap_col), member["SAP_Account"]):
                targets[role.sap_col] = member["SAP_Account"]
        else:
            role_targets, reason = _refresh_by_account(master, role, index)
            targets.update(role_targets)
            if reason:
                anomalies.append({"Role": role.label, "Value": clean(master.get(role.account_col)),
                                  "Reason": reason})

    return targets, keeper_change, anomalies


def compute_sync(master_rows, sap_rows, members):
    """
    主流程：比對主檔、SAP、人事資料，產生寫入內容與報表
    master_rows: 主檔（MASTER_READ_COLUMNS）；sap_rows: 已 normalize + validate 的 SAP 資料；members: BPMSysUsers
    回傳 dict：
      updates  {Instrument_No: {欄位: 新值}}  — 只含有異動的儀器與欄位
      synced   [Instrument_No]                 — 在 SAP 比對成功、要更新 Last_Sync_Date 的儀器
      changes / sap_missing / sap_skipped / keeper_changes / keeper_anomalies  — 報表區塊
      stats                                    — 執行摘要
    """
    usable, skipped = split_sap_rows(sap_rows)
    index = build_member_index(members)

    updates, synced = {}, []
    changes, sap_missing, sap_skipped, keeper_changes, keeper_anomalies = [], [], [], [], []
    master_with_asset = 0

    for m in master_rows:
        no = m["Instrument_No"]
        asset = clean(m.get("Asset_No"))
        base = {"Instrument_No": no, "Asset_No": asset, "Instrument_Name": clean(m.get("Instrument_Name"))}

        sap = None
        if asset:
            master_with_asset += 1
            if asset in usable:
                sap = usable[asset]
                synced.append(no)
            elif asset in skipped:
                sap_skipped.append(dict(base, Reason="SAP 必要欄位空值：" + ", ".join(skipped[asset])))
            else:
                sap_missing.append(dict(base, Reason="SAP 查無此資產（可能已報廢或資產編號錯誤）"))

        targets = diff_asset_fields(m, sap) if sap else {}
        k_targets, k_change, k_anomalies = diff_keepers(m, sap, index)
        targets.update(k_targets)

        if k_change:
            keeper_changes.append(dict(base, **k_change))
        for a in k_anomalies:
            keeper_anomalies.append(dict(base, **a))

        bad = set(targets) - WRITABLE_COLUMNS
        if bad:  # 防呆：規則寫錯也不可能寫到白名單以外的欄位
            raise RuntimeError(f"規則產生白名單以外的欄位：{sorted(bad)}")

        if targets:
            updates[no] = targets
            for col, new in targets.items():
                changes.append(dict(base, Field=col, Field_Label=FIELD_LABELS.get(col, col),
                                    Old=clean(m.get(col)), New=new))

    stats = {
        "sap_rows": len(sap_rows),
        "master_rows": len(master_rows),
        "master_with_asset": master_with_asset,
        "matched": len(synced),
        "changed_instruments": len(updates),
        "change_count": len(changes),
        "field_counts": dict(Counter(c["Field"] for c in changes)),
        "sap_missing": len(sap_missing),
        "sap_skipped": len(sap_skipped),
        "keeper_changes": len(keeper_changes),
        "keeper_anomalies": len(keeper_anomalies),
    }
    return {
        "updates": updates,
        "synced": synced,
        "changes": changes,
        "sap_missing": sap_missing,
        "sap_skipped": sap_skipped,
        "keeper_changes": keeper_changes,
        "keeper_anomalies": keeper_anomalies,
        "stats": stats,
    }
