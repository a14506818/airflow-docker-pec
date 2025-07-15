import pandas as pd
import pyodbc

from src.common.common import get_mssql_conn_str

class DBHandler:
    def __init__(self):
        """
        初始化資料庫連線
        """
        conn_str = get_mssql_conn_str()
        self.conn = pyodbc.connect(conn_str, timeout=5)
        self.cursor = self.conn.cursor()
        print("✅ MSSQL 連線成功")

    def shutdown(self):
        """
        關閉資料庫連線
        """
        self.cursor.close()
        self.conn.close()
        print("✅ MSSQL 連線已關閉")

    def del_tmp_table(self):
        """
        刪除暫存資料表
        """
        query = "TRUNCATE TABLE TMP_MOL_compliance_result;"
        print("SQL:", query)
        self.cursor.execute(query)
        
        query = "TRUNCATE TABLE TMP_ENV_compliance_result;"
        print("SQL:", query)
        self.cursor.execute(query)

        self.conn.commit()
        print("✅ 刪除暫存資料表成功")

    def get_partner_list(self):
        """
        取得 REF 供應商評估任務的爬蟲清單
        """
        query = "SELECT partner_name FROM REF_partner_crawler_list"  # REF_partner_crawler_list or  REF_partner_crawler_list_test
        print("SQL:", query)
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if not rows:
            raise ValueError("❌ REF_partner_crawler_list 資料表為空，請檢查資料庫")
        
        # 把 tuple 拆開
        company_names = [str(r[0]).strip() for r in rows if r[0]]
        company_names = list(set(company_names))  # 去重複，非必要但

        print("✅ 取得公司名稱清單成功:", company_names)
        return company_names

    def insert_job(self, job: dict):
        """
        寫入 JOB 資料表
        """
        print("Job id:", job)
        self.cursor.execute("""
            INSERT INTO JOB_compliance_crawler (id, run_key, supplier_name, source_type, status)
            VALUES (?, ?, ?, ?, ?)
        """, 
        job['id'], job['run_key'], job['supplier_name'], job['source_type'], 'pending')

        self.conn.commit()
        print("✅ 寫入 JOB 成功")

    def update_job_status(self, job_id: str, status: str, error_msg: str = None):
        """
        更新 JOB 資料表的狀態
        """
        self.cursor.execute("""
            UPDATE JOB_compliance_crawler
            SET status = ?, error_msg = ?, ended_at = GETDATE()
            WHERE id = ?
        """, status, error_msg, job_id)

        self.conn.commit()
        print(f"✅ 更新 JOB {job_id} 狀態為 {status} 成功")

    def insert_tmp_result(self, df: pd.DataFrame, source: str):
        """將查詢結果寫入 TMP__compliance_result"""
        table_name = f"TMP_{source}_compliance_result"
        if df.empty:
            print("⚠️ 傳入空的 DataFrame，未執行寫入。")
            return
        else:
            print(f"✅ df讀取成功: ", df)

        MOL_COLUMNS_MAPPING = {
            "run_key": "run_key",
            "job_id": "job_id",
            "查詢名稱": "search_name",
            "法律類別": "law_type",
            "序號": "seq",
            "縣市/單位別": "country",
            "公告日期": "announce_date",
            "處分日期": "penalty_date",
            "處分字號": "penalty_no",
            "事業單位名稱(負責人)自然人姓名": "partner_name",
            "違法法規法條": "law",
            "違反法規內容": "content",
            "備註說明": "remark",
            "罰鍰金額": "fine"  
        }
        ENV_COLUMNS_MAPPING = {
            "run_key": "run_key",
            "job_id": "job_id",
            "統一編號": "partner",
            "供應商名稱": "partner_name",
            "裁罰日": "penalty_date",
            "違規日": "violation_date",
            "縣市": "country",
            "裁罰內容": "description",
            "訴願狀態": "status",
            "限改日期-改善完妥": "refine",
            "裁罰金額": "fine_amount",
            "裁罰備註": "fine_description"  
        }
        #  source : MOL or ENV
        if source == "MOL":
            COLUMNS_MAPPING = MOL_COLUMNS_MAPPING
        elif source == "ENV":
            COLUMNS_MAPPING = ENV_COLUMNS_MAPPING
        else:
            raise ValueError("Invalid source type. Expected 'MOL' or 'ENV'.")
        # 欄位轉換
        df = df.rename(columns=COLUMNS_MAPPING)
        # 只保留 COLUMNS_MAPPING 定義的欄位
        keep_cols = list(COLUMNS_MAPPING.values())  # 取 rename 後的新欄位名
        df = df[keep_cols]
        # 將所有欄位的值轉為字串 （NaN -> ''）
        df = df.fillna('').astype(str)

        # 寫入新資料
        for index, row in df.iterrows():
            print(f"SQL: INSERT INTO {table_name} ({', '.join(row.index)}) VALUES ({', '.join(['?' for _ in row])})")   
            self.cursor.execute(f"""
                INSERT INTO {table_name} ({', '.join(row.index)})
                VALUES ({', '.join(['?' for _ in row])})
            """, tuple(row))
            
            
        self.conn.commit()
        print(f"✅ 寫入 {len(df)} 筆至 {table_name}")

    def copy_tmp_to_his_and_prd(self, source: str):
        tmp_table = f"TMP_{source}_compliance_result"
        his_table = f"HIS_{source}_compliance_result"
        prd_table = f"PRD_{source}_compliance_result"

        exclude_cols = {"id", "inserted_at"}

        # 取欄位名稱
        cols = [row.column_name for row in self.cursor.columns(tmp_table)]
        filtered_cols = [col for col in cols if col not in exclude_cols]
        col_str = ', '.join(filtered_cols)

        # Append TMP → HIS
        query = f"""
            INSERT INTO {his_table} ({col_str})
            SELECT {col_str}
            FROM {tmp_table}
        """
        print("SQL:", query)
        self.cursor.execute(query)
        print(f"✅ 複製 TMP 資料至 {his_table} 成功")

        # Truncate PRD
        query = f"TRUNCATE TABLE {prd_table}"
        print("SQL:", query)
        self.cursor.execute(query)
        print(f"✅ 清空 {prd_table} 成功")

        # Insert TMP → PRD
        query = f"""
            INSERT INTO {prd_table} ({col_str})
            SELECT {col_str}
            FROM {tmp_table}
        """
        print("SQL:", query)
        self.cursor.execute(query)
        print(f"✅ 複製 TMP 資料至 {prd_table} 成功")

        self.conn.commit()
        print("✅ 複製 TMP 資料至 HIS PRD 成功")

    def get_specific_table(self, table_name: str):
        """
        取得特定資料表的所有資料
        """
        query = f"SELECT * FROM {table_name}"
        print("SQL:", query)
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if not rows:
            raise ValueError(f"❌ {table_name} 資料表為空，請檢查資料庫")
        
        # 將結果轉為 DataFrame
        df = pd.DataFrame.from_records(rows, columns=[column[0] for column in self.cursor.description])
        print(f"✅ 取得 {table_name} 資料成功")
        return df