import os
import sys

# 讓測試可以 `import src.xxx`（Airflow 執行時會自動把 plugins 加進 sys.path）
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
