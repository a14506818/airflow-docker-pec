import pytest

from src.instrument_sync.loader import is_dry_run, write_updates


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, *params):
        self.conn.executed.append((" ".join(sql.split()), params))
        if self.conn.fail_on is not None and len(self.conn.executed) == self.conn.fail_on:
            raise RuntimeError("模擬寫入失敗")


class FakeConn:
    def __init__(self, fail_on=None):
        self.executed, self.fail_on = [], fail_on
        self.committed = self.rolled_back = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


UPDATES = {1: {"Location_Code": "C005", "Location_Name": "19樓"}, 2: {"Keeper_Name": "王小美"}}


def test_write_sets_session_context_first_and_commits():
    conn = FakeConn()
    write_updates(conn, UPDATES, [1, 2, 3], "2026/11/19 02:00")
    sql, params = conn.executed[0]
    assert "sp_set_session_context" in sql
    assert params == ("InstrumentChangeSource", "SAP_SYNC")
    assert conn.committed and not conn.rolled_back
    # 2 筆欄位異動 + 3 筆 Last_Sync_Date
    assert len(conn.executed) == 1 + 2 + 3


def test_update_sql_sets_updated_by_only_on_changed_rows():
    conn = FakeConn()
    write_updates(conn, UPDATES, [1, 2, 3], "2026/11/19 02:00")
    sql, params = conn.executed[1]
    assert sql.startswith("UPDATE dbo.REF_Instrument_Master SET [Location_Code] = ?, [Location_Name] = ?, "
                          "[Updated_By] = ? WHERE [Instrument_No] = ?")
    assert params == ("C005", "19樓", "SAP_SYNC", 1)
    sync_sqls = [s for s, _ in conn.executed if "Last_Sync_Date" in s]
    assert len(sync_sqls) == 3
    assert all("Updated_By" not in s for s in sync_sqls)


def test_failure_mid_write_rolls_back_everything():
    conn = FakeConn(fail_on=3)  # session context、第 1 筆成功，第 2 筆失敗
    with pytest.raises(RuntimeError, match="模擬寫入失敗"):
        write_updates(conn, UPDATES, [1, 2], "2026/11/19 02:00")
    assert conn.rolled_back
    assert not conn.committed


def test_non_whitelisted_column_is_rejected_and_rolled_back():
    conn = FakeConn()
    with pytest.raises(RuntimeError, match="白名單"):
        write_updates(conn, {1: {"is_GMP": "N"}}, [], "2026/11/19 02:00")
    assert conn.rolled_back and not conn.committed


@pytest.mark.parametrize("value,expected", [
    (True, True), (False, False), ("true", True), ("False", False), (None, False),
])
def test_is_dry_run(value, expected):
    assert is_dry_run({"dry_run": value}) is expected


def test_is_dry_run_defaults_false():
    assert is_dry_run({}) is False
    assert is_dry_run(None) is False
