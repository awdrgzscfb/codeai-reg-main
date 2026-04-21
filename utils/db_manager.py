import sqlite3
import pymysql
import json
import os
from datetime import datetime
from typing import Any

from utils.config import DB_TYPE, MYSQL_CFG

os.makedirs("data", exist_ok=True)
DB_PATH = "data/data.db"


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")
class get_db_conn:
    """æ¹å¹³ SQLite å MySQL è¿æ¥å·®å¼"""
    def __init__(self, as_dict=False):
        self.as_dict = as_dict

    def __enter__(self):
        if DB_TYPE == "mysql":
            self.conn = pymysql.connect(
                host=MYSQL_CFG.get('host', '127.0.0.1'),
                port=MYSQL_CFG.get('port', 3306),
                user=MYSQL_CFG.get('user', 'root'),
                password=MYSQL_CFG.get('password', ''),
                database=MYSQL_CFG.get('db_name', 'wenfxl_manager'),
                charset='utf8mb4'
            )
        else:
            self.conn = sqlite3.connect(DB_PATH, timeout=10)
            if self.as_dict:
                self.conn.row_factory = sqlite3.Row
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()


def get_cursor(conn, as_dict=False):
    """è·åééçæ¸¸æ """
    if DB_TYPE == "mysql" and as_dict:
        return conn.cursor(pymysql.cursors.DictCursor)
    return conn.cursor()


def execute_sql(cursor, sql: str, params=()):
    if DB_TYPE == "mysql":
        sql = sql.replace('?', '%s')
        sql = sql.replace('AUTOINCREMENT', 'AUTO_INCREMENT')

        sql = sql.replace('INSERT OR IGNORE', 'INSERT IGNORE')
        sql = sql.replace('INSERT OR REPLACE', 'REPLACE')

        sql = sql.replace('TEXT UNIQUE', 'VARCHAR(191) UNIQUE')
        sql = sql.replace('TEXT PRIMARY KEY', 'VARCHAR(191) PRIMARY KEY')

        # 3. æ¹å¹³ç¹æ®ç PRAGMA
        if 'PRAGMA' in sql:
            return None

    return cursor.execute(sql, params)

def init_db():
    """åå§åæ°æ®åºï¼èªå¨éåºåå¼æå»ºè¡¨"""
    with get_db_conn() as conn:
        c = get_cursor(conn)
        execute_sql(c, 'PRAGMA journal_mode=WAL;')
        execute_sql(c, 'PRAGMA synchronous=NORMAL;')

        execute_sql(c, '''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                password TEXT,
                token_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        execute_sql(c, '''
            CREATE TABLE IF NOT EXISTS system_kv (
                `key` TEXT PRIMARY KEY, 
                value TEXT
            )
        ''')
        execute_sql(c, '''
            CREATE TABLE IF NOT EXISTS local_mailboxes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                password TEXT,
                client_id TEXT,
                refresh_token TEXT,
                status INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        execute_sql(c, '''
            CREATE TABLE IF NOT EXISTS local_imap_mailboxes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                password TEXT,
                imap_server TEXT,
                imap_port INTEGER DEFAULT 993,
                use_ssl INTEGER DEFAULT 1,
                provider TEXT DEFAULT '',
                status TEXT DEFAULT 'idle',
                in_use_by TEXT DEFAULT '',
                locked_at TIMESTAMP NULL,
                last_used_at TIMESTAMP NULL,
                last_check_at TIMESTAMP NULL,
                last_error TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        try:
            execute_sql(c, 'ALTER TABLE local_mailboxes ADD COLUMN fission_count INTEGER DEFAULT 0;')
            execute_sql(c, 'ALTER TABLE local_mailboxes ADD COLUMN retry_master INTEGER DEFAULT 0;')
        except Exception:
            pass
    print(f"[{ts()}] [ç³»ç»] æ°æ®åºæ¨¡ååå§åå®æ (å¼æ: {DB_TYPE.upper()})")


def save_account_to_db(email: str, password: str, token_json_str: str) -> bool:
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, '''
                INSERT OR REPLACE INTO accounts (email, password, token_data)
                VALUES (?, ?, ?)
            ''', (email, password, token_json_str))
            return True
    except Exception as e:
        print(f"[{ts()}] [ERROR] æ°æ®åºä¿å­å¤±è´¥: {e}")
        return False


def get_all_accounts() -> list:
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "SELECT email, password, created_at FROM accounts ORDER BY id DESC")
            rows = c.fetchall()
            # MySQL é»è®¤æ¸¸æ è¿åçä¹æ¯åç»ï¼å¼å®¹åçåçé»è¾
            return [{"email": r[0], "password": r[1], "created_at": r[2]} for r in rows]
    except Exception as e:
        print(f"[{ts()}] [ERROR] è·åè´¦å·åè¡¨å¤±è´¥: {e}")
        return []


def get_token_by_email(email: str) -> dict:
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "SELECT token_data FROM accounts WHERE email = ?", (email,))
            row = c.fetchone()
            if row and row[0]:
                return json.loads(row[0])
            return None
    except Exception as e:
        print(f"[{ts()}] [ERROR] è¯»å Token å¤±è´¥: {e}")
        return None


def get_tokens_by_emails(emails: list) -> list:
    if not emails: return []
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            placeholders = ','.join(['?'] * len(emails))
            execute_sql(c, f"SELECT token_data FROM accounts WHERE email IN ({placeholders})", tuple(emails))
            rows = c.fetchall()

            export_list = []
            for r in rows:
                if r[0]:
                    try:
                        export_list.append(json.loads(r[0]))
                    except:
                        pass
            return export_list
    except Exception as e:
        return []


def delete_accounts_by_emails(emails: list) -> bool:
    if not emails: return True
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            placeholders = ','.join(['?'] * len(emails))
            execute_sql(c, f"DELETE FROM accounts WHERE email IN ({placeholders})", tuple(emails))
            return True
    except Exception as e:
        print(f"[{ts()}] [ERROR] æ°æ®åºæ¹éå é¤è´¦å·å¼å¸¸: {e}")
        return False


def get_accounts_page(page: int = 1, page_size: int = 50) -> dict:
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "SELECT COUNT(1) FROM accounts")
            total = c.fetchone()[0]

            offset = (page - 1) * page_size
            execute_sql(c, "SELECT email, password, created_at, token_data FROM accounts ORDER BY id DESC LIMIT ? OFFSET ?",
                        (page_size, offset))
            rows = c.fetchall()

            data = [
                {
                    "email": r[0],
                    "password": r[1],
                    "created_at": r[2],
                    "status": "æå­è¯" if '"access_token"' in str(r[3] or "") else (
                        "ä»æ³¨åæå" if '"ä»æ³¨åæå"' in str(r[3] or "") else "æªç¥")
                }
                for r in rows
            ]
            return {"total": total, "data": data}
    except Exception as e:
        print(f"[{ts()}] [ERROR] åé¡µè·åè´¦å·åè¡¨å¤±è´¥: {e}")
        return {"total": 0, "data": []}


def set_sys_kv(key: str, value: Any):
    try:
        val_str = json.dumps(value, ensure_ascii=False)
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "INSERT OR REPLACE INTO system_kv (`key`, value) VALUES (?, ?)", (key, val_str))
    except Exception as e:
        print(f"[{ts()}] [ERROR] ç³»ç»éç½®ä¿å­å¤±è´¥: {e}")


def get_sys_kv(key: str, default=None):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "SELECT value FROM system_kv WHERE `key` = ?", (key,))
            row = c.fetchone()
            if row:
                return json.loads(row[0])
    except Exception:
        pass
    return default


def get_all_accounts_with_token(limit: int = 10000) -> list:
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "SELECT email, password, token_data FROM accounts ORDER BY id DESC LIMIT ?", (limit,))
            rows = c.fetchall()
            return [{"email": r[0], "password": r[1], "token_data": r[2]} for r in rows]
    except Exception as e:
        print(f"[{ts()}] [ERROR] æåå®æ´è´¦å·æ°æ®å¤±è´¥: {e}")
        return []


def import_local_mailboxes(mailboxes_data: list) -> int:
    count = 0
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            for mb in mailboxes_data:
                try:
                    execute_sql(c, '''
                        INSERT OR IGNORE INTO local_mailboxes (email, password, client_id, refresh_token, status)
                        VALUES (?, ?, ?, ?, 0)
                    ''', (mb['email'], mb['password'], mb.get('client_id', ''), mb.get('refresh_token', '')))
                    if c.rowcount > 0:
                        count += 1
                except:
                    pass
    except Exception as e:
        print(f"[{ts()}] [ERROR] å¯¼å¥é®ç®±åºå¤±è´¥: {e}")
    return count


def get_local_mailboxes_page(page: int = 1, page_size: int = 50) -> dict:
    try:
        # as_dict=True éç¥æ¸¸æ è¿åå­å¸æ ¼å¼ï¼ééåæ¥ç sqlite3.Row
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)
            execute_sql(c, "SELECT COUNT(1) AS cnt FROM local_mailboxes")
            total_row = c.fetchone()
            total = total_row['cnt'] if DB_TYPE == "mysql" else total_row[0]

            offset = (page - 1) * page_size
            execute_sql(c, "SELECT * FROM local_mailboxes ORDER BY id DESC LIMIT ? OFFSET ?", (page_size, offset))
            rows = c.fetchall()
            return {"total": total, "data": [dict(r) for r in rows]}
    except Exception as e:
        return {"total": 0, "data": []}


def delete_local_mailboxes(ids: list) -> bool:
    if not ids: return True
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            placeholders = ','.join(['?'] * len(ids))
            execute_sql(c, f"DELETE FROM local_mailboxes WHERE id IN ({placeholders})", tuple(ids))
            return True
    except Exception as e:
        return False

def get_and_lock_unused_local_mailbox() -> dict:
    """æåä¸ä¸ªæªä½¿ç¨çè´¦å·ï¼å¹¶ç¶æéå®ä¸ºå ç¨ä¸­"""
    try:
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)

            filter_sql = """
                SELECT * FROM local_mailboxes 
                WHERE status = 0 
                AND email NOT IN (SELECT email FROM accounts) 
                ORDER BY id ASC LIMIT 1
            """

            if DB_TYPE == "mysql":
                execute_sql(c, "START TRANSACTION")
                execute_sql(c, filter_sql + " FOR UPDATE")
            else:
                execute_sql(c, "BEGIN EXCLUSIVE")
                execute_sql(c, filter_sql)

            row = c.fetchone()
            if row:
                execute_sql(c, "UPDATE local_mailboxes SET status = 1 WHERE id = ?", (row['id'],))
                return dict(row)
            return None
    except Exception as e:
        print(f"[{ts()}] [ERROR] æåæ¬å°é®ç®±å¤±è´¥: {e}")
        return None


def get_mailbox_for_pool_fission() -> dict:
    """å¸¦éè¯ä¼åçº§çå¹¶ååå·"""
    try:
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)
            if DB_TYPE == "mysql":
                execute_sql(c, "START TRANSACTION")
                execute_sql(c, "SELECT * FROM local_mailboxes WHERE status = 0 AND retry_master = 1 LIMIT 1 FOR UPDATE")
            else:
                execute_sql(c, "BEGIN EXCLUSIVE")
                execute_sql(c, "SELECT * FROM local_mailboxes WHERE status = 0 AND retry_master = 1 LIMIT 1")

            row = c.fetchone()

            if not row:
                if DB_TYPE == "mysql":
                    execute_sql(c,
                                "SELECT * FROM local_mailboxes WHERE status = 0 ORDER BY fission_count ASC LIMIT 1 FOR UPDATE")
                else:
                    execute_sql(c, "SELECT * FROM local_mailboxes WHERE status = 0 ORDER BY fission_count ASC LIMIT 1")
                row = c.fetchone()

            if row:
                execute_sql(c, "UPDATE local_mailboxes SET fission_count = fission_count + 1 WHERE id = ?",
                            (row['id'],))
                return dict(row)
            return None
    except Exception as e:
        print(f"[{ts()}] [DB_ERROR] æåå¤±è´¥: {e}")
        return None


def update_local_mailbox_status(email: str, status: int):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "UPDATE local_mailboxes SET status = ? WHERE email = ?", (status, email))
    except Exception:
        pass

def update_local_mailbox_refresh_token(email: str, new_rt: str):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "UPDATE local_mailboxes SET refresh_token = ? WHERE email = ?", (new_rt, email))
    except Exception:
        pass


def update_pool_fission_result(email: str, is_blocked: bool, is_raw: bool):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            if not is_blocked:
                execute_sql(c, "UPDATE local_mailboxes SET retry_master = 0 WHERE email = ?", (email,))
            else:
                if not is_raw:
                    execute_sql(c, "UPDATE local_mailboxes SET retry_master = 1 WHERE email = ?", (email,))
                else:
                    execute_sql(c, "UPDATE local_mailboxes SET status = 3, retry_master = 0 WHERE email = ?", (email,))
    except Exception as e:
        print(f"[{ts()}] [DB_ERROR] ç»ææ´æ°å¤±è´¥: {e}")

def clear_retry_master_status(email: str):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "UPDATE local_mailboxes SET retry_master = 0 WHERE email = ?", (email,))
    except Exception as e:
        print(f"[{ts()}] [DB_ERROR] æ¸é¤ {email} ç retry_master ç¶æå¤±è´¥: {e}")

def get_all_accounts_raw() -> list:
    """è·åè´¦å·åºææåå§æ°æ®"""
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "SELECT email, password, token_data FROM accounts ORDER BY id DESC")
            rows = c.fetchall()
            return [{"email": r[0], "password": r[1], "token_data": json.loads(r[2]) if r[2] else {}} for r in rows]
    except: return []

def clear_all_accounts() -> bool:
    """ä¸é®æ¸ç©ºè´¦å·åº"""
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "DELETE FROM accounts")
            return True
    except: return False

def get_all_mailboxes_raw() -> list:
    """è·åé®ç®±åºææåå§æ°æ®"""
    try:
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)
            execute_sql(c, "SELECT * FROM local_mailboxes ORDER BY id DESC")
            return [dict(r) for r in c.fetchall()]
    except: return []

def clear_all_mailboxes() -> bool:
    """ä¸é®æ¸ç©ºé®ç®±åº"""
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "DELETE FROM local_mailboxes")
            return True
    except: return False

def import_local_imap_mailboxes(mailboxes_data: list) -> tuple[int, list]:
    count = 0
    errors = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            for idx, mb in enumerate(mailboxes_data, 1):
                try:
                    execute_sql(c, '''
                        INSERT OR IGNORE INTO local_imap_mailboxes
                        (email, password, imap_server, imap_port, use_ssl, provider, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, 'idle', ?, ?)
                    ''', (
                        mb['email'],
                        mb['password'],
                        mb['imap_server'],
                        int(mb.get('imap_port', 993) or 993),
                        1 if mb.get('use_ssl', True) else 0,
                        mb.get('provider', ''),
                        now,
                        now,
                    ))
                    if c.rowcount > 0:
                        count += 1
                    else:
                        errors.append(f"µÚ {idx} ÐÐÖØ¸´: {mb['email']}")
                except Exception as e:
                    errors.append(f"µÚ {idx} ÐÐµ¼ÈëÊ§°Ü: {e}")
    except Exception as e:
        errors.append(f"Êý¾Ý¿âµ¼ÈëÊ§°Ü: {e}")
    return count, errors


def get_local_imap_mailboxes_page(page: int = 1, page_size: int = 50, status: str = "", keyword: str = "") -> dict:
    try:
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)
            where_parts = []
            params = []
            if status:
                where_parts.append("status = ?")
                params.append(status)
            if keyword:
                where_parts.append("email LIKE ?")
                params.append(f"%{keyword}%")
            where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

            execute_sql(c, f"SELECT COUNT(1) AS cnt FROM local_imap_mailboxes{where_sql}", tuple(params))
            total_row = c.fetchone()
            total = total_row['cnt'] if DB_TYPE == "mysql" else total_row[0]

            offset = (page - 1) * page_size
            execute_sql(c,
                        f"SELECT * FROM local_imap_mailboxes{where_sql} ORDER BY id DESC LIMIT ? OFFSET ?",
                        tuple(params + [page_size, offset]))
            rows = c.fetchall()
            return {"total": total, "data": [dict(r) for r in rows]}
    except Exception as e:
        print(f"[{ts()}] [ERROR] »ñÈ¡IMAPºÅ³Ø·ÖÒ³Ê§°Ü: {e}")
        return {"total": 0, "data": []}


def delete_local_imap_mailboxes(ids: list) -> bool:
    if not ids:
        return True
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            placeholders = ','.join(['?'] * len(ids))
            execute_sql(c, f"DELETE FROM local_imap_mailboxes WHERE id IN ({placeholders})", tuple(ids))
            return True
    except Exception as e:
        print(f"[{ts()}] [ERROR] É¾³ýIMAPºÅ³ØÊ§°Ü: {e}")
        return False


def get_and_lock_unused_local_imap_mailbox(reuse_used: bool = False, lock_owner: str = "") -> dict:
    try:
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)
            statuses = ["idle"]
            if reuse_used:
                statuses.append("used")
            placeholders = ",".join(["?"] * len(statuses))

            if DB_TYPE == "mysql":
                execute_sql(c, "START TRANSACTION")
                execute_sql(c,
                            f"SELECT * FROM local_imap_mailboxes WHERE status IN ({placeholders}) ORDER BY id ASC LIMIT 1 FOR UPDATE",
                            tuple(statuses))
            else:
                execute_sql(c, "BEGIN EXCLUSIVE")
                execute_sql(c,
                            f"SELECT * FROM local_imap_mailboxes WHERE status IN ({placeholders}) ORDER BY id ASC LIMIT 1",
                            tuple(statuses))

            row = c.fetchone()
            if not row:
                return None

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            execute_sql(c,
                        "UPDATE local_imap_mailboxes SET status = ?, in_use_by = ?, locked_at = ?, updated_at = ?, last_error = '' WHERE id = ?",
                        ("using", str(lock_owner or ""), now, now, row["id"]))
            result = dict(row)
            result["status"] = "using"
            result["in_use_by"] = str(lock_owner or "")
            result["locked_at"] = now
            return result
    except Exception as e:
        print(f"[{ts()}] [ERROR] ·ÖÅäIMAPºÅ³ØÓÊÏäÊ§°Ü: {e}")
        return None


def release_local_imap_mailbox(mailbox_id: int, to_status: str = "idle", last_error: str = ""):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            execute_sql(c,
                        "UPDATE local_imap_mailboxes SET status = ?, in_use_by = '', locked_at = NULL, updated_at = ?, last_error = ? WHERE id = ?",
                        (to_status, now, str(last_error or ""), mailbox_id))
    except Exception as e:
        print(f"[{ts()}] [ERROR] ÊÍ·ÅIMAPºÅ³ØÓÊÏäÊ§°Ü: {e}")


def update_local_imap_mailbox_status_by_email(email: str, status: str, last_error: str = ""):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            execute_sql(c,
                        "UPDATE local_imap_mailboxes SET status = ?, in_use_by = '', locked_at = NULL, updated_at = ?, last_error = ? WHERE email = ?",
                        (status, now, str(last_error or ""), email))
    except Exception as e:
        print(f"[{ts()}] [ERROR] Í¨¹ýÓÊÏä¸üÐÂIMAPºÅ³Ø×´Ì¬Ê§°Ü: {e}")


def mark_local_imap_mailbox_success(mailbox_id: int):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            execute_sql(c,
                        "UPDATE local_imap_mailboxes SET status = 'used', in_use_by = '', locked_at = NULL, last_used_at = ?, updated_at = ?, last_error = '' WHERE id = ?",
                        (now, now, mailbox_id))
    except Exception as e:
        print(f"[{ts()}] [ERROR] ±ê¼ÇIMAPºÅ³Ø³É¹¦Ê§°Ü: {e}")


def mark_local_imap_mailbox_invalid(mailbox_id: int, error: str = ""):
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            execute_sql(c,
                        "UPDATE local_imap_mailboxes SET status = 'invalid', in_use_by = '', locked_at = NULL, updated_at = ?, last_check_at = ?, last_error = ? WHERE id = ?",
                        (now, now, str(error or ""), mailbox_id))
    except Exception as e:
        print(f"[{ts()}] [ERROR] ±ê¼ÇIMAPºÅ³ØÊ§Ð§Ê§°Ü: {e}")


def batch_update_local_imap_mailboxes_status(ids: list, status: str) -> bool:
    if not ids:
        return True
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            placeholders = ",".join(["?"] * len(ids))
            execute_sql(c,
                        f"UPDATE local_imap_mailboxes SET status = ?, in_use_by = '', locked_at = NULL, updated_at = ? WHERE id IN ({placeholders})",
                        tuple([status, now] + list(ids)))
            return True
    except Exception as e:
        print(f"[{ts()}] [ERROR] ÅúÁ¿¸üÐÂIMAPºÅ³Ø×´Ì¬Ê§°Ü: {e}")
        return False


def get_local_imap_mailbox_by_id(mailbox_id: int) -> dict:
    try:
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)
            execute_sql(c, "SELECT * FROM local_imap_mailboxes WHERE id = ?", (mailbox_id,))
            row = c.fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def get_all_local_imap_mailboxes_raw() -> list:
    try:
        with get_db_conn(as_dict=True) as conn:
            c = get_cursor(conn, as_dict=True)
            execute_sql(c, "SELECT * FROM local_imap_mailboxes ORDER BY id DESC")
            return [dict(r) for r in c.fetchall()]
    except Exception:
        return []


def clear_all_local_imap_mailboxes() -> bool:
    try:
        with get_db_conn() as conn:
            c = get_cursor(conn)
            execute_sql(c, "DELETE FROM local_imap_mailboxes")
            return True
    except Exception as e:
        print(f"[{ts()}] [ERROR] Çå¿ÕIMAPºÅ³ØÊ§°Ü: {e}")
        return False
