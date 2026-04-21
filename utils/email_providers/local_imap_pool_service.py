import imaplib
import json
import re
import socket
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import socks

from utils import config as cfg
from utils import db_manager


class ProxyIMAP4_SSL(imaplib.IMAP4_SSL):
    def __init__(self, host, port, proxy_url=None, **kwargs):
        self.proxy_url = proxy_url
        super().__init__(host, port, **kwargs)

    def _create_socket(self, timeout):
        if not self.proxy_url:
            return socket.create_connection((self.host, self.port), timeout)

        parsed = urlparse(self.proxy_url)
        p_type = socks.SOCKS5 if 'socks5' in parsed.scheme.lower() else socks.HTTP
        proxy_port = parsed.port or (1080 if p_type == socks.SOCKS5 else 8080)
        sock = socks.socksocket(socket.AF_INET, socket.SOCK_STREAM)
        sock.set_proxy(p_type, parsed.hostname, proxy_port, rdns=True)
        sock.settimeout(timeout)
        sock.connect((self.host, self.port))
        return sock


OTP_CODE_PATTERN = r"(?<!\d)(\d{6})(?!\d)"


def extract_otp_code(content: str) -> str:
    if not content:
        return ""
    patterns = [
        r"(?i)Your ChatGPT code is\s*(\d{6})",
        r"(?i)ChatGPT code is\s*(\d{6})",
        r"(?i)verification code to continue:\s*(\d{6})",
        r"(?i)Subject:.*?(\d{6})",
    ]
    for pattern in patterns:
        m = re.search(pattern, content)
        if m:
            return m.group(1)
    m = re.search(OTP_CODE_PATTERN, content)
    return m.group(1) if m else ""


def _mask_email(email: str) -> str:
    if not email or "@" not in email:
        return email or ""
    name, _ = email.split("@", 1)
    keep = 3 if len(name) > 3 else max(1, len(name))
    return f"{name[:keep]}***@***.***"


def _proxy_str(proxies: Any = None) -> Optional[str]:
    mail_proxies = proxies if cfg.USE_PROXY_FOR_EMAIL else None
    if not mail_proxies:
        return None
    if isinstance(mail_proxies, dict):
        return mail_proxies.get("https") or mail_proxies.get("http")
    return str(mail_proxies)


def create_imap_conn(server: str, port: int, proxy_str: Optional[str] = None):
    if proxy_str:
        return ProxyIMAP4_SSL(server, port, proxy_url=proxy_str, timeout=15)
    return imaplib.IMAP4_SSL(server, port, timeout=15)


def get_unused_mailbox(lock_owner: str = "") -> Optional[dict]:
    mailbox = db_manager.get_and_lock_unused_local_imap_mailbox(
        reuse_used=getattr(cfg, "LOCAL_IMAP_POOL_REUSE_USED_MAILBOX", False),
        lock_owner=lock_owner,
    )
    if not mailbox:
        return None
    mailbox["assigned_at"] = mailbox.get("locked_at")
    return mailbox


def test_mailbox_login(mailbox: Dict[str, Any], proxies: Any = None) -> tuple[bool, str]:
    server = str(mailbox.get("imap_server", "")).strip()
    port = int(mailbox.get("imap_port", 993) or 993)
    user = str(mailbox.get("email", "")).strip()
    password = str(mailbox.get("password", "")).replace(" ", "")
    if not server or not user or not password:
        return False, "IMAP 配置不完整"

    conn = None
    try:
        conn = create_imap_conn(server, port, _proxy_str(proxies))
        conn.login(user, password)
        status, data = conn.select("INBOX", readonly=True)
        if status != "OK":
            return False, f"登录成功，但打开 INBOX 失败: {data}"
        return True, "IMAP 登录测试成功"
    except Exception as e:
        return False, str(e)
    finally:
        if conn:
            try:
                conn.logout()
            except Exception:
                pass


def wait_for_verification_code(mailbox: Dict[str, Any], target_email: str, *, max_attempts: int = 20, proxies: Any = None) -> str:
    proxy_str = _proxy_str(proxies)
    processed_mail_ids = set()
    conn = None
    server = str(mailbox.get("imap_server", "")).strip()
    port = int(mailbox.get("imap_port", 993) or 993)
    user = str(mailbox.get("email", "")).strip()
    password = str(mailbox.get("password", "")).replace(" ", "")
    mailbox_id = mailbox.get("id")

    for attempt in range(max_attempts):
        if getattr(cfg, 'GLOBAL_STOP', False):
            return ""
        try:
            if not conn:
                conn = create_imap_conn(server, port, proxy_str)
                conn.login(user, password)

            folders = ["INBOX", "Junk", '"Junk Email"', "Spam", '"[Gmail]/Spam"', '"垃圾邮件"']
            for folder in folders:
                try:
                    conn.noop()
                    status, _ = conn.select(folder, readonly=True)
                    if status != "OK":
                        continue
                    status, messages = conn.search(None, '(UNSEEN FROM "openai.com")')
                    if status != "OK" or not messages or not messages[0]:
                        continue

                    for mail_id in reversed(messages[0].split()):
                        if mail_id in processed_mail_ids:
                            continue
                        fetch_status, data = conn.fetch(mail_id, "(RFC822)")
                        if fetch_status != "OK":
                            processed_mail_ids.add(mail_id)
                            continue
                        for resp_part in data:
                            if not isinstance(resp_part, tuple):
                                continue
                            import email as email_lib
                            msg = email_lib.message_from_bytes(resp_part[1])
                            subject = str(msg.get("Subject", ""))
                            content = ""
                            if msg.is_multipart():
                                for part in msg.walk():
                                    if part.get_content_type() == "text/plain":
                                        try:
                                            content += part.get_payload(decode=True).decode("utf-8", "ignore")
                                        except Exception:
                                            pass
                            else:
                                try:
                                    content = msg.get_payload(decode=True).decode("utf-8", "ignore")
                                except Exception:
                                    content = str(msg.get_payload())

                            to_h = str(msg.get("To", "")).lower()
                            del_h = str(msg.get("Delivered-To", "")).lower()
                            xo_h = str(msg.get("X-Original-To", "")).lower()
                            merged = f"{subject}\n{to_h}\n{del_h}\n{xo_h}\n{content}"
                            if str(target_email or "").lower() not in merged.lower():
                                processed_mail_ids.add(mail_id)
                                continue
                            code = extract_otp_code(merged)
                            processed_mail_ids.add(mail_id)
                            if code:
                                print(f"\n[{cfg.ts()}] [SUCCESS] IMAP号池 ({_mask_email(target_email)}) 提取成功: {code}")
                                return code
                except imaplib.IMAP4.abort:
                    conn = None
                    break
                except Exception:
                    continue
        except Exception as e:
            if mailbox_id:
                db_manager.mark_local_imap_mailbox_invalid(mailbox_id, str(e))
            print(f"\n[{cfg.ts()}] [ERROR] IMAP号池登录失败: {e}")
            return ""

        if attempt > 0 and attempt % 3 == 0:
            print(f"[{cfg.ts()}] [INFO] 仍在查询 IMAP号池邮箱({_mask_email(target_email)}) 验证码 ({attempt + 1}/{max_attempts})...")
        import time
        time.sleep(max(1, int(getattr(cfg, "LOCAL_IMAP_POOL_FETCH_RETRY_INTERVAL_SEC", 3) or 3)))

    if mailbox_id:
        db_manager.release_local_imap_mailbox(mailbox_id, to_status="idle", last_error="等待验证码超时")
    print(f"\n[{cfg.ts()}] [ERROR] IMAP号池邮箱({_mask_email(target_email)}) 接收验证码超时")
    return ""


def parse_mailbox_payload(jwt: str) -> Optional[dict]:
    try:
        data = json.loads(jwt or "{}")
        return data if isinstance(data, dict) else None
    except Exception:
        return None
