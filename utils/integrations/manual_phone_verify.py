import secrets
import threading
import time
from typing import Any

from utils import config as cfg
from utils.integrations.hero_sms import _build_sentinel_for_session, _extract_next_url, _follow_redirect_chain, _post_with_retry

_TASK_LOCK = threading.Lock()
_MANUAL_PHONE_TASKS: dict[str, dict[str, Any]] = {}


def _now() -> float:
    return time.time()


def _touch(task: dict[str, Any]) -> None:
    task["updated_at"] = _now()


def _get_task(task_id: str) -> dict[str, Any]:
    with _TASK_LOCK:
        task = _MANUAL_PHONE_TASKS.get(str(task_id or "").strip())
    if not task:
        raise ValueError("未找到手动手机验证任务")
    return task


def _cleanup_expired_tasks() -> None:
    ttl = max(300, int(getattr(cfg, "PHONE_VERIFY_MANUAL_TIMEOUT_SEC", 600) or 600) * 2)
    now = _now()
    with _TASK_LOCK:
        expired = [
            task_id for task_id, task in _MANUAL_PHONE_TASKS.items()
            if now - float(task.get("updated_at") or task.get("created_at") or now) > ttl
        ]
        for task_id in expired:
            _MANUAL_PHONE_TASKS.pop(task_id, None)


def create_manual_phone_task(session, proxies: Any, *, email: str = "", stage: str = "register", hint_url: str = "") -> str:
    _cleanup_expired_tasks()
    task_id = secrets.token_urlsafe(12)
    task = {
        "task_id": task_id,
        "email": str(email or "").strip(),
        "stage": str(stage or "register").strip(),
        "status": "pending_phone",
        "phone_number": "",
        "hint_url": str(hint_url or "").strip(),
        "next_url": "",
        "error": "",
        "created_at": _now(),
        "updated_at": _now(),
        "session": session,
        "proxies": proxies,
    }
    with _TASK_LOCK:
        _MANUAL_PHONE_TASKS[task_id] = task
    return task_id


def get_visible_tasks() -> list[dict[str, Any]]:
    _cleanup_expired_tasks()
    rows = []
    with _TASK_LOCK:
        for task in _MANUAL_PHONE_TASKS.values():
            rows.append({
                "task_id": task["task_id"],
                "email": task["email"],
                "stage": task["stage"],
                "status": task["status"],
                "phone_number": task["phone_number"],
                "error": task["error"],
                "created_at": task["created_at"],
                "updated_at": task["updated_at"],
            })
    rows.sort(key=lambda x: float(x.get("created_at") or 0), reverse=True)
    return rows


def send_code(task_id: str, phone_number: str) -> tuple[bool, str]:
    task = _get_task(task_id)
    phone = str(phone_number or "").strip()
    if not phone:
        return False, "手机号不能为空"
    if task.get("status") not in ("pending_phone", "failed"):
        return False, f"当前任务状态不允许发送验证码: {task.get('status')}"

    session = task["session"]
    proxies = task["proxies"]
    headers = {
        "referer": "https://auth.openai.com/add-phone",
        "accept": "application/json",
        "content-type": "application/json",
    }
    sentinel = _build_sentinel_for_session(session, "authorize_continue", proxies)
    if sentinel:
        headers["openai-sentinel-token"] = sentinel
    try:
        resp = _post_with_retry(
            session,
            "https://auth.openai.com/api/accounts/add-phone/send",
            headers=headers,
            json_body={"phone_number": phone},
            proxies=proxies,
            timeout=30,
            retries=1,
        )
        if resp.status_code != 200:
            task["status"] = "failed"
            task["error"] = f"发送验证码失败: HTTP {resp.status_code}"
            _touch(task)
            return False, task["error"]
        task["phone_number"] = phone
        task["status"] = "pending_code"
        task["error"] = ""
        _touch(task)
        return True, "验证码已发送，请输入短信验证码"
    except Exception as e:
        task["status"] = "failed"
        task["error"] = f"发送验证码异常: {e}"
        _touch(task)
        return False, task["error"]


def validate_code(task_id: str, code: str) -> tuple[bool, str]:
    task = _get_task(task_id)
    sms_code = str(code or "").strip()
    if not sms_code:
        return False, "验证码不能为空"
    if task.get("status") != "pending_code":
        return False, f"当前任务状态不允许提交验证码: {task.get('status')}"

    session = task["session"]
    proxies = task["proxies"]
    headers = {
        "referer": "https://auth.openai.com/phone-verification",
        "accept": "application/json",
        "content-type": "application/json",
    }
    sentinel = _build_sentinel_for_session(session, "authorize_continue", proxies)
    if sentinel:
        headers["openai-sentinel-token"] = sentinel
    try:
        resp = _post_with_retry(
            session,
            "https://auth.openai.com/api/accounts/phone-otp/validate",
            headers=headers,
            json_body={"code": sms_code},
            proxies=proxies,
            timeout=30,
            retries=1,
        )
        if resp.status_code != 200:
            task["status"] = "failed"
            task["error"] = f"验证码校验失败: HTTP {resp.status_code}"
            _touch(task)
            return False, task["error"]
        try:
            data = resp.json() or {}
        except Exception:
            data = {}
        next_url = _extract_next_url(data).strip() or str(data.get("continue_url") or "").strip()
        if next_url and not next_url.startswith("http"):
            next_url = f"https://auth.openai.com{next_url}" if next_url.startswith("/") else next_url
        if next_url:
            try:
                _, follow_url = _follow_redirect_chain(session, next_url, proxies)
                if follow_url:
                    next_url = follow_url
            except Exception:
                pass
        if not next_url:
            next_url = str(task.get("hint_url") or "").strip()
        task["status"] = "verified"
        task["next_url"] = next_url
        task["error"] = ""
        _touch(task)
        return True, next_url or "手机验证成功"
    except Exception as e:
        task["status"] = "failed"
        task["error"] = f"验证码校验异常: {e}"
        _touch(task)
        return False, task["error"]


def cancel_task(task_id: str) -> tuple[bool, str]:
    task = _get_task(task_id)
    task["status"] = "cancelled"
    task["error"] = "用户已取消"
    _touch(task)
    return True, "已取消手动手机验证任务"


def wait_for_manual_phone_result(task_id: str, timeout: int = 600) -> tuple[bool, str]:
    started = _now()
    while _now() - started < max(30, int(timeout or 600)):
        if getattr(cfg, "GLOBAL_STOP", False):
            return False, "任务已停止"
        task = _get_task(task_id)
        status = str(task.get("status") or "")
        if status == "verified":
            return True, str(task.get("next_url") or "")
        if status in ("failed", "cancelled"):
            return False, str(task.get("error") or status)
        time.sleep(1)
    task = _get_task(task_id)
    task["status"] = "failed"
    task["error"] = "手动手机验证超时"
    _touch(task)
    return False, task["error"]
