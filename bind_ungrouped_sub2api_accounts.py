import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

SUB2API_URL = "http://148.135.30.201:8080"
API_KEY = "admin-489935976372d9c736635b00ebde3b11b4305f0141990a31af2d5e93992bcd18"
PAGE_SIZE = 100
MAX_WORKERS = 50   # 可改成 10/20/30，别太大

headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json",
}

session = requests.Session()
session.headers.update(headers)

def get_all_groups():
    r = session.get(f"{SUB2API_URL.rstrip('/')}/api/v1/admin/groups/all", timeout=20)
    r.raise_for_status()
    data = r.json().get("data", [])
    return data if isinstance(data, list) else []

def pick_groups(groups):
    print("\n=== 可用分组 ===")
    for g in groups:
        gid = g.get("id")
        name = g.get("name") or g.get("title") or g.get("group_name") or "UNKNOWN"
        print(f"ID={gid} | NAME={name}")

    text = input("\n输入目标分组关键词或ID，多个用逗号分隔: ").strip()
    parts = [x.strip() for x in text.split(",") if x.strip()]
    if not parts:
        raise ValueError("未输入分组")

    matched_ids = []
    for part in parts:
        if part.isdigit():
            matched_ids.append(int(part))
            continue

        found = None
        for g in groups:
            gid = g.get("id")
            name = str(g.get("name") or g.get("title") or g.get("group_name") or "")
            if part.lower() in name.lower():
                found = gid
                print(f"[MATCH] {part} -> ID={gid}, NAME={name}")
                break

        if found is None:
            raise ValueError(f"没找到匹配分组: {part}")

        matched_ids.append(found)

    result = []
    for x in matched_ids:
        if x not in result:
            result.append(x)
    return result

def get_all_accounts():
    page = 1
    result = []

    while True:
        r = session.get(
            f"{SUB2API_URL.rstrip('/')}/api/v1/admin/accounts",
            params={"page": page, "page_size": PAGE_SIZE},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json().get("data", {})
        items = data.get("items", [])
        if not items:
            break

        result.extend(items)
        total = data.get("total", 0)
        if len(result) >= total:
            break
        page += 1

    return result

def is_ungrouped(acc):
    gids = acc.get("group_ids")
    return not gids

def update_one(acc, target_group_ids):
    acc_id = acc.get("id")
    name = acc.get("name") or acc.get("email") or "UNKNOWN"

    payload = dict(acc)
    payload["group_ids"] = target_group_ids

    r = session.put(
        f"{SUB2API_URL.rstrip('/')}/api/v1/admin/accounts/{acc_id}",
        json=payload,
        timeout=20,
    )
    return {
        "id": acc_id,
        "name": name,
        "status_code": r.status_code,
        "ok": r.status_code in (200, 201, 204),
        "resp": r.text[:200]
    }

def main():
    groups = get_all_groups()
    if not groups:
        print("没读取到分组。")
        return

    target_group_ids = pick_groups(groups)
    print(f"\n目标分组ID: {target_group_ids}")

    accounts = get_all_accounts()
    ungrouped = [a for a in accounts if is_ungrouped(a)]

    print(f"总账号: {len(accounts)}")
    print(f"未分组: {len(ungrouped)}")
    print(f"并发数: {MAX_WORKERS}")

    if not ungrouped:
        print("没有未分组账号。")
        return

    confirm = input("输入 yes 开始批量绑定: ").strip().lower()
    if confirm != "yes":
        print("已取消。")
        return

    ok = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(update_one, acc, target_group_ids) for acc in ungrouped]

        for i, fut in enumerate(as_completed(futures), 1):
            try:
                result = fut.result()
                if result["ok"]:
                    ok += 1
                    print(f"[{i}/{len(ungrouped)}] OK   ID={result['id']} NAME={result['name']}")
                else:
                    fail += 1
                    print(f"[{i}/{len(ungrouped)}] FAIL ID={result['id']} HTTP={result['status_code']} RESP={result['resp']}")
            except Exception as e:
                fail += 1
                print(f"[{i}/{len(ungrouped)}] EXCEPTION {e}")

    print("\n=== 完成 ===")
    print(f"成功: {ok}")
    print(f"失败: {fail}")

if __name__ == "__main__":
    main()
