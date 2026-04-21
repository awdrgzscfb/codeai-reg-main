from curl_cffi import requests


class ICloudHMEService:
    def __init__(self, cookies: str, label: str = "Wenfxl-Codex", proxies=None):
        self.cookies = (cookies or "").strip()
        self.label = (label or "Wenfxl-Codex").strip()
        self.proxies = proxies
        self.base_url_v1 = "https://p68-maildomainws.icloud.com/v1/hme"
        self.params = {
            "clientBuildNumber": "2413Project28",
            "clientMasteringNumber": "2413B20",
            "clientId": "",
            "dsid": "",
        }
        self.headers = {
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Content-Type": "text/plain",
            "Accept": "*/*",
            "Origin": "https://www.icloud.com",
            "Referer": "https://www.icloud.com/",
            "Accept-Language": "en-US,en;q=0.9",
            "Cookie": self.cookies,
        }

    def _check(self):
        if not self.cookies:
            raise ValueError("未配置 iCloud Cookies")

    def generate_email(self) -> dict:
        self._check()
        resp = requests.post(
            f"{self.base_url_v1}/generate",
            params=self.params,
            json={"langCode": "en-us"},
            headers=self.headers,
            proxies=self.proxies,
            timeout=15,
            impersonate="chrome120",
        )
        resp.raise_for_status()
        return resp.json()

    def reserve_email(self, email: str) -> dict:
        resp = requests.post(
            f"{self.base_url_v1}/reserve",
            params=self.params,
            json={
                "hme": email,
                "label": self.label,
                "note": "Wenfxl-Codex",
            },
            headers=self.headers,
            proxies=self.proxies,
            timeout=15,
            impersonate="chrome120",
        )
        resp.raise_for_status()
        return resp.json()

    def create_email_and_token(self):
        gen = self.generate_email()
        if not gen.get("success"):
            raise RuntimeError(f"iCloud HME generate 失败: {gen}")

        email = (gen.get("result", {}) or {}).get("hme")
        if not email:
            raise RuntimeError(f"iCloud HME generate 未返回邮箱: {gen}")

        reserve = self.reserve_email(email)
        if not reserve.get("success"):
            raise RuntimeError(f"iCloud HME reserve 失败: {reserve}")

        return str(email).strip(), ""
