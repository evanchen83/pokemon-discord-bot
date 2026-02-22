from __future__ import annotations

import time
from typing import Any, Optional
from urllib.parse import urlparse

import requests

from ibm_watsonx_orchestrate.client.agents.agent_client import AgentClient
from ibm_watsonx_orchestrate.client.chat.run_client import RunClient
from ibm_watsonx_orchestrate.client.threads.threads_client import ThreadsClient
from ibm_watsonx_orchestrate.client.utils import instantiate_client, is_local_dev


def _is_local_wxo_url(base_url: Optional[str]) -> bool:
    if not base_url:
        return False
    if is_local_dev(base_url):
        return True
    try:
        host = (urlparse(base_url).hostname or "").lower()
    except Exception:
        return False
    return host in {"localhost", "127.0.0.1", "::1", "host.docker.internal"}


class WXOChatClient:
    def __init__(
        self,
        agent_name: str,
        wxo_agent_id: Optional[str] = None,
        wxo_base_url: Optional[str] = None,
        wxo_api_key: Optional[str] = None,
        wxo_local_username: str = "wxo.archer@ibm.com",
        wxo_local_password: str = "watsonx",
        wxo_tenant_id: Optional[str] = None,
        wxo_tenant_name: str = "wxo-dev",
    ):
        self.agent_name = agent_name
        self.wxo_base_url = (wxo_base_url or "").rstrip("/")
        self.wxo_api_key = wxo_api_key
        self.wxo_local_username = wxo_local_username
        self.wxo_local_password = wxo_local_password
        self.wxo_tenant_id = wxo_tenant_id
        self.wxo_tenant_name = wxo_tenant_name
        self.local_token: Optional[str] = None
        self.agent_llm: Optional[str] = None
        self.local_mode = _is_local_wxo_url(self.wxo_base_url)
        self.cloud_iam_mode = bool(self.wxo_base_url and self.wxo_api_key and not self.local_mode)
        self.api_prefix = "/api/v1" if self.local_mode else "/v1"

        if self.cloud_iam_mode or self.local_mode:
            self.agent_client = None
            self.run_client = None
            self.threads_client = None
        else:
            self.agent_client = self._make_client(AgentClient, wxo_base_url, wxo_api_key, self.local_mode)
            self.run_client = self._make_client(RunClient, wxo_base_url, wxo_api_key, self.local_mode)
            self.threads_client = self._make_client(ThreadsClient, wxo_base_url, wxo_api_key, self.local_mode)
        self.agent_id = wxo_agent_id or self._resolve_agent_id(agent_name)

    @staticmethod
    def _make_client(client_cls: type, wxo_base_url: Optional[str], wxo_api_key: Optional[str], local_mode: bool):
        if wxo_base_url and wxo_api_key and local_mode:
            return client_cls(base_url=wxo_base_url, api_key=wxo_api_key, is_local=True)
        return instantiate_client(client_cls)

    def _build_url(self, path: str) -> str:
        if not self.wxo_base_url:
            raise RuntimeError("Missing WXO base URL")
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.wxo_base_url}{path}"

    def _local_login(self) -> None:
        token_url = self._build_url(f"{self.api_prefix}/auth/token")
        form = {"username": self.wxo_local_username, "password": self.wxo_local_password}
        first = requests.post(token_url, data=form, timeout=60)
        first.raise_for_status()
        first_token = first.json().get("access_token")
        if not first_token:
            raise RuntimeError("Local WXO auth token missing access_token")

        headers = {"Authorization": f"Bearer {first_token}", "Accept": "application/json"}
        tenant_id = self.wxo_tenant_id
        if not tenant_id:
            tenants_resp = requests.get(self._build_url(f"{self.api_prefix}/tenants"), headers=headers, timeout=60)
            tenants_resp.raise_for_status()
            tenants = tenants_resp.json() if tenants_resp.text else []
            if not isinstance(tenants, list) or not tenants:
                raise RuntimeError("No tenants returned by local WXO")

            target = None
            for tenant in tenants:
                if isinstance(tenant, dict) and tenant.get("name") == self.wxo_tenant_name:
                    target = tenant
                    break
            if target is None:
                target = tenants[0] if isinstance(tenants[0], dict) else None
            tenant_id = str((target or {}).get("id", "")).strip()
            if not tenant_id:
                raise RuntimeError("Could not resolve tenant id for local WXO")
            self.wxo_tenant_id = tenant_id

        scoped = requests.post(f"{token_url}?tenant_id={tenant_id}", data=form, timeout=60)
        scoped.raise_for_status()
        scoped_token = scoped.json().get("access_token")
        if not scoped_token:
            raise RuntimeError("Scoped local WXO token missing access_token")
        self.local_token = str(scoped_token)

    def _http_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.local_mode:
            if not self.local_token:
                self._local_login()
            headers["Authorization"] = f"Bearer {self.local_token}"
        elif self.cloud_iam_mode:
            if not self.wxo_api_key:
                raise RuntimeError("Missing WXO API key for cloud IAM mode")
            headers["IAM-API_KEY"] = self.wxo_api_key
        return headers

    def _http_get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        if not self.wxo_base_url:
            raise RuntimeError("Missing WXO base URL")
        url = self._build_url(path)
        headers = self._http_headers()
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=60,
        )
        if self.local_mode and response.status_code in {401, 403}:
            self.local_token = None
            headers = self._http_headers()
            response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json() if response.text else {}

    def _http_post(self, path: str, payload: dict[str, Any]) -> Any:
        if not self.wxo_base_url:
            raise RuntimeError("Missing WXO base URL")
        url = self._build_url(path)
        headers = self._http_headers()
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=120,
        )
        if self.local_mode and response.status_code in {401, 403}:
            self.local_token = None
            headers = self._http_headers()
            response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        return response.json() if response.text else {}

    def _orchestrate_path(self, suffix: str) -> str:
        if not suffix.startswith("/"):
            suffix = f"/{suffix}"
        return f"{self.api_prefix}/orchestrate{suffix}"

    def _resolve_agent_id(self, name: str) -> str:
        if self.cloud_iam_mode or self.local_mode:
            agents = self._http_get(self._orchestrate_path("/agents"), params={"names": name, "include_hidden": "true"})
        else:
            agents = self.agent_client.get_draft_by_name(name)
        if not agents:
            if self.cloud_iam_mode or self.local_mode:
                available = self._http_get(self._orchestrate_path("/agents"), params={"include_hidden": "true"})
                names = []
                if isinstance(available, list):
                    names = [str(a.get("name", "")) for a in available if isinstance(a, dict) and a.get("name")]
                sample = ", ".join(names[:12]) if names else "(none)"
                raise RuntimeError(
                    f"WXO agent '{name}' not found in configured cloud instance. "
                    f"Set WO_AGENT_NAME to an existing name, or set WO_AGENT_ID directly. "
                    f"Sample available agents: {sample}"
                )
            raise RuntimeError(f"WXO agent '{name}' not found")

        agent_id = agents[0].get("id")
        if not agent_id:
            raise RuntimeError(f"WXO agent '{name}' has no id")
        self.agent_llm = str(agents[0].get("llm") or "").strip() or None
        return str(agent_id)

    def _extract_assistant_text(self, thread_id: str, fallback_message_id: Optional[str] = None) -> str:
        if self.local_mode:
            data = self._http_get(f"{self.api_prefix}/threads/{thread_id}/messages")
        elif self.cloud_iam_mode:
            data = self._http_get(self._orchestrate_path(f"/threads/{thread_id}/messages"))
        else:
            data = self.threads_client.get_thread_messages(thread_id)
        messages: list[dict[str, Any]] = []

        if isinstance(data, list):
            messages = [m for m in data if isinstance(m, dict)]
        elif isinstance(data, dict):
            raw = data.get("data", data.get("messages", []))
            if isinstance(raw, list):
                messages = [m for m in raw if isinstance(m, dict)]

        if fallback_message_id:
            for msg in reversed(messages):
                if str(msg.get("id")) == str(fallback_message_id) and msg.get("role") == "assistant":
                    text = self._message_to_text(msg)
                    if text:
                        return text

        for msg in reversed(messages):
            if msg.get("role") == "assistant":
                text = self._message_to_text(msg)
                if text:
                    return text

        return "I couldn't read an assistant response from WXO for this run."

    @staticmethod
    def _message_to_text(msg: dict[str, Any]) -> str:
        content = msg.get("content")
        if isinstance(content, str):
            return content.strip()

        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    text_parts.append(item)
                elif isinstance(item, dict):
                    if isinstance(item.get("text"), str):
                        text_parts.append(item["text"])
                    elif item.get("response_type") == "text" and isinstance(item.get("text"), str):
                        text_parts.append(item["text"])
            return "\n".join([p.strip() for p in text_parts if p and p.strip()]).strip()

        return ""

    def ask(self, prompt: str, thread_id: Optional[str]) -> tuple[str, str]:
        if self.cloud_iam_mode or self.local_mode:
            payload: dict[str, Any] = {
                "message": {"role": "user", "content": prompt},
                "agent_id": self.agent_id,
                "capture_logs": False,
            }
            if thread_id:
                payload["thread_id"] = thread_id
            run = self._http_post(self._orchestrate_path("/runs"), payload)
        else:
            run = self.run_client.create_run(
                message=prompt,
                agent_id=self.agent_id,
                thread_id=thread_id,
                capture_logs=False,
            )

        run_id = str(run.get("run_id", ""))
        next_thread_id = str(run.get("thread_id", thread_id or ""))
        if not run_id:
            raise RuntimeError("WXO run did not return run_id")
        if not next_thread_id:
            raise RuntimeError("WXO run did not return thread_id")

        if self.cloud_iam_mode or self.local_mode:
            status: dict[str, Any] = {}
            for _ in range(90):
                status = self._http_get(self._orchestrate_path(f"/runs/{run_id}"))
                run_status = str(status.get("status", "")).lower()
                if run_status in {"completed", "failed", "cancelled"}:
                    break
                time.sleep(2)
        else:
            status = self.run_client.wait_for_run_completion(run_id=run_id, poll_interval=2, max_retries=90)
        run_status = str(status.get("status", "")).lower()
        if run_status != "completed":
            err = status.get("error") or f"Run ended with status={run_status}"
            raise RuntimeError(str(err))

        assistant_text = self._extract_assistant_text(
            thread_id=next_thread_id,
            fallback_message_id=status.get("message_id"),
        )
        return next_thread_id, assistant_text
