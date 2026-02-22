from __future__ import annotations

from datetime import datetime, timezone

from prometheus_client import Counter, Histogram, start_http_server


class BotMetrics:
    def __init__(self, enabled: bool, port: int):
        self.enabled = bool(enabled)
        self.port = int(port)
        self.command_total = Counter(
            "discord_command_total",
            "Total Discord bot command executions.",
            ["command", "outcome"],
        )
        self.command_hour_total = Counter(
            "discord_command_hour_total",
            "Total Discord command executions grouped by UTC hour.",
            ["command", "hour_utc"],
        )
        self.command_duration_seconds = Histogram(
            "discord_command_duration_seconds",
            "Discord command execution latency in seconds.",
            ["command", "outcome"],
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, float("inf")),
        )
        self.open_pack_set_total = Counter(
            "discord_open_pack_set_total",
            "Total successful /open_pack executions per set.",
            ["set_name"],
        )
        if self.enabled:
            start_http_server(self.port)

    def record_command(self, *, command: str, outcome: str, duration_seconds: float) -> None:
        if not self.enabled:
            return
        safe_command = (command or "unknown").strip().lower() or "unknown"
        safe_outcome = (outcome or "unknown").strip().lower() or "unknown"
        hour_utc = str(datetime.now(timezone.utc).hour)
        self.command_total.labels(command=safe_command, outcome=safe_outcome).inc()
        self.command_hour_total.labels(command=safe_command, hour_utc=hour_utc).inc()
        self.command_duration_seconds.labels(command=safe_command, outcome=safe_outcome).observe(max(duration_seconds, 0.0))

    def record_open_pack_set(self, set_name: str) -> None:
        if not self.enabled:
            return
        safe_set = (set_name or "unknown").strip() or "unknown"
        self.open_pack_set_total.labels(set_name=safe_set).inc()
