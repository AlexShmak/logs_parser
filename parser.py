from pathlib import Path
from datetime import datetime
from ipaddress import IPv4Address
from typing import Dict, Optional, Set, Tuple
import re


class Parser:
    INTERNAL_TIMESTAMP = re.compile(r"\[(\d{2})\.(\d{2})\.(\d{2}) (\d{2}:\d{2}:\d{2})]")

    CONNECTION = re.compile(
        r".*?Incoming Conn\{(?P<conn_hex_id>[0-9a-f]+)} on (?P<raw_ip_port>\S+) accepted, "
        r"(?P<open_connections>\d+) of (?P<limit>\d+)"
    )
    NEW_QUERY = re.compile(
        r".*?On Conn\{(?P<conn_hex_id>[0-9a-f]+)} new Query\{(?P<query_hex_id>[0-9a-f]+)} "
        r"\[(?P<query_id>\d+)]: (?P<text_decoded>.+)"
    )
    END = re.compile(
        r".*?End Query\{(?P<query_hex_id>[0-9a-f]+)} \[(?P<query_id>\d+)], (?P<status>\w+),\s"
        r"spent \{ (?P<time_total_ms>[\d.]+) : (?P<time_in_queue_ms>[\d.]+) queue, "
        r"(?P<time_working_ms>[\d.]+) work } ms, (?P<response_size_bytes>\d+) bytes"
    )

    def __init__(self, file_path: Path):
        self.path = file_path

        self.connections: Dict[str, str] = {}
        self.query_to_ip: Dict[int, str] = {}

        self.request_freq: Dict[str, int] = {}
        self.total_words: int = 0

        self.all_ips: Set[str] = set()
        self.invalid_ips: Set[str] = set()

        self.first_end_time: Optional[datetime] = None
        self.last_end_time: Optional[datetime] = None

        self.total_time_working_ms: float = 0.0
        self.total_time_with_waiting_ms: float = 0.0
        self.max_handling_time_working_ms: float = 0.0
        self.max_handling_time_with_waiting_ms: float = 0.0

        self.new_query_ids: Set[int] = set()
        self.finished_query_ids: Set[int] = set()

    def parse(self):
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")

                if "Incoming Conn{" in line and self._parse_connection(line):
                    continue
                if "On Conn{" in line and self._parse_new_query(line):
                    continue
                if "End Query{" in line and self._parse_end_query(line):
                    continue

    def _parse_connection(self, line: str) -> bool:
        match = self.CONNECTION.search(line)
        if not match:
            return False
        conn_hex_id = match.group("conn_hex_id")
        raw_ip_port = match.group("raw_ip_port")
        ip = self._extract_ip(raw_ip_port)
        if ip:
            self.all_ips.add(ip)
            self.connections[conn_hex_id] = ip
        else:
            self.invalid_ips.add(raw_ip_port)
            self.connections[conn_hex_id] = raw_ip_port
        return True

    def _parse_new_query(self, line: str) -> bool:
        match = self.NEW_QUERY.search(line)
        if not match:
            return False
        query_id = int(match.group("query_id"))

        if query_id in self.new_query_ids:
            return True

        self.new_query_ids.add(query_id)
        text_decoded = match.group("text_decoded").strip()

        conn_hex_id = match.group("conn_hex_id")
        ip = self.connections.get(conn_hex_id)
        if ip:
            self.query_to_ip[query_id] = ip

        if text_decoded:
            self.request_freq[text_decoded] = self.request_freq.get(text_decoded, 0) + 1
            self.total_words += len(text_decoded.split())

        return True

    def _parse_end_query(self, line: str) -> bool:
        match = self.END.search(line)
        if not match:
            return False

        query_id = int(match.group("query_id"))
        if query_id in self.finished_query_ids:
            return True

        self.finished_query_ids.add(query_id)

        time_total_ms = float(match.group("time_total_ms"))
        time_working_ms = float(match.group("time_working_ms"))

        self.total_time_with_waiting_ms += time_total_ms
        self.total_time_working_ms += time_working_ms

        if time_total_ms > self.max_handling_time_with_waiting_ms:
            self.max_handling_time_with_waiting_ms = time_total_ms
        if time_working_ms > self.max_handling_time_working_ms:
            self.max_handling_time_working_ms = time_working_ms

        ts = self._extract_internal_timestamp(line)
        if ts:
            if self.first_end_time is None or ts < self.first_end_time:
                self.first_end_time = ts
            if self.last_end_time is None or ts > self.last_end_time:
                self.last_end_time = ts

        return True

    def _extract_internal_timestamp(self, line: str) -> Optional[datetime]:
        match = self.INTERNAL_TIMESTAMP.search(line)
        if not match:
            return None
        dd, mm, yy, hms = match.groups()
        return datetime.strptime(f"20{yy}-{mm}-{dd} {hms}", "%Y-%m-%d %H:%M:%S")

    def most_popular_request(self) -> Optional[str]:
        if not self.request_freq:
            return None
        return max(self.request_freq.items(), key=lambda kv: kv[1])[0]

    def average_words(self) -> float:
        n = len(self.new_query_ids)
        return self.total_words / n if n else 0.0

    def average_times(self) -> Tuple[float, float]:
        c = len(self.finished_query_ids)
        if c == 0:
            return 0.0, 0.0
        return self.total_time_working_ms / c, self.total_time_with_waiting_ms / c

    def max_times(self) -> Tuple[float, float]:
        return self.max_handling_time_working_ms, self.max_handling_time_with_waiting_ms

    def rps(self) -> float:
        c = len(self.finished_query_ids)
        if c == 0 or not self.first_end_time or not self.last_end_time:
            return 0.0
        span = (self.last_end_time - self.first_end_time).total_seconds()
        if span <= 0:
            return float(c)
        return c / span

    def _extract_ip(self, raw: str) -> Optional[str]:
        if ":" in raw:
            ip = raw.split(":", 1)[0]
            if self._is_valid_ipv4(ip):
                return ip
        else:
            if self._is_valid_ipv4(raw):
                return raw
        return None

    @staticmethod
    def _is_valid_ipv4(raw: str) -> bool:
        try:
            IPv4Address(raw)
            return True
        except ValueError:
            return False
