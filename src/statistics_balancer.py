import asyncio
import json
import random
import time
from collections import Counter, defaultdict
from statistics import fmean
from typing import Optional, Any

import aiohttp
import requests  # noqa


def get_delay(delay: float) -> float:
    min_delay = 0.3 * delay if delay else 0
    max_delay = delay
    return random.uniform(min_delay, max_delay)


class AsyncLoadTester:
    def __init__(
            self,
            base_url: str,
            headers: dict,
            requests_per_load: int = 30,
            delay_between_requests: float = 2.0,
    ):
        self.base_url = base_url.rstrip('/')
        self.requests_per_load = requests_per_load
        self.delay_between_requests = delay_between_requests
        self.headers = headers
        self.session: Optional[aiohttp.ClientSession] = None
        self._latencies: defaultdict[str, dict[str, list[float]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._makespans: list[float] = []

        self._results_list: list[str] = []

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def make_request(self, endpoint: str, **kwargs) -> str:
        assert self.session is not None, "Session is not initialized!"
        url = f"{self.base_url}/{endpoint}"

        t0 = time.perf_counter()
        async with self.session.get(url, **kwargs) as resp:
            text_res = await resp.text()
            latency_ms = (time.perf_counter() - t0) * 1_000

            socket = resp.headers.get("X-Upstream-Socket")
            host = "unknown"
            port = "unknown"
            if socket and ":" in socket:
                host, port = socket.rsplit(":", 1)

            self._latencies[host][port].append(latency_ms)

            return text_res

    async def load(
            self,
            endpoint: str,
            requests_count: Optional[int] = None,
            delay: Optional[float] = None,
            **request_kwargs,
    ) -> list[Any]:
        """Отправляет requests_count запросов с задержкой delay между ними."""
        tasks = []
        requests_count = requests_count or self.requests_per_load
        delay = delay if delay is not None else self.delay_between_requests

        start_wave = time.perf_counter()
        for i in range(requests_count):
            task = asyncio.create_task(self.make_request(endpoint, **request_kwargs))
            tasks.append(task)
            if i < requests_count - 1:
                await asyncio.sleep(get_delay(delay))

        res = await asyncio.gather(*tasks)
        self._results_list.extend(res)

        makespan_s = time.perf_counter() - start_wave
        self._makespans.append(makespan_s)

        return res

    async def multi_load(self, endpoints: list[str], **kwargs) -> list[list[Any]]:
        """Параллельно запускает несколько load по списку endpoints."""
        tasks = [self.load(endpoint, **kwargs) for endpoint in endpoints]
        return await asyncio.gather(*tasks)

    async def stats(self):
        # --- latency: всё по хостам ---
        avg_latency_ms_by_host: dict[str, float] = {}
        for host, ports in self._latencies.items():
            all_vals: list[float] = []
            for vals in ports.values():
                all_vals.extend(vals)
            avg_latency_ms_by_host[host] = fmean(all_vals) if all_vals else 0.0

        local = {
            "avg_latency_ms_by_host": avg_latency_ms_by_host,
            "total_makespan_s": sum(self._makespans),
            "per_wave_makespan_s": self._makespans,
        }

        # --- порт -> хост (чтобы cpu/mem считать по хостам) ---
        port_to_host: dict[str, str] = {}
        for host, ports in self._latencies.items():
            for port in ports.keys():
                # если один и тот же порт встретился на разных хостах — последнего перетрёт
                # но в вашей схеме порты 8001..8006 обычно уникальны на хосте,
                # а хосты разные, так что лучше использовать socket, но вы хотите по хостам.
                port_to_host[port] = host

        cpu_hosts: list[str] = []
        mem_hosts: list[str] = []

        for line in self._results_list:
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            port = data.get("port")
            if port is None:
                continue
            port = str(port)

            host = port_to_host.get(port, "unknown")

            if "cpu_burn" in data:
                cpu_hosts.append(host)
            elif "mem_burn" in data:
                mem_hosts.append(host)

        local.update({
            "cpu": dict(Counter(cpu_hosts)),
            "mem": dict(Counter(mem_hosts)),
        })

        remote = {}
        if self.session is None:
            raise RuntimeError("Session is not initialized!")

        try:
            async with self.session.get(f"{self.base_url}/stats") as resp:
                if resp.status == 200:
                    remote = await resp.json()
                else:
                    print(f"⚠ Не удалось получить /stats: HTTP {resp.status}")
        except Exception as e:
            print(f"⚠ Ошибка запроса /stats: {e}")

        return {
            **local,
            **remote,
        }


HEADERS = {
    "X-Balancer-Strategy": "topsis",
    "X-Weights-Strategy": "entropy",
    "X-Balancer-Deadline": "11000"
}


async def main():
    res = requests.get("http://127.0.0.1:8000/cpu?seconds=5", headers={
        "X-Balancer-Strategy": "topsis",
        "X-Weights-Strategy": "entropy",
        "X-Balancer-Deadline": "5500",
        "X-Replications-Strategy": "speculative",
        "X-Replications-Count": "4"
    })
    print(res.text)
    print(res.headers['X-Upstream-Socket'])

    # async with AsyncLoadTester(
    #         base_url="http://localhost:8000",
    #         requests_per_load=5,
    #         delay_between_requests=2,
    #         headers=HEADERS
    # ) as tester:
    #     seconds = 2
    #     await tester.multi_load([
    #         f'cpu?seconds={seconds}', f'mem?seconds={seconds}',
    #         f'cpu?seconds={seconds}', f'mem?seconds={seconds}',
    #         f'cpu?seconds={seconds}', f'mem?seconds={seconds}',
    #         f'cpu?seconds={seconds}', f'mem?seconds={seconds}',
    #         f'cpu?seconds={seconds}', f'mem?seconds={seconds}',
    #     ])
    #
    #     print(await tester.stats())


if __name__ == '__main__':
    asyncio.run(main())
