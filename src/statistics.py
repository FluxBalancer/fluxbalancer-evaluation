import asyncio
import json
import time
from collections import Counter, defaultdict
from typing import Optional, Any

import aiohttp


class AsyncLoadTester:
    def __init__(
            self,
            base_url: str,
            requests_per_load: int = 30,
            delay_between_requests: float = 2.0,
    ):
        self.base_url = base_url.rstrip('/')
        self.requests_per_load = requests_per_load
        self.delay_between_requests = delay_between_requests
        self.session: Optional[aiohttp.ClientSession] = None

        self._latencies: defaultdict[str, list[float]] = defaultdict(list)
        self._makespans: list[float] = []

        self._results_list: list[str] = []

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def make_request(self, endpoint: str, **kwargs) -> str:
        assert self.session is not None, "Session is not initialized!"
        url = f"{self.base_url}/{endpoint}"

        t0 = time.perf_counter()
        async with self.session.get(url, **kwargs) as resp:
            json_res = await resp.text()
            latency_ms = (time.perf_counter() - t0) * 1_000

            print(f"{resp.status} {endpoint}: {json_res}")

            try:
                port = str(json.loads(json_res).get("port", "unknown"))
            except json.JSONDecodeError:
                port = "unknown"
            self._latencies[port].append(latency_ms)

            return json_res

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
                await asyncio.sleep(delay)

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
        local = {
            "avg_latency_ms_by_port": {
                p: (sum(v) / len(v)) if v else 0.0
                for p, v in self._latencies.items()
            },
            "total_makespan_s": sum(self._makespans),
            "per_wave_makespan_s": self._makespans,
        }

        cpu_ports, mem_ports = [], []
        for line in self._results_list:
            try:
                data = json.loads(line)
                port = str(data.get("port"))
                if "cpu_burn" in data:
                    cpu_ports.append(port)
                elif "mem_burn" in data:
                    mem_ports.append(port)
            except json.JSONDecodeError:
                pass

        local.update({
            "cpu": dict(Counter(cpu_ports)),
            "mem": dict(Counter(mem_ports)),
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


async def main():
    async with AsyncLoadTester(
            base_url="http://localhost:8000",
            requests_per_load=50,
            delay_between_requests=2
    ) as tester:
        await tester.multi_load([
            'cpu', 'mem',
            # 'cpu', 'mem',
            # 'cpu', 'mem',
            # 'cpu', 'mem',
            # 'cpu', 'mem',
        ])

        print(await tester.stats())


if __name__ == '__main__':
    asyncio.run(main())
