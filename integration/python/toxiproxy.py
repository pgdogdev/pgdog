"""Toxiproxy HTTP API wrapper and pytest fixtures."""

import pytest
import requests

TOXI_URL = "http://127.0.0.1:8474"
PROXY_NAME = "primary"


class Toxiproxy:
    """Simple wrapper around the toxiproxy HTTP API."""

    def __init__(self, proxy_name=PROXY_NAME, url=TOXI_URL):
        self.proxy_name = proxy_name
        self.url = url

    def add(self, name, toxic_type, toxicity=1.0, stream="downstream", **attributes):
        self.remove(name)
        requests.post(
            f"{self.url}/proxies/{self.proxy_name}/toxics",
            json={
                "name": name,
                "type": toxic_type,
                "stream": stream,
                "toxicity": toxicity,
                "attributes": attributes,
            },
        ).raise_for_status()
        return self

    def remove(self, name):
        try:
            requests.delete(f"{self.url}/proxies/{self.proxy_name}/toxics/{name}")
        except Exception:
            pass
        return self

    def enable(self):
        requests.post(
            f"{self.url}/proxies/{self.proxy_name}",
            json={"enabled": True},
        ).raise_for_status()
        return self

    def disable(self):
        requests.post(
            f"{self.url}/proxies/{self.proxy_name}",
            json={"enabled": False},
        ).raise_for_status()
        return self

    def reset(self):
        """Remove all toxics and re-enable the proxy."""
        resp = requests.get(f"{self.url}/proxies/{self.proxy_name}")
        resp.raise_for_status()
        for toxic in resp.json().get("toxics", []):
            self.remove(toxic["name"])
        self.enable()
        return self


@pytest.fixture
def toxi():
    """Provide a clean Toxiproxy instance; reset after the test."""
    proxy = Toxiproxy()
    proxy.reset()
    yield proxy
    proxy.reset()


@pytest.fixture
def reset_peer(toxi):
    """Add reset_peer toxics on both upstream and downstream at 100%.

    Applies to both directions so that existing pooled connections get killed
    when data flows through them, not just on new connection establishment.
    """
    toxi.add("reset_peer", "limit_data", toxicity=1.0, bytes=1000)
    return toxi
