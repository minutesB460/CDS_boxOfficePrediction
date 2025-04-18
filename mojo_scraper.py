import requests
from bs4 import BeautifulSoup

__all__ = ["scrape_worldwide_box_office"]


def scrape_worldwide_box_office(movie_id: str) -> str:
    """Return Box Office Mojo worldwide gross (e.g. "$47,680,966") or raise."""
    url = f"https://www.boxofficemojo.com/title/{movie_id}/credits/"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Bad status {r.status_code} for {url}")

    soup = BeautifulSoup(r.text, "html.parser")
    for span in soup.find_all("span", string=lambda t: t and "Worldwide" in t):
        parent = span.find_parent("div")
        if parent and (money := parent.find(class_="money")):
            return money.text.strip()
    raise RuntimeError("Worldwide gross not found")