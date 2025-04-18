import re
import time
from typing import List

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------- TEXT NORMALISATION ----------
EMOJI_PATTERN = re.compile(
    "["  # a bunch of unicode ranges
    u"\U0001F600-\U0001F64F"
    u"\U0001F300-\U0001F5FF"
    u"\U0001F680-\U0001F6FF"
    u"\U0001F700-\U0001F77F"
    u"\U0001F780-\U0001F7FF"
    u"\U0001F800-\U0001F8FF"
    u"\U0001F900-\U0001F9FF"
    u"\U0001FA00-\U0001FA6F"
    u"\U0001FA70-\U0001FAFF"
    u"\U00002702-\U000027B0"
    u"\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE,
)


def preprocess_text(text: str) -> str:
    """Minimal cleaning suitable for SBERT."""
    text = EMOJI_PATTERN.sub("", text)
    text = BeautifulSoup(text, "html.parser").get_text()
    text = re.sub(r"http\S+", "", text)
    text = text.strip().lower()
    return re.sub(r"\s+", " ", text)


# ---------- SCRAPER ----------

def scrape_reviews(movie_id: str, *, load_more: int = 5, headless: bool = True) -> List[str]:
    """Return list of cleaned review bodies for a given IMDb movie ID."""
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(f"https://www.imdb.com/title/{movie_id}/reviews")
    wait = WebDriverWait(driver, 10)

    for _ in range(load_more):
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "button.ipc-see-more__button")
            btn.send_keys("\n")
            wait.until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, ".ipl-load-more__load-indicator"))
            )
            time.sleep(1)
        except Exception:
            break

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    bodies = []
    for art in soup.select("article.user-review-item"):
        body_el = art.select_one("div.ipc-html-content-inner-div")
        if not body_el:
            continue
        bodies.append(preprocess_text(body_el.text))
    return bodies