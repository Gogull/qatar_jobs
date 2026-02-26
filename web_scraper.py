import streamlit as st
import asyncio
import httpx
import pandas as pd
import re
from urllib.parse import quote
from datetime import datetime, timedelta
import io

# ==============================
# CONFIG
# ==============================

BASE_URL = "https://gulfjobs.el7far.com"
FEED_BASE = "/feeds/posts/summary/-/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://gulfjobs.el7far.com/",
}

MAX_RESULTS = 100
CONCURRENCY = 10

CATEGORIES = [
    "Qatar jobs today",
    "job vacancies in qatar",
    "وظائف الخليج اليوم",
    "Top Companies Jobs in Qatar",
    "Technical & Engineering Jobs",
    "Corporate jobs in Qatar",
    "Transport & Logistics Jobs",
    "Administrative Jobs",
    "Private Sector Jobs",
    "Daily job postings",
    "Marketing & Sales Jobs",
    "Qatar Newspaper Jobs",
    "Oil and Gas Jobs in Qatar",
    "Accounting & Finance Jobs",
    "Education & Training Jobs",
    "Medical & Healthcare Jobs",
    "Tourism & Hospitality Jobs",
    "Walk In Interview",
    "Media & Creative Jobs",
    "Technology & IT Jobs",
    "Full Time Jobs",
    "Government Jobs in Qatar",
    "Part Time Jobs",
    "Remote Jobs",
    "Qatar Jobs",
    "Qatar Airways",
    "Hospitality",
    "Medical & Health",
    "Pro Tips",
    "Qatar Discounts",
]

email_pattern = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# ==============================
# SCRAPER
# ==============================

async def scrape_jobs(date_from, date_to, progress_placeholder):

    semaphore = asyncio.Semaphore(CONCURRENCY)
    seen_links = set()
    seen_emails = set()
    failed_503 = []
    results_data = []
    total_found = 0

    async def fetch_post(client, title, published, link):
        nonlocal total_found
        async with semaphore:
            try:
                response = await client.get(link)

                if response.status_code == 503:
                    failed_503.append((title, published, link))
                    return

                if response.status_code != 200:
                    return

                emails = email_pattern.findall(response.text)

                for email in emails:
                    email = email.lower().strip()
                    if email not in seen_emails:
                        seen_emails.add(email)
                        results_data.append(
                            [title, published, link, email]
                        )
                        total_found += 1

            except:
                pass

    async with httpx.AsyncClient(
        http2=True,
        headers=HEADERS,
        timeout=20.0,
    ) as client:

        for idx, category in enumerate(CATEGORIES, start=1):

            progress_placeholder.info(
                f"Scraping ({idx}/{len(CATEGORIES)}) | Category: {category} | Unique Emails Found: {total_found}"
            )

            start_index = 1

            while True:

                params = {
                    "alt": "json",
                    "start-index": start_index,
                    "max-results": MAX_RESULTS,
                }

                encoded_category = quote(category)
                feed_url = BASE_URL + FEED_BASE + encoded_category

                response = await client.get(feed_url, params=params)

                if response.status_code != 200:
                    break

                data = response.json()
                entries = data.get("feed", {}).get("entry", [])

                if not entries:
                    break

                tasks = []
                stop_category = False

                for entry in entries:

                    title = entry.get("title", {}).get("$t", "")
                    published_full = entry.get("published", {}).get("$t", "")

                    if not published_full:
                        continue

                    published_date = datetime.fromisoformat(
                        published_full.replace("Z", "+00:00")
                    ).date()

                    if published_date < date_from:
                        stop_category = True
                        break

                    if published_date > date_to:
                        continue

                    link = ""
                    for l in entry.get("link", []):
                        if l.get("rel") == "alternate":
                            link = l.get("href")

                    if not link or link in seen_links:
                        continue

                    seen_links.add(link)
                    tasks.append(
                        fetch_post(client, title, str(published_date), link)
                    )

                if tasks:
                    await asyncio.gather(*tasks)

                if stop_category:
                    break

                start_index += MAX_RESULTS
                await asyncio.sleep(1)

        # Retry 503 sequentially
        for title, published, link in failed_503:
            try:
                response = await client.get(link)
                if response.status_code == 200:
                    emails = email_pattern.findall(response.text)

                    for email in emails:
                        email = email.lower().strip()
                        if email not in seen_emails:
                            seen_emails.add(email)
                            results_data.append(
                                [title, published, link, email]
                            )
                            total_found += 1

                await asyncio.sleep(1)

            except:
                pass

    return results_data


# ==============================
# STREAMLIT UI
# ==============================

st.title("📊 Gulf Jobs Email Scraper")

today = datetime.today().date()
default_from = today - timedelta(days=7)

date_from = st.date_input("From Date", default_from)
date_to = st.date_input("To Date", today)

if date_from > date_to:
    st.error("Invalid date range")

if st.button("Start Scraping"):

    progress_placeholder = st.empty()

    with st.spinner("Running scraper..."):

        data = asyncio.run(scrape_jobs(date_from, date_to, progress_placeholder))

    progress_placeholder.success("Scraping Completed ✅")

    if data:
        df = pd.DataFrame(
            data,
            columns=["Title", "Published Date", "Link", "Email"],
        )

        output = io.BytesIO()
        df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)

        st.download_button(
            label="📥 Download Excel File",
            data=output,
            file_name="unique_job_emails.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    else:
        st.warning("No unique emails found.")