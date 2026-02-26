import streamlit as st
import asyncio
import re
import io
import pdfplumber
import pandas as pd
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# ==============================
# CONFIG
# ==============================

api_id = st.secrets["API_ID"]
api_hash = st.secrets["API_HASH"]
SESSION_STRING = st.secrets["SESSION_STRING"]
SAUDI_CHANNEL = "saudia_jobs"
QATAR_CHANNEL = "m_jobvacancies"

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")

# ==============================
# UI
# ==============================

st.title("📩 Telegram Email Scraper")

country = st.radio("Select Country", ["Saudi", "Qatar"])

today = datetime.today().date()
default_from = today - timedelta(days=7)

date_from = st.date_input("From Date", default_from)
date_to = st.date_input("To Date", today)

if date_from > date_to:
    st.error("From date cannot be greater than To date")
    st.stop()

# ==============================
# SCRAPER FUNCTION
# ==============================

async def scrape_telegram(country, date_from, date_to):

    client = TelegramClient(
        StringSession(SESSION_STRING),
        api_id,
        api_hash
    )

    await client.connect()

    if not await client.is_user_authorized():
        await client.disconnect()
        return []

    channel_username = SAUDI_CHANNEL if country == "Saudi" else QATAR_CHANNEL
    channel = await client.get_entity(channel_username)

    date_from_dt = datetime.combine(date_from, datetime.min.time())
    date_to_dt = datetime.combine(date_to, datetime.max.time())

    messages = []

    async for msg in client.iter_messages(channel):

        msg_date = msg.date.replace(tzinfo=None)

        if msg_date > date_to_dt:
            continue

        if msg_date < date_from_dt:
            break

        messages.append(msg)

    unique_emails = set()
    results = []

    for msg in messages:
        try:
            # ================= SAUDI =================
            if country == "Saudi":

                if msg.text:
                    emails = EMAIL_REGEX.findall(msg.text)

                    for email in emails:
                        email = email.lower().strip()

                        if email not in unique_emails:
                            unique_emails.add(email)
                            title = msg.text.split("\n")[0]
                            results.append(
                                (email, title, str(msg.date.date()))
                            )

            # ================= QATAR =================
            else:

                if msg.document and msg.file and msg.file.name:
                    if msg.file.name.lower().endswith(".pdf"):

                        pdf_bytes = await client.download_media(msg, bytes)
                        if not pdf_bytes:
                            continue

                        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                            full_text = ""

                            for page in pdf.pages:
                                text = page.extract_text()
                                if text:
                                    full_text += text + "\n"

                        full_text_upper = full_text.upper()

                        start = full_text_upper.find("SITUATION VACANT")
                        end = full_text_upper.find("SITUATION WANTED")

                        if start != -1:
                            section = (
                                full_text[start:end]
                                if end != -1
                                else full_text[start:]
                            )

                            emails = EMAIL_REGEX.findall(section)

                            for email in emails:
                                email = email.lower().strip()

                                if email not in unique_emails:
                                    unique_emails.add(email)
                                    results.append(
                                        (str(msg.date.date()), email)
                                    )

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)

    await client.disconnect()
    return results


# ==============================
# RUN BUTTON
# ==============================

if st.button("🚀 Start Scraping"):

    with st.spinner("Scraping Telegram... Please wait..."):

        results = asyncio.run(
            scrape_telegram(country, date_from, date_to)
        )

    if results:

        if country == "Saudi":
            df = pd.DataFrame(results, columns=["Email", "Title", "Date"])
        else:
            df = pd.DataFrame(results, columns=["Date", "Email"])

        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        st.download_button(
            label="📥 Download Excel File",
            data=output,
            file_name=f"{country.lower()}_emails.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )