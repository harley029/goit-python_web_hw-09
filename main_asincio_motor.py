import json
import requests
import asyncio
import aiohttp
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Налаштування логування
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

async def get_soup(url, session):
    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, "lxml")
        return soup


async def write_to_json(filename: str, data: list):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(
            pool,
            lambda: json.dump(
                data,
                open(filename, "w", encoding="utf-8"),
                ensure_ascii=False,
                indent=4,
            ),
        )
    logging.info(f"Збережено дані у файл {filename}")


async def read_from_json(filename: str):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        data = await loop.run_in_executor(
            pool, lambda: json.load(open(filename, encoding="utf-8"))
        )
    logging.info(f"Прочитано дані з файлу {filename}")
    return data


async def get_pages(base_link: str, session) -> list:
    links = [base_link]
    link_to_parse = base_link
    while link_to_parse:
        soup = await get_soup(link_to_parse, session)
        link = soup.find("li", class_="next")
        if link:
            link = link.find("a").attrs.get("href", "")
            links.append(base_link + link)
            link_to_parse = base_link + link
        else:
            link_to_parse = None
    logging.info(f"Зібрано {len(links)} сторінок для парсингу")
    return links


async def get_authors_info(base_url: str, urls: list, session) -> list:
    author_links = []
    for url in urls:
        soup = await get_soup(url, session)
        authors = soup.find_all("small", class_="author")
        authors_description = soup.find_all("div", class_="quote")
        for el in range(len(authors)):
            author_name = authors[el].text
            author_found = any(
                author["author_name"] == author_name for author in author_links
            )
            if not author_found:
                author_info_url = base_url + authors_description[el].find(
                    "a", class_=""
                ).attrs.get("href", "")
                author_links.append(
                    {"author_name": author_name, "author_info_url": author_info_url}
                )
    logging.info(f"Зібрано інформацію про {len(author_links)} авторів")
    return author_links


async def parse_authors(author_links: list, session):
    tasks = []
    for author in author_links:
        url = author["author_info_url"]
        tasks.append(get_soup(url, session))

    soups = await asyncio.gather(*tasks)

    author_list = []
    for soup in soups:
        fullname = soup.find("h3", class_="author-title").text.strip()
        born_date = soup.find("span", class_="author-born-date").text.strip()
        born_location = soup.find("span", class_="author-born-location").text.strip()
        description = soup.find("div", class_="author-description").text.strip()
        author_list.append(
            {
                "fullname": fullname,
                "born_date": born_date,
                "born_location": born_location,
                "description": description,
            }
        )
    logging.info(f"Розпарсено інформацію про {len(author_list)} авторів")
    return author_list


async def parse_quotes(links: list, session):
    tasks = []
    for url in links:
        tasks.append(get_soup(url, session))

    soups = await asyncio.gather(*tasks)

    quotes_list = []
    for soup in soups:
        quotes = soup.find_all("div", class_="quote")
        for el in quotes:
            author = el.find("small", class_="author").text
            quote = el.find("span", class_="text").text
            tags_list = []
            tags = el.find("div", class_="tags").find_all("a", class_="tag")
            for tag in tags:
                tags_list.append(tag.text.strip())
            quotes_list.append(
                {
                    "tags": tags_list,
                    "author": author,
                    "quote": quote,
                }
            )
    logging.info(f"Зібрано {len(quotes_list)} цитат")
    return quotes_list


# Асинхронний запис в базу з використанням motor
async def upload_author_data(authors_data: list, db):
    author_collection = db["authors"]
    await author_collection.insert_many(authors_data)
    logging.info(f"Збережено {len(authors_data)} авторів у базу даних")

# Асинхронний запис в базу з використанням motor
async def upload_quotes_data(quotes_data: list, db):
    author_collection = db["authors"]
    quote_collection = db["quotes"]
    for quote_data in quotes_data:
        author_name = quote_data["author"]
        author = await author_collection.find_one({"fullname": author_name})
        if author:
            quote_data["author"] = author["_id"]
            await quote_collection.insert_one(quote_data)
    logging.info(f"Збережено {len(quotes_data)} цитат у базу даних")


async def main():
    start_time = datetime.now()
    logging.info("Початок роботи скрипта")

    BASE_URL = "https://quotes.toscrape.com"

    client = AsyncIOMotorClient(
        "mongodb+srv://harley029:8lyrMibko@cluster0.b6lozo9.mongodb.net/book_db?retryWrites=true&w=majority",
        serverSelectionTimeoutMS=5000,  # 5 seconds
    )
    db = client["book_db"]
    logging.info("Підключення до бази даних встановлено")

    async with aiohttp.ClientSession() as session:
        pages = await get_pages(BASE_URL, session)
        author_links = await get_authors_info(BASE_URL, pages, session)
        authors_info = await parse_authors(author_links, session)
        await write_to_json("authors.json", authors_info)
        quotes_info = await parse_quotes(pages, session)
        await write_to_json("quotes.json", quotes_info)

    authors_data = await read_from_json("authors.json")
    quotes_data = await read_from_json("quotes.json")

    await upload_author_data(authors_data, db)
    await upload_quotes_data(quotes_data, db)

    end_time = datetime.now()
    logging.info(f"Скрипт завершив роботу за {end_time - start_time}")


if __name__ == "__main__":
    asyncio.run(main())
