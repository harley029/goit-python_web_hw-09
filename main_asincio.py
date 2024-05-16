import json
import logging
import time
import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from mongoengine import connect
from models import Author, Quote

# Налаштування логування
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def get_soup(session: ClientSession, url: str):
    logging.info(f"Отримання вмісту сторінки: {url}")
    async with session.get(url) as response:
        html = await response.text()
        soup = BeautifulSoup(html, "lxml")
        return soup


async def write_to_json(filename: str, data: list):
    logging.info(f"Запис даних у файл: {filename}")
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


async def read_from_json(filename: str):
    logging.info(f"Читання даних з файлу: {filename}")
    with open(filename) as f:
        data = json.load(f)
    return data


async def get_pages(session: ClientSession, base_link: str) -> list:
    logging.info(f"Отримання списку сторінок для парсингу з: {base_link}")
    links = [base_link]
    link_to_parse = base_link
    while link_to_parse:
        soup = await get_soup(session, link_to_parse)
        link = soup.find("li", class_="next")
        if link:
            link = link.find("a").attrs.get("href", "")
            links.append(base_link + link)
            link_to_parse = base_link + link
        else:
            link_to_parse = None
    logging.info(f"Знайдено {len(links)} сторінок для парсингу.")
    return links


async def get_authors_info(session: ClientSession, base_url: str, urls: list) -> list:
    logging.info("Отримання інформації про авторів.")
    author_links = []
    for url in urls:
        soup = await get_soup(session, url)
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
    logging.info(f"Знайдено {len(author_links)} авторів.")
    return author_links


async def parse_authors(session: ClientSession, author_links: list):
    logging.info("Парсинг інформації про авторів.")
    author_list = []
    for author in author_links:
        url = author["author_info_url"]
        soup = await get_soup(session, url)
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
    logging.info(f"Парсинг завершено. Знайдено {len(author_list)} авторів.")
    return author_list


async def parse_quotes(session: ClientSession, links: list):
    logging.info("Парсинг цитат.")
    quotes_list = []
    for url in links:
        soup = await get_soup(session, url)
        quotes = soup.find_all("div", class_="quote")
        for el in quotes:
            author = el.find("small", class_="author").text
            quote = el.find("span", class_="text").text
            tags_list = [
                tag.text.strip()
                for tag in el.find("div", class_="tags").find_all("a", class_="tag")
            ]
            quotes_list.append(
                {
                    "tags": tags_list,
                    "author": author,
                    "quote": quote,
                }
            )
    logging.info(f"Парсинг завершено. Знайдено {len(quotes_list)} цитат.")
    return quotes_list


def upload_author_data(authors_data: list):
    logging.info("Збереження авторів у базі даних.")
    for author_data in authors_data:
        author = Author(**author_data)
        author.save()
    logging.info("Збереження авторів завершено.")


def upload_quotes_data(quotes_data: list):
    logging.info("Збереження цитат у базі даних.")
    for quote_data in quotes_data:
        author_name = quote_data["author"]
        author = Author.objects(fullname=author_name).first()
        if author:
            quote_data["author"] = author
            quote = Quote(**quote_data)
            quote.save()
    logging.info("Збереження цитат завершено.")


async def main():
    start_time = time.time()

    BASE_URL = "https://quotes.toscrape.com"

    logging.info("Підключення до бази даних.")
    connect(
        "book_db",
        host="mongodb+srv://harley029:8lyrMibko@cluster0.b6lozo9.mongodb.net/?retryWrites=true&w=majority",
    )

    async with aiohttp.ClientSession() as session:
        pages = await get_pages(session, BASE_URL)
        author_links = await get_authors_info(session, BASE_URL, pages)
        authors_info = await parse_authors(session, author_links)
        await write_to_json("authors.json", authors_info)
        quotes_info = await parse_quotes(session, pages)
        await write_to_json("quotes.json", quotes_info)

    # Читання файлів авторів та цитат
    authors_data = await read_from_json("authors.json")
    quotes_data = await read_from_json("quotes.json")

    # Збереження авторів та цитат у базі даних
    upload_author_data(authors_data)
    upload_quotes_data(quotes_data)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Скрипт завершено успішно за {elapsed_time:.2f} секунд.")


if __name__ == "__main__":
    asyncio.run(main())
