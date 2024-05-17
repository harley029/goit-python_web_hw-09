import json
import logging
import time

from mongoengine import connect
import requests
from bs4 import BeautifulSoup

from models import Author, Quote

# Налаштування логування
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_soup(url):
    logging.info("Отримання вмісту сторінки: %s", url)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")
    return soup


def write_to_json(filename: str, data: list):
    logging.info("Запис даних у файл: %s", filename)
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def read_from_json(filename: str):
    logging.info("Читання даних з файлу: %s", filename)
    with open(filename) as f:
        data = json.load(f)
    return data


def get_pages(base_link: str) -> list:
    logging.info("Отримання списку сторінок для парсингу з: %s", base_link)
    links = [base_link]
    link_to_parse = base_link
    while link_to_parse:
        soup = get_soup(link_to_parse)
        link = soup.find("li", class_="next")
        if link:
            link = link.find("a").attrs.get("href", "")
            links.append(base_link + link)
            link_to_parse = base_link + link
        else:
            link_to_parse = None
    logging.info("Знайдено %d сторінок для парсингу.", len(links))
    return links


def get_authors_info(base_url: str, urls: list) -> list:
    logging.info("Отримання інформації про авторів.")
    author_links = []
    for url in urls:
        soup = get_soup(url)
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
    logging.info("Знайдено %d авторів.", len(author_links))
    return author_links


def parse_authors(author_links: list):
    logging.info("Парсинг інформації про авторів.")
    author_list = []
    for author in author_links:
        url = author["author_info_url"]
        soup = get_soup(url)
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
    logging.info("Парсинг завершено. Знайдено %d авторів.", len(author_list))
    return author_list


def parse_quotes(links: list):
    logging.info("Парсинг цитат.")
    quotes_list = []
    for url in links:
        soup = get_soup(url)
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
    logging.info("Парсинг завершено. Знайдено %d цитат.", len(quotes_list))
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


if __name__ == "__main__":

    BASE_URL = "https://quotes.toscrape.com"

    logging.info("Підключення до бази даних.")
    connect(
        "book_db",
        host="mongodb+srv://harley029:8lyrMibko@cluster0.b6lozo9.mongodb.net/?retryWrites=true&w=majority",
    )
    start_time = time.time()

    # Отримання списку сторінок для парсингу
    pages = get_pages(BASE_URL)

    # Отримання списку сторінок авторів з послідуючим парсингом та записом у json файл
    author_links = get_authors_info(BASE_URL, pages)
    authors_info = parse_authors(author_links)
    write_to_json("authors.json", authors_info)

    # Парсинг всіх цитат з отриманого списку сторінок з послідуючим записом у json файл
    quotes_info = parse_quotes(pages)
    write_to_json("quotes.json", quotes_info)

    # Зчитування данних з файлів json
    authors_data = read_from_json("authors.json")
    quotes_data = read_from_json("quotes.json")

    # Завантаження зчитаних даних у MongoDB
    upload_author_data(authors_data)
    upload_quotes_data(quotes_data)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info("Скрипт завершено успішно за %.2f секунд.", elapsed_time)
