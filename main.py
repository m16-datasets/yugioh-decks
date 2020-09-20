import click
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import csv
import shutil
from collections import defaultdict
from slugify import slugify
from urllib.parse import urlparse
import json
import time
import datetime
import hashlib
import dateparser

def get_urls():
    old_urls = []
    backup_file = Path("decks.txt")
    if backup_file.exists():
        with open(backup_file) as decks_fp:
            for line in decks_fp:
                if line.strip():
                    old_urls.append(line.strip())
    
    url_template = "https://ygoprodeck.com/category/decks/page/{}/"
    page = 1
    new_urls = []
    keep_crawling = True
    while keep_crawling:
        if page % 20 == 0:
            print(page)
        response = requests.get(url_template.format(page))
        if response.status_code != 200:
            print("reached the end of the list")
            break
        soup = BeautifulSoup(response.text, "lxml")
        all_anchors = soup.find_all("a", {"class":"more-link"})
        if not all_anchors:
            print("reached the end of the list")
            break

        for a in all_anchors:
            if old_urls and a["href"] == old_urls[0]:
                print("reached the oldest crawled url")
                keep_crawling = False
                break
            else:
                new_urls.append(a["href"])
        page += 1

    with open(backup_file, "w") as decks_fp:
        for url in new_urls + old_urls:
            decks_fp.write(url)
            decks_fp.write("\n")

    return new_urls, old_urls

name_id_mapping = dict()
temp_files_path = Path("tmp")
html_files_path = Path("html")
if not html_files_path.exists():
    html_files_path.mkdir(parents=True)

def fill_mapping(file):
    with open(file) as fd:
        reader = csv.DictReader(fd)
        for card in reader:
            name_id_mapping[card["name"]] = card["id"]

def find_by_id_name(card_name, logger):
    card_id = name_id_mapping.get(card_name)
    if card_id is None:
        logger.warning("The card '{card_name}' does not have an entry")
    return card_id or "-1"

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def hash(string):
    hash_object = hashlib.md5(string.encode())
    return hash_object.hexdigest()



def get_deck_info(deck_url, logger):
    deck_response = requests.get(deck_url)
    soup_deck = BeautifulSoup(deck_response.text, "lxml")
    parsd = urlparse(url)
    with open(Path(html_files_path, f"{slugify(parsd.path)}.html"), "w") as writable:
        writable.write(deck_desponse.text)
    article_content = soup_deck.find("div", {"class":"article-content"})
    if not article_content or not article_content.find("table"):
        # no content
        return None
    trs = article_content.find("table").find_all("tr")[1:]
    deck_info = {td[0].text.lower().strip("\n: "): td[1].text for td in [tr.find_all("td") for tr in trs] if len(td) > 1}
    deck_info["name"] = article_content.find("h1", {"class":"entry-title"}).text.strip()
    if "author" in deck_info:
        deck_info["author"] = hash(deck_info["author"])
    if "master" in deck_info:
        deck_info["deck master"] = find_by_id_name(deck_info["deck master"], logger)

    decks = defaultdict(list)
    deck_name = "main"
    deck_view = article_content.find("div", {"class":"uploaded-deck-view"})
    if deck_view:
        deck_number = 0
        
        for ch in deck_view.children:
            if ch.name == "hr":
                deck_name = ch.get("class",["-side"])[0].partition("-")[2]
                deck_number += 1
            if ch.name == "a":
                decks[deck_name].append(ch["data-name"])
    else:
        tables = article_content.find_all("table")
        if len(tables) == 1:
            # No cards specified
            return deck_info
        tables = [table for table in tables[1:] if "wikitable" not in table.get("class", [])]
        deck = tables[0].find_all("tr")[1:]
        data = {td[0].text: td[1] for td in [tr.find_all("td") for tr in deck] if len(td) > 1}
        contents = {key:list() for key in data}
        for key, children in data.items():
            for ch in chunks(list(children), 2):
                if "data-name" in ch[0]:
                    contents[key].extend([ch[0]["data-name"]] * int(ch[1][-1]))

        for main_deck_key in ["Monster", "Spells", "Traps"]:
            decks["main"].extend(contents.get(main_deck_key, list()))
        decks["side"] = contents.get("Side", [])
        decks["extra"] = contents.get("Extra", [])


        # replace by ids
        for dk_keys in ["main", "side", "extra"]:
            lst = []
            for card_name in decks.get(dk_keys, list()):
                lst.append(find_by_id_name(card_name))
            decks[dk_keys] = lst

    deck_info["deck"] = dict(decks)

    return deck_info


def download_data(logger):
    new_urls, old_urls = get_urls()
    fill_mapping("../yugioh-cards/data/cards.csv")

    print(f"found {len(new_urls)} new decks")

    for url in new_urls + old_urls:
        parsd = urlparse(url)
        try:
            destination_file = Path(temp_files_path, f"{slugify(parsd.path)}.json")
            if destination_file.exists():
                continue
            deck_info = get_deck_info(url, logger)
            if not deck_info:
                continue
            with open(destination_file, "w") as fd:
                json.dump(deck_info, fd)
        except:
            logging.error("Exception occurred", exc_info=True)

def process_data(logger):
    output_folder = Path("data")
    if output_folder.exists():
        shutil.rmtree(output_folder)
    for json_file in sorted(temp_files_path.glob("*.json")):
        deck = json.load(json_file.open())
        date = dateparser.parse(deck["submission date"]) if "submission date" in deck else datetime.datetime.min
        deck["submission date"] = date.isoformat()

        keys = list(deck.keys())
        for k in keys:
            deck[slugify(k,separator="_")] = deck.pop(k)

        output_file = Path(output_folder, f"{date.year:04}", f"{date.month:02}.jsonl")
        if not output_file.parent.exists():
            output_file.parent.mkdir(parents=True)
        with open(output_file, "a") as fd:
            json.dump(deck, fd)
            fd.write("\n")


@click.command()
@click.argument("log_file", type=click.Path(dir_okay=False))
def main(log_file):
    logger = setup_logger(log_file)
    download_data(logger)
    process_data(logger)


if __name__ == "__main__":
    main()
