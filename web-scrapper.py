import re
import pandas as pd
from requests_html import HTMLSession
from bs4 import BeautifulSoup, NavigableString, Tag
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Initialize an HTML session
session = HTMLSession()

def get_text_by_search_key(html: BeautifulSoup, key):
    svg = html.find('title', string=key)
    parent = None
    sibling = None
    if svg: parent = svg.find_parent('div')
    if parent: sibling = parent.find_next_sibling('div')
    if sibling: return sibling.getText().strip()
    else: return None
    
# Function to scrape data from an item page
def scrape_item_page(item_url):
    item_data = {}
    response = session.get(item_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        nhab_key = 'FichaInmueble_dormitorios' 
        ciudad_key = 'fichaproductos_ciudad'
        precio_path = 'div.clearfix div.container.main-wrapper.px3.py0.mx-auto.md-px4.mt1 div.col.md-col-4.mt4 div.bg-darken-1.px3.py2 div.flex.flex-baseline'
        direccion_path = 'div.container.main-wrapper.px3.py0.mx-auto.md-px4.mt1 > div.col.md-col-8.pr4 > div.col.col-12.pb2.mt2 > div.container.px2.md-px0 > div > p.h4.bolder.m0'
        barrio_path='div.container.main-wrapper.px3.py0.mx-auto.md-px4.mt1 > div.col.md-col-8.pr4 > div.col.col-12.pb2.mt2 > div.container.px2.md-px0 > div > p:nth-child(3)'
        descripcion_path='div.clearfix div.container.main-wrapper.px3.py0.mx-auto.md-px4.mt1 div.col.md-col-8.pr4 div.col.col-12.px1.md-px0.h4'
        
        item_data['URL'] = item_url
        item_data['Ciudad'] = get_text_by_search_key(soup, ciudad_key)
        if soup.select(barrio_path): item_data['Barrio'] = soup.select(barrio_path)[0].get_text(strip=True)
        if soup.select(direccion_path): item_data['Direccion'] = soup.select(direccion_path)[0].get_text(strip=True)
        if soup.select(descripcion_path): item_data['Descripcion'] = soup.select(descripcion_path)[0].get_text(separator=" ", strip=True)
        item_data['N° Hab'] = get_text_by_search_key(soup, nhab_key)
        if soup.select(precio_path) :item_data['Precio'] = soup.select(precio_path)[0].get_text(strip=True)
    else:
        print(f"Failed to retrieve {item_url}. Status code: {response.status_code}")
    return item_data

# Function to scrape items from a single page
def scrape_page(page_number):
    page_url = f'https://clasificados.lavoz.com.ar/inmuebles/casas/alquileres?cantidad-de-dormitorios[0]=2-dormitorios&cantidad-de-dormitorios[1]=3-dormitorios &page={page_number}'
    response = session.get(page_url)
    items_data = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find all item links on the page
        item_links = soup.select('div.flex.flex-wrap > div > div > a')
        item_urls = [link['href'] for link in item_links]
        
        # Use ThreadPoolExecutor to scrape item pages concurrently
        with  ThreadPoolExecutor(max_workers=5) as executor:
            results = list(tqdm(executor.map(scrape_item_page, item_urls), total=len(item_urls), desc=f'Scraping page {page_number}'))
            for result in results:
                if result:
                    items_data.append(result)     
    else:
        print(f"Failed to retrieve {page_url}. Status code: {response.status_code}")
    return items_data

# Function to scrape multiple pages
def scrape_multiple_pages(start_page, end_page):
    all_items_data = []
    for page_number in tqdm(range(start_page, end_page + 1), desc='Overall Progress'):
        page_data = scrape_page(page_number)
        all_items_data.extend(page_data)
    return all_items_data

# Function to retry failed pages
def retry_failed_pages(failed_pages):
    all_items_data = []
    for page_number in tqdm(failed_pages, desc='Retrying Failed Pages'):
        page_data, failed_items = scrape_page(page_number)
        if page_data is not None:
            all_items_data.extend(page_data)
            failed_items.extend(failed_items)
        else:
            print(f"Page {page_number} failed again.")
    return all_items_data

# Function to retry failed items
def retry_failed_items(failed_items):
    all_items_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(tqdm(executor.map(scrape_item_page, failed_items), total=len(failed_items), desc='Retrying Failed Items'))
        for item_url, result in zip(failed_items, results):
            if result:
                all_items_data.append(result)
            else:
                print(f"Item {item_url} failed again.")
    return all_items_data

# Define the range of pages to scrape
start_page = 1
end_page = 12  # Adjust this to the number of pages you want to scrape

#$save
# Scrape the data
all_data = scrape_multiple_pages(start_page, end_page)
"""
# Retry failed pages and items
if failed_pages:
    print("Retrying failed pages...")
    retry_data = retry_failed_pages(failed_pages)
    all_data.extend(retry_data)

if failed_items:
    print("Retrying failed items...")
    retry_data = retry_failed_items(failed_items)
    all_data.extend(retry_data)
"""
# Save the data to a CSV file
df = pd.DataFrame(all_data)
df.to_csv('scraped_data.csv', index=False)

print('Data has been scraped and saved to scraped_data.csv')

