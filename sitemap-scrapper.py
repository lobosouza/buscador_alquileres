from requests_html import HTMLSession
from bs4 import BeautifulSoup

# Initialize an HTML session
session = HTMLSession()

# Function to fetch and parse the sitemap
def fetch_sitemap(sitemap_url):
    response = session.get(sitemap_url)
    if response.status_code == 200:
        # Parse the XML content
        soup = BeautifulSoup(response.content, 'xml')
        # Find all <loc> tags in the sitemap
        urls = soup.find_all('loc')
        # Log the URLs to the console
        for url in urls:
            print(url.text)
    else:
        print(f"Failed to retrieve sitemap. Status code: {response.status_code}")

# URL of the sitemap
sitemap_url = 'https://www.zonaprop.com.ar/departamentos-alquiler-cordoba.html?utm_source=google&utm_medium=cpc&utm_campaign=Search_Rent_CBA_Tipo-inmueble_DSA&utm_content=Departamentos&utm_term=&gad_source=1&gclid=Cj0KCQjwtsy1BhD7ARIsAHOi4xao-lBMx3T0dtcxIQ97nDojwbW1HQv2XKlyIZYYbY4ykDNBP-Wq5k8aAl6HEALw_wcB'

# Fetch and log the sitemap URLs
fetch_sitemap(sitemap_url)
