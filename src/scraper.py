from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import random
import sys


def parse_listing_helper(elem):
    """Parse a single property listing card."""
    listing = {
        'type': None,
        'kind': None,
        'price': None,
        'title': None,
        'location': None,
        'beds': None,
        'baths': None,
        'agent': None,
        'contact': None,
        'listing_url': None
    }
    
    try:
        listing['type']     = elem.find_element(By.CLASS_NAME, "framer-cecu2t-container").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['kind']     = elem.find_element(By.CLASS_NAME, "framer-qv6gk7-container").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['price']    = elem.find_element(By.CLASS_NAME, "framer-maw6ss").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['title']    = elem.find_element(By.CLASS_NAME, "framer-1aak7at").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['location'] = elem.find_element(By.CLASS_NAME, "framer-gwir1w").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['beds']     = elem.find_element(By.CLASS_NAME, "framer-ix3jh1").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['baths']    = elem.find_element(By.CLASS_NAME, "framer-16vvyc2").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['agent']    = elem.find_element(By.CLASS_NAME, "framer-1kvgq0a").text.strip()
    except NoSuchElementException:
        pass
    try:
        listing['contact']  = elem.find_element(By.CLASS_NAME, "framer-9ce212").text.strip()
    except NoSuchElementException:
        pass
    try:
        link_elem = elem.find_element(By.TAG_NAME, "a")
        listing['listing_url'] = link_elem.get_attribute("href")
    except NoSuchElementException:
        pass
    return listing


def scrape_all_listings():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)
    
    try:
        driver.get("https://cwlagos.com/property")
        time.sleep(3)  # Initial load
        
        print("Starting to click 'Load More' buttons...")
        
        while True:
            try:
                load_more = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Load')] | //*[contains(text(), 'Load More')]"))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more)
                time.sleep(random.uniform(1, 1.5))
                load_more.click()
                print("Clicked 'Load More'...")
                time.sleep(random.uniform(1, 2))
                
            except TimeoutException:
                print("No more 'Load More' button found. All listings loaded.")
                break
            except Exception as e:
                print(f"Error clicking load more: {e}")
                break
        
        print("Now extracting listings...")
        # Main container class you identified
        elems = driver.find_elements(By.CLASS_NAME, "framer-12de3j-container")
        listings = []
        
        for i, elem in enumerate(elems, 1):
            parsed = parse_listing_helper(elem)
            listings.append(parsed)
            if i % 50 == 0:
                print(f"Parsed {i}/{len(elems)} listings...")
        
        print(f"Successfully scraped {len(listings)} listings.")
        return listings
        
    finally:
        driver.quit()


def save_to_df(listings):
    df = pd.DataFrame(listings)
    df.to_csv("cwlagos_listings.csv", index=False)
    print("Data saved to cwlagos_listings.csv")
    print(f"Total rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")


if __name__ == "__main__":
    try:
        listings = scrape_all_listings()
        save_to_df(listings)
    except Exception as e:
        print(f"Script failed with error: {e}")
        sys.exit(1)