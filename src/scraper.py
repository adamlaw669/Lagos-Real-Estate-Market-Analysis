import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import os

def lagos_house_scraping(url):
    """ The lagos houes price scraping function
    it functions only for the cwlagos site as it was customized for it 
    """
    #loading our delicious soup
    main_rows = []
    
    # --- ADDED: START FUNCTION LOG ---
    print("--- Starting lagos_house_scraping function ---")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        print(f"Successfully fetched main page: {url}") # ADDED
        
    except requests.RequestException as e:
        print('Error fetching the main page: ', e)
        # --- ADDED: END FUNCTION LOG ---
        print("--- Function ended with an error. ---")
        return []
    
    main_soup = BeautifulSoup(response.text, 'html.parser')
    
    # the scraping logic begins...
    print("Beginning scraping logic...") # ADDED
    locations = main_soup.select('a.location-banner-inner')
    print(f"Identified {len(locations)} major locations from the main page.") # ADDED

    # --- Outer Loop Start ---
    for links in locations:
            place = links.select_one('h4.title')
            place = place.get_text(strip=True) if place else 'Nan'
            
            #adding logs just cos there might be an error somewhere
            print(f'found {place} and will be scraping the listings there now ')
            
            location_url = links["href"] if links and links.has_attr("href") else 'Nan'

            current_page_url = location_url
            
            print(f"Starting pagination loop for location URL: {location_url}") # ADDED

            # --- Inner (Pagination) Loop Start ---
            while current_page_url:
                
                print(f"Scraping list page: {current_page_url}\n")
                
                try:
                    page_response = requests.get(current_page_url)
                    page_response.raise_for_status() 
                except requests.RequestException as e:
                    print(f"Error fetching list page {current_page_url}: {e}")
                    break 

                loaded_soup = BeautifulSoup(page_response.text, 'html.parser')

                #  4. Loop Through Property Cards on Current Page
                cards_on_page = loaded_soup.select("article.property-item")
                
                # --- ADDED: CARD COUNT LOG ---
                print(f"Found {len(cards_on_page)} property cards on this list page.")
                
                if not cards_on_page:
                    print("No property cards found on this page. Ending scrape.")
                    break 

                # --- Innermost Loop Start ---
                for card in cards_on_page:
                    row = {}
                    
                    # Scrape List Page Data (omitting repetitive logs here)
                    price = card.select_one("div.property-price")
                    row["price_raw"] = price.get_text(strip=True) if price else None
                    
                    title = card.select_one("h2.property-title")
                    row["title"] = title.get_text(strip=True) if title else None
                        
                    loc = card.select_one("div.property-location a")
                    row["location"] = loc.get_text(strip=True) if loc else None        
                        
                    status = card.select_one("a.status-property-label")
                    row["status"] = status.get_text(strip=True) if status else None
                    
                    avail = card.select_one("a.label-property-label")
                    row["availability"] = avail.get_text(strip=True) if avail else None
                    
                    ptype = card.select_one("a.type-property")
                    row["property_type"] = ptype.get_text(strip=True) if ptype else None
                    
                    info = card.select("div.property-meta")
                    for div in info:
                        text = div.get_text(strip=True)
                        if "Beds" in text:
                            row["beds"] = text.replace("Beds:", "").strip() if text.replace("Beds:", "").strip() != '-' else None
                        elif "Baths" in text:
                            row["baths"] = text.replace("Baths:", "").strip() if text.replace("Baths:", "").strip() != '-' else None
                        elif "sqm" in text:
                            row["area(sqm)"] = text.replace("sqm:", "").strip() if text.replace("sqm:", "").strip() != '-' else None
                    
                    author = card.select_one("div.name-author")
                    row['author'] = author.get_text(strip=True) if author else None
                    
                    date = card.select_one("div.property-postdate")
                    row['post-date'] = date.get_text(strip=True) if date else None
                    

                    detail = card.select_one("a.property-image")
                    detail_url = detail["href"] if detail and detail.has_attr("href") else None
                    
                    row["listing_url"] = detail_url
                    
                    if detail_url:
                        print(f"Scraping detail page: {row['title']}")
                        
                        # --- Detail Page Scrape Start ---
                        try:
                            time.sleep(1) 
                            response = requests.get(detail_url)
                            response.raise_for_status()
                            time.sleep(1)
                            soup = BeautifulSoup(response.text, 'html.parser')
                            
                            # --- ADDED: AMENITY GATHERING LOG ---
                            print(f"    - Extracting amenities for {row.get('title', 'listing')}...")
                            
                            for amenity_list_ul in soup.select('ul.list-check'):
                                for amenity_name in list(amenity_list_ul.stripped_strings):
                                    if amenity_name:
                                        row[amenity_name] = True
                                        
                        except requests.RequestException as e:
                            print(f" Could not scrape detail page {detail_url}: {e}")
                        # --- Detail Page Scrape End ---

                    main_rows.append(row)
                # --- Innermost Loop End ---
                    
                next_page_link = loaded_soup.select_one("a.next.page-numbers")
                
                if next_page_link:
                    current_page_url = next_page_link['href'] 
                    
                    print(f"\n Found the next page: {current_page_url}")
                    time.sleep(2)
                else:
                    print("\n No more 'next page' links found. Our scrape might be complete.")
                    current_page_url = None 
            # --- Inner (Pagination) Loop End ---


    # --- FINALIZATION ---
    print(f"Scraping is finally done, that took a while! We found {len(main_rows)} properties though")
    listings = pd.DataFrame(main_rows)
    
    # --- ADDED: OUTPUT LOG ---
    print(f"Attempting to save {len(main_rows)} records to CSV...")
    
    listings.to_csv('../data/raw/lagos_housing_data.csv', index=False)
    
    # --- ADDED: FINAL SUCCESS LOG ---
    print("CSV saved successfully.")
    
    # --- ADDED: END FUNCTION LOG ---
    print("--- lagos_house_scraping function finished. ---")


if __name__ == "__main__":
    url = "https://www.cwlagos.com"
    if not os.path.exists('../data/raw/'):
        os.makedirs('../data/raw/')
        print("Created directory: ../data/raw/") # ADDED
    lagos_house_scraping(url)