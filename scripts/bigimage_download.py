from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests
import time
import random
from pathlib import Path


def download_images_with_selenium():
  # Set up Chrome to look more like a regular browser
  options = Options()
  options.add_argument("--disable-blink-features=AutomationControlled")
  options.add_experimental_option("excludeSwitches", ["enable-automation"])
  options.add_experimental_option('useAutomationExtension', False)

  driver = webdriver.Chrome(options=options)
  driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

  # Visit main blog page first
  driver.get("https://endlessknots.netage.com/")
  time.sleep(5)

  # Extract cookies
  selenium_cookies = driver.get_cookies()
  cookies = {cookie['name']: cookie['value'] for cookie in selenium_cookies}

  # Create requests session with cookies
  session = requests.Session()
  session.cookies.update(cookies)
  session.headers.update({
    'User-Agent': driver.execute_script("return navigator.userAgent;"),
    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'image',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-origin',
  })

  # Test with one image first
  test_url = "https://endlessknots.netage.com/.a/6a00df3523b1d0883401b8d13a538a970c-popup"

  try:
    response = session.get(test_url)
    print(f"Test download: {response.status_code}")
    if response.status_code == 200:
      print("Success! Session cookies are working")
      # Save test image
      with open("test_image.jpg", 'wb') as f:
        f.write(response.content)
    else:
      print(f"Failed: {response.status_code}")
      print(f"Response headers: {dict(response.headers)}")

  except Exception as e:
    print(f"Error: {e}")

  driver.quit()


# Run the test
download_images_with_selenium()
