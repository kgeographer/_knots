import requests
import time
import os
import mimetypes
from urllib.parse import urlparse

def get_file_extension_from_response(response):
  """Determine file extension from response headers or content"""

  # First try Content-Type header
  content_type = response.headers.get('Content-Type', '').lower()

  if 'jpeg' in content_type or 'jpg' in content_type:
    return '.jpg'
  elif 'png' in content_type:
    return '.png'
  elif 'gif' in content_type:
    return '.gif'
  elif 'webp' in content_type:
    return '.webp'

  # If header doesn't help, check the first few bytes (magic numbers)
  content_start = response.content[:10]

  # JPEG magic numbers
  if content_start.startswith(b'\xff\xd8\xff'):
    return '.jpg'
  # PNG magic number
  elif content_start.startswith(b'\x89PNG\r\n\x1a\n'):
    return '.png'
  # GIF magic numbers
  elif content_start.startswith(b'GIF87a') or content_start.startswith(b'GIF89a'):
    return '.gif'
  # WebP magic number
  elif b'WEBP' in content_start:
    return '.webp'

  # Default fallback
  return '.jpg'

def test_single_download():
  test_url = "https://web.archive.org/web/20250911223709id_/https://endlessknots.netage.com/.a/6a00df3523b1d0883401b8d13a538a970c-320wi"

  response = requests.get(test_url)
  print(f"Status: {response.status_code}")
  print(f"Content-Type: {response.headers.get('Content-Type')}")
  print(f"Size: {len(response.content)} bytes")

  if response.status_code == 200:
    extension = get_file_extension_from_response(response)
    print(f"Detected extension: {extension}")

    with open(f"test_image{extension}", "wb") as f:
      f.write(response.content)
    print("Test download successful!")


# Run test first
test_single_download()
