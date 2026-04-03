import httpx
from bs4 import BeautifulSoup
import asyncio

async def test_anker_raw():
    url = "https://us.anker.com/products/a121d-45w-usb-c-fast-charger-foldable-plug-compact?variant=46070529228950&collections_chargers&Sort_by=Recommended"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            resp = await client.get(url, headers=headers, timeout=30.0)
            print(f"Status: {resp.status_code}")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                h1 = soup.find('h1')
                print(f"H1 found: {h1.text.strip() if h1 else 'None'}")
                # Check for SalePrice or similar
                print(f"salePrice in text? {'salePrice' in resp.text}")
                # Check for JSON-LD
                scripts = soup.find_all('script', type='application/ld+json')
                print(f"JSON-LD scripts found: {len(scripts)}")
                if scripts:
                    print("Sample JSON-LD content (first 100 chars):")
                    print(scripts[0].string[:100] if scripts[0].string else "No string content")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_anker_raw())
