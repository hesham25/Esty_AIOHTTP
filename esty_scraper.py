import asyncio, aiohttp, csv, time, pathlib, pandas as pd
from requests_html import HTML, HTMLSession, AsyncHTMLSession


headers = {
    'authority': 'www.etsy.com',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'sec-ch-ua': 'Google',
    'accept': '*/*',
    'x-page-guid': 'eb4e169aa93.73c43acc6c5af4c11bbe.00',
    'x-requested-with': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
    'x-detected-locale': 'USD^|en-US^|US',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.etsy.com/c?ref=pagination&explicit=1&page=241',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': 'uaid=sei8M8705bZgrlc8hQKIU0Y5vuVjZACChMSalTC6Wqk0MTNFyUrJ1MAoXzc_N8ewyL3Ay7MiMKy8yjs3IKkipSTCSamWAQA.; user_prefs=7-qFZFNGKoL6O6Jnl2q73j_-Nn9jZACChMSalTA6Wik02EVJJ680J0dHKTVPNzRYSUcJRIBFjCAULiKWAQA.; fve=1617001641.0; _gcl_au=1.1.595130933.1617001645; _ga=GA1.2.1945994983.1617001645; _gid=GA1.2.152633265.1617001645; ua=531227642bc86f3b5fd7103a0c0b4fd6; _pin_unauth=dWlkPU16TXlaVEUyWmpVdE1qVTFZeTAwTXpJMExUaGlNalV0T0RZeU5XWXpObUZoTkRFeg; last_browse_page=https^%^3A^%^2F^%^2Fwww.etsy.com^%^2Fshop^%^2Ftreasureimports; _uetsid=6be23450905d11eb85a7c70681d13d29; _uetvid=6be28070905d11ebbe58b979ee5b486c; granify.uuid=4e01b20f-05f9-48f4-a8e0-cbe014fd393b; pla_spr=0^|0^|1; granify.session.qivBM=-1; exp_hangover=EDyIV-MxC7DQq69qK1fyL4lMRWRjZACChMSalRC6NalaqTw1KT6xqCQzLTM5MzEnPiexJDUvuTK-0CTeyMDQUslKKTMvNSczPTMpJ1WplgEA',
	}

results = []

async def get_data_desc():
	for i in range(0,801):
		for p in range(1,241):
			url = f'https://www.etsy.com/search?q=mug&explicit=1&min={round((i/20),2)}&max={round(((i/20)+0.05),2)}&page={p}&ref=pagination'
			res = await session.get(url,headers=headers)
			if res.status_code in range(200,300):
				no_of_rows = res.html.xpath('//div[@data-search-results-region]//li')
				for item in no_of_rows:
					if item.xpath('.//span[contains(text(),"Bestseller")]', first=True) != None:
						row = {}
						row['listing-link'] = res.url
						row['title'] = item.xpath('.//a[contains(@class,"listing-link")]/@title',first=True)
						row['link'] = item.xpath('//a[contains(@class,"listing-link")]/@href',first=True)
						#print(row)
						results.append(row)
			elif res.status_code == 429 :
				print(f'response {res.status_code} too many requests\nRetry again after 5 minutes....')
				time.sleep(5*60)
			else :
				pass
			if len(no_of_rows) == 0 :
				break
		df = pd.DataFrame(results)
		df.to_excel('Esty Bestseller Results shirt 2.xlsx')
		print(f'finished Price {round((i/20),2)} to {round(((i/20)+0.05),2)}, total results = {len(results)}')

s = time.time()
with AsyncHTMLSession() as session:
	session.run(get_data_desc)
	session.close()
print(f'{time.time()-s :.2f}')


# aiohttp
async def fetch_eith_sem(semaphore, url, asession):
	async with semaphore:
		async with asession.get(url,headers=headers) as response:
			if response.status in range(200,300):
				return await response.text()
			elif response.status == 429 :
				print(f'response {response.status} too many requests\nRetry again after 5 minutes....')
				await asyncio.sleep(5*60)
			else : pass

head_row = ['#', 'listing-link', 'title','link']
x = 1
async def parse(html):
	global x
	f = open('Esty Bestseller Results.csv', 'a+',encoding='utf-8-sig',newline='')
	wr = csv.DictWriter(f, fieldnames=head_row,)#,delimiter=';'
	#wr.writeheader()
	html = HTML(html=html)
	products = html.xpath('//div[@data-search-results-region]//li')
	if len(products) == 0 : pass
	for item in products:
		if item.xpath('.//span[contains(text(),"Bestseller")]', first=True) != None:
			row = {}
			row['#'] = x
			row['listing-link'] = url
			row['title'] = item.xpath('.//a[contains(@class,"listing-link")]/@title',first=True)
			row['link'] = item.xpath('//a[contains(@class,"listing-link")]/@href',first=True)
			print(row)
			wr.writerow(row)
			x += 1
	f.close()

async def main(urls):
	async with aiohttp.ClientSession() as asession:
		semaphore = asyncio.Semaphore(1)
		tasks = [asyncio.create_task(fetch_eith_sem(semaphore, url, asession)) for url in urls ]
		for html in  await asyncio.gather(*tasks):	  #return html_bodies
			await parse(html)


'''
urls = [f'https://www.etsy.com/search?q=mug&explicit=1&min={round((i/20),2)}&max={round(((i/20)+0.05),2)}&order=price_desc' for i in range(600)]
links = []
for link in urls:
	for i in range(1,241):
		links.append(f'{link}&page={i}&ref=pagination')
print(len(links))
s = time.time()
loop = asyncio.get_event_loop()
loop.run_until_complete(main(links[1000:1100]))
#loop.close()
print(f'{time.time()-s :.2f}')
'''
