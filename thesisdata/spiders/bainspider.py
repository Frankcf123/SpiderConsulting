import scrapy
import json

class BainSpider(scrapy.Spider):
    name = "bain"

    def start_requests(self):
        allowed_domains = ['www.bain.com']
        url = (
            'https://www.bain.com/en/api/search/keyword/get?'
            'start=0&results=50&filters=|types(422,426,427,428,430)'
            '&searchValue=artificial%20intelligence&sortValue=relevance'
        )
     
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        data=json.loads(response.body)
        totalResults=data.get("totalResults")
        results = data.get('rsults')
        data_dict = collections.defaultdict(dict)
        for result in results:
            ar_title=str(result.get('title'))
            ar_url="https://www.bain.com"+str(result.get('url'))
            ar_type=str(result.get('type'))
            ar_date=str(result.get('date')).split(',')[-1]
            room_id = str(home.get('listing').get('id'))
            data_dict[room_id]['url'] = BASE_URL + str(home.get('listing').get('id'))
            data_dict[room_id]['price'] = home.get('pricing_quote').get('rate').get('amount')
            request = scrapy.Request(
                url=ar_url,
                callback=self.parse_article,
                 meta={'jobs_url': url, 'job_page_url': job_page_url,'company_id':company_id})

     
        with open(_file, 'wb') as f:
            f.write(str1.encode())

    
    def parse_article(self,response):
        ar_author = response.xpath('//*[@class="hero__byline"]/p/text()').extract()
        ar_content = response.xpath('//*[@class="rte rte--show-end-of-content rte__heading"]/div/text()').extract()

