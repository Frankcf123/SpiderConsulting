import scrapy
import json
import pymysql
import collections
import re

pymysql.install_as_MySQLdb()



class MckinseySpider(scrapy.Spider):
    name = "mckinsey"

    def start_requests(self):
        allowed_domains = ['www.mckinsey.com']
        url = (
            'https://www.bain.com/en/api/search/keyword/get?'
            'start=0&results=500&filters=|types(422,426,427,428,430)'
            '&searchValue=artificial%20intelligence&sortValue=relevance'
        )
        # self.connection = pymysql.connect(self.host, self.user, self.password, self.db,use_unicode=True, charset="utf8")

        self.conn = pymysql.connect(host=self.settings.get('DB_HOST'),
                                    user=self.settings.get('DB_USER'),
                                    password=self.settings.get('DB_PASSWORD'))
        self.conn.cursor().execute('use consulting_articles;')

     
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        data=json.loads(response.body)
        totalResults=data.get("totalResults")
        results = data.get('results')
        data_dict = collections.defaultdict(dict)
        for result in results:
            ar_title=str(result.get('title'))
            ar_url="https://www.bain.com"+str(result.get('url'))
            ar_type=str(result.get('type'))
            ar_date=str(result.get('date')).split(',')[-1]
            try:
                self.conn.cursor().execute(
                    'INSERT INTO article (ar_title,ar_url,ar_type,ar_date,ar_company)'
                    'values (%s, %s, %s, %s, %s)',(ar_title,ar_url,ar_type,ar_date,"Bain"))
                self.conn.commit()
                request = scrapy.Request(url=ar_url,callback=self.parse_article,meta={"ar_title":ar_title})
                yield request                    
            except Exception as e:
                print('Bain major error')
                print(e)
           
    
    def parse_article(self,response):
        title=response.meta["ar_title"]
        ar_author = response.xpath('//p[@class="hero__byline"]/text()').get()
        ar_content = response.xpath('//div[@class="rte rte--show-end-of-content rte__heading"]').get()
        ar_content_str=str(ar_content)
        cleantext = re.sub(re.compile('<.*?>'), '', ar_content_str)
        try:
             self.conn.cursor().execute( 'UPDATE article SET ar_author=%s, ar_content=%s where ar_title=%s',
             (ar_author,cleantext,title))
             self.conn.commit()
                
        except Exception as e:
            print('Bain content error')
            print(e)


    