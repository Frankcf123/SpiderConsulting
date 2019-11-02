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
        self.url = ("https://www.mckinsey.com/services/ContentAPI/SearchAPI.svc/search")
        # url="https://www.mckinsey.com/business-functions/mckinsey-analytics/our-insights?UseAjax=true&PresentationId={225AFD08-2381-4FCD-B73F-F68A6859B4FC}&ds={E30B533C-7F08-4172-8780-F4CF9F3B832F}&showfulldek=False&hideeyebrows=False&ig_page=20"
        frmdata={"q":"artificial%20intelligence","page":"1","app":"","sort":"default"}

        self.conn = pymysql.connect(host=self.settings.get('DB_HOST'),
                                    user=self.settings.get('DB_USER'),
                                    password=self.settings.get('DB_PASSWORD'))
        self.conn.cursor().execute('use consulting_articles;')

        # frmdata={}
        # yield scrapy.Request(url=url, method="POST",callback=self.parse,body=json.dumps(frmdata))
        # yield scrapy.Request(url=url, callback=self.parse)
        yield scrapy.http.JsonRequest(url=self.url, data=frmdata)

    def parse(self, response):
        print(response.body)
        data=json.loads(response.body).get('data')
        totalResults=data.get("totalResults")
        totalPages=data.get("totalPages")
        currentPages=data.get("currentPage")
        results = data.get('results')
        data_dict = collections.defaultdict(dict)
        for result in results:
            ar_title=str(result.get('title'))
            ar_url=str(result.get('url'))
            ar_tags=str(result.get('tag'))
            if "Video" in ar_tags or "Podcast" in ar_tags:
                pass
            else:
                if ar_tags:
                    ar_date=ar_tags.split()[-1]
                else:
                    ar_date=""
                try:
                    self.conn.cursor().execute(
                        'INSERT INTO article (ar_title,ar_url,ar_date,ar_company)'
                        'values (%s, %s, %s, %s)',(ar_title,ar_url,ar_date,"mckinsey"))
                    self.conn.commit()
                    request = scrapy.Request(url=ar_url,callback=self.parse_article,meta={"ar_title":ar_title})
                    # yield request                    
                except Exception as e:
                    print('Bain major error')
                    print(e)
                if totalPages>currentPages:
                    
                    frmdata={"q":"artificial%20intelligence","page":currentPages+1,"app":"","sort":"default"}
                    yield scrapy.http.JsonRequest(url=self.url, data=frmdata)
                    
           
    
    def parse_article(self,response):
        title=response.meta["ar_title"]
        ar_author = response.xpath('//div[@class="author-by-line"]').get()
        ar_content = response.xpath('//article[@class="main-copy text-longform main"]').get()
        ar_content_str=str(ar_content)
        cleantext_content = re.sub(re.compile('<.*?>'), '', ar_content_str)
        cleantext_author= re.sub(re.compile('<.*?>'), '', ar_author)
        try:
             self.conn.cursor().execute( 'UPDATE article SET ar_author=%s, ar_content=%s where ar_title=%s',
             (cleantext_author,cleantext_content,title))
             self.conn.commit()
                
        except Exception as e:
            print('Bain content error')
            print(e)


    