import scrapy
import json
import pymysql
import collections
import re

pymysql.install_as_MySQLdb()



class BostonSpider(scrapy.Spider):
    name = "boston"

    def start_requests(self):
        allowed_domains = ['www.bcg.com']
        url = (
            "https://rbet5di12b.execute-api.us-east-1.amazonaws.com/Prod/v1/content/_search"
        )
        frmdata={"query":{"bool":{"must":[{"function_score":{"filter":{"bool":{"must":[{"term":{"lens":"global"}}],"must_not":{"term":{"_type":"video"}}}}}},{"function_score":{"query":{"bool":{"should":[{"multi_match":{"query":"artificial intelligence","fields":["article_title","browser_title","event_name","full_name","article_teaser","event_teaser","in_the_news_body","seo_description","authors","capabilities","consulting_title","country","education_background_school","industries","language","content_type","featured_topics","body","office.en","article_title.en","browser_title.en","event_name.en","article_teaser.en","event_teaser.en","in_the_news_body.en","seo_description.en","authors.en","capabilities.en","consulting_title.en","country.en","education_background_school.en","industries.en","language.en","office.en","content_type.en","featured_topics.en","body.en","article_title.folded","browser_title.folded","event_name.folded","article_teaser.folded","event_teaser.folded","in_the_news_body.folded","seo_description.folded","authors.folded","capabilities.folded","consulting_title.folded","country.folded","education_background_school.folded","industries.folded","language.folded","office.folded","content_type.folded","featured_topics.folded"],"type":"most_fields","operator":"AND","boost":30}},{"multi_match":{"query":"artificial intelligence","fields":["first_name","last_name","first_name.folded","last_name.folded"],"type":"cross_fields","operator":"AND","boost":20}},{"multi_match":{"query":"artificial intelligence","fields":["article_title.edge","browser_title.edge","event_name.edge","article_teaser.edge","event_teaser.edge","in_the_news_body.edge","seo_description.edge","authors.edge","capabilities.edge","consulting_title.edge","country.edge","education_background_school.edge","industries.edge","language.edge","content_type.edge","featured_topics.edge","body.edge"],"type":"best_fields","minimum_should_match":"75%","operator":"AND","boost":20}},{"multi_match":{"query":"artificial intelligence","fields":["first_name.edge","last_name.edge"],"type":"best_fields","minimum_should_match":"75%","operator":"AND","boost":20}},{"multi_match":{"query":"artificial intelligence","fields":["article_title","browser_title","event_name","full_name","article_teaser","event_teaser","in_the_news_body","seo_description","authors","capabilities","consulting_title","country","education_background_school","industries","language","content_type","featured_topics","body","office.en","article_title.en","browser_title.en","event_name.en","article_teaser.en","event_teaser.en","in_the_news_body.en","seo_description.en","authors.en","capabilities.en","consulting_title.en","country.en","education_background_school.en","industries.en","language.en","office.en","content_type.en","featured_topics.en","body.en","article_title.folded","browser_title.folded","event_name.folded","article_teaser.folded","event_teaser.folded","in_the_news_body.folded","seo_description.folded","authors.folded","capabilities.folded","consulting_title.folded","country.folded","education_background_school.folded","industries.folded","language.folded","office.folded","content_type.folded","featured_topics.folded"],"type":"most_fields","minimum_should_match":"50%"}}],"must_not":{"term":{"_type":"video"}}}},"filter":{"bool":{"must":[{"term":{"lens":"global"}}]}},"min_score":0.0002}}]}},"post_filter":{"nested":{"path":"tax-content_type","query":{"term":{"tax-content_type.value":"Publications"}}}},"aggs":{"tax-content_type":{"filter":{"match_all":{}},"aggs":{"children":{"nested":{"path":"tax-content_type"},"aggs":{"lvl0":{"filter":{"term":{"tax-content_type.level":1}},"aggs":{"children":{"terms":{"field":"tax-content_type.value","size":2147483647}}}},"lvl1":{"filter":{"bool":{"must":[{"term":{"tax-content_type.level":2}},{"term":{"tax-content_type.ancestors":"Publications"}}]}},"aggs":{"children":{"terms":{"field":"tax-content_type.value","size":2147483647}}}}}}}},"featured_topics5":{"filter":{"nested":{"path":"tax-content_type","query":{"term":{"tax-content_type.value":"Publications"}}}},"aggs":{"featured_topics.raw":{"terms":{"field":"featured_topics.raw","size":5}},"featured_topics.raw_count":{"cardinality":{"field":"featured_topics.raw"}}}},"offices6":{"filter":{"nested":{"path":"tax-content_type","query":{"term":{"tax-content_type.value":"Publications"}}}},"aggs":{"offices.raw":{"terms":{"field":"offices.raw","size":5}},"offices.raw_count":{"cardinality":{"field":"offices.raw"}}}},"tax-industries":{"filter":{"nested":{"path":"tax-content_type","query":{"term":{"tax-content_type.value":"Publications"}}}},"aggs":{"children":{"nested":{"path":"tax-industries"},"aggs":{"lvl0":{"filter":{"term":{"tax-industries.level":1}},"aggs":{"children":{"terms":{"field":"tax-industries.value","size":2147483647}}}}}}}},"tax-capabilities":{"filter":{"nested":{"path":"tax-content_type","query":{"term":{"tax-content_type.value":"Publications"}}}},"aggs":{"children":{"nested":{"path":"tax-capabilities"},"aggs":{"lvl0":{"filter":{"term":{"tax-capabilities.level":1}},"aggs":{"children":{"terms":{"field":"tax-capabilities.value","size":2147483647}}}}}}}},"language9":{"filter":{"nested":{"path":"tax-content_type","query":{"term":{"tax-content_type.value":"Publications"}}}},"aggs":{"language.raw":{"terms":{"field":"language.raw","size":5}},"language.raw_count":{"cardinality":{"field":"language.raw"}}}},"country10":{"filter":{"nested":{"path":"tax-content_type","query":{"term":{"tax-content_type.value":"Publications"}}}},"aggs":{"country.raw":{"terms":{"field":"country.raw","size":5}},"country.raw_count":{"cardinality":{"field":"country.raw"}}}}},"size":500,"from":0,"sort":[{"_score":"desc"}]}

        self.conn = pymysql.connect(host=self.settings.get('DB_HOST'),
                                    user=self.settings.get('DB_USER'),
                                    password=self.settings.get('DB_PASSWORD'))
        self.conn.cursor().execute('use consulting_articles;')
        yield scrapy.Request(url=url, method="POST",callback=self.parse,body=json.dumps(frmdata))

    def parse(self, response):
        data=json.loads(response.body).get("hits")
        totalResults=data.get("total")
        results = data.get('hits')
        data_dict = collections.defaultdict(dict)
        for res in results:
            result=res.get('_source')
            ar_content=str(result.get('body'))
            ar_title=str(result.get('title'))
            ar_url="https://www.bcg.com"+str(result.get('url'))
            ar_type=str(result.get('content_type'))
            ar_date=str(result.get('article_date')).split('-')[0]
            try:
                self.conn.cursor().execute(
                    'INSERT INTO article (ar_title,ar_url,ar_type,ar_date,ar_company,ar_content)'
                    'values (%s, %s, %s, %s, %s, %s)',(ar_title,ar_url,ar_type,ar_date,"Boston",ar_content))
                self.conn.commit()
                # request = scrapy.Request(url=ar_url,callback=self.parse_article,meta={"ar_title":ar_title})
                # yield request                    
            except Exception as e:
                print('Boston major error')
                print(e)
           
    
    def parse_article(self,response):
        title=response.meta["ar_title"]
        ar_author=response.xpath('//div[@class="container-text date-and-author"]/div/div/p').get()
        author_str = re.sub(re.compile('<.*?>'), '', str(ar_author))
        try:
             self.conn.cursor().execute( 'UPDATE article SET ar_author=%s, where ar_title=%s',
             (author_str,title))
             self.conn.commit()
                
        except Exception as e:
            print('Boston content error')
            print(e)


    