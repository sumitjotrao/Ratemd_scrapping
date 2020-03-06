import scrapy
from scrapy.crawler import CrawlerProcess
import csv
import pandas
from scrapy.item import Item, Field

class First_scrapyItem(scrapy.Item):
   name = scrapy.Field()
   url = scrapy.Field()
   desc = scrapy.Field()
   rate=scrapy.Field()
   reviews=scrapy.Field()
   rank=scrapy.Field()

class Spider1(scrapy.Spider):
    name='spider_1'

    #############weaving the web##################
    def start_requests(self):

      urls=['https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=2','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=3','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=4','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=5','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=6','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=7','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=8','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=9','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=10','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=11','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=12','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=13','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=14','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=15','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=16','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=17','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=18','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=19','https://www.ratemds.com/best-doctors/ny/buffalo/family-gp/?page=20']
      for url in urls:
          yield scrapy.Request(url=url,callback=self.parse)
    def parse(self,response):
        item = First_scrapyItem()
        #extract the name
        links1=response.css('img.search-item-image')
        links=links1.xpath('./@alt').extract()

        #extract the ratings
        b=response.xpath('//span[@class="star-rating"]/@title').extract()
        while len(b)<10:
            s=len(b)
            for i in range(s,10):
                b.append(0)
        print(b)
        #extract the number of reviews
        c=response.xpath('//div[@class="star-rating-count"]/span[1]/text()').extract()
        while len(c)<10:
            l=len(c)
            for i in range(l,10):
                c.append(0)
        #print(c)
        #extract the specialty of doctor
        d=response.xpath('//div[@class="search-item-specialty"]/a/text()').extract()

        #extract the link for doctors page
        e=response.xpath('//h2[@class="search-item-doctor-name"]/a/@href').extract()
        site_link=['https://www.ratemds.com']
        for i in range(len(e)):
            e[i]=site_link[0]+e[i]

        for link in e:
            yield response.follow(url=link, callback=self.parse2)

        df = pandas.DataFrame(data={"name": links, "rating": b,"number_of_reviews":c,"specialty":d,"profile_url":e})
        temp1['name'].append(df['name'])
        temp1['rating'].append(df['rating'])
        temp1['number_of_reviews'].append(df['number_of_reviews'])
        temp1['specialty'].append(df['specialty'])
        temp1['profile_url'].append(df['profile_url'])



    def parse2(self,response):
            k=response.xpath('//div[@class="col-sm-6"]/h1/text()').extract()
            g=response.xpath('//div[@class="search-item-info"]/div/span/span[2]/text()').extract()
            gen=response.xpath('//div[@class="col-sm-3 col-md-4 search-item-extra"]/div/div/a/text()').extract()
            temp[k[0]]=[g[0]]
            gen_dict[k[0]]=[gen[0]]


temp=dict()
temp1=dict({'name':[],'rating':[],'number_of_reviews':[],'specialty':[],'profile_url':[]})

gen_dict=dict()
process=CrawlerProcess()
process.crawl(Spider1)
process.start()

print("asasdas",temp1)
name=[]
for i in range(len(temp1['name'])):
 for j in range(len(temp1['name'][i])):
    name.append(temp1['name'][i][j])

rating=[]
for i in range(len(temp1['rating'])):
 for j in range(len(temp1['rating'][i])):
    rating.append(temp1['rating'][i][j])

number_of_reviews=[]
for i in range(len(temp1['number_of_reviews'])):
 for j in range(len(temp1['number_of_reviews'][i])):
    number_of_reviews.append(temp1['number_of_reviews'][i][j])

specialty=[]
for i in range(len(temp1['specialty'])):
 for j in range(len(temp1['specialty'][i])):
    specialty.append(temp1['specialty'][i][j])

profile_url=[]
for i in range(len(temp1['profile_url'])):
 for j in range(len(temp1['profile_url'][i])):
    profile_url.append(temp1['profile_url'][i][j])

page_1=dict({'name':name,'rating':rating,'number_of_reviews':number_of_reviews,'specialty':specialty,'profile_url':profile_url})

#data from main page
page_1_df=pandas.DataFrame(page_1)
print("gjkkjgkjk",page_1_df)

#rank from subsequent pages_1
df=pandas.DataFrame(temp.items())
print(df)
df=df.drop([0],axis=1)

#gender data from subsequent pages_1
df2=pandas.DataFrame(gen_dict.items())
df2=df2.drop([0],axis=1)
df_final=pandas.concat([page_1_df,df,df2],axis=1)
print(df_final)
print(df2)
df_final.to_csv("./file.csv", sep=",", index=False)



