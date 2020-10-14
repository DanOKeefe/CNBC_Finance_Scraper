import os
import re
import boto3
import requests
import datetime
from bs4 import BeautifulSoup

class Article:
    def __init__(self, paragraphs, url, soup):
        self.soup = soup
        self.url = url
        self.paragraphs = paragraphs

        self.text = ' '.join(self.paragraphs)

        rep = [("\xa0", " "), 
               ("\"", ""), 
               ("&amp;", "&")]

        for r in rep:
            self.text = self.text.replace(*r)

        year, month, day = re.findall(r'/(\d{4})/(\d{1,2})/(\d{1,2})/', self.url)[0]
        self.date = datetime.date(int(year), int(month), int(day))
        
class CNBC_Finance:
    def __init__(self):
        # Retrieve cards from the CNBC Finance homepage
        soup = BeautifulSoup(
            requests.request("GET", "https://www.cnbc.com/finance/").text, 
            'html.parser')
        print("Successfully retrieved CNBC Finance homepage")
        self.cards = soup.find_all("a", class_="Card-title")

        # Retrieve article url from each card
        self.urls = []
        for card in self.cards:
            try:
                self.urls.append(re.search("(?P<url>https?://[^\s]+)", str(card)).group("url")[:-1])
            except:
                pass

        # For each article link, retrieve the text from the article
        self.articles = []
        for url in self.urls:
            soup = BeautifulSoup(requests.request("GET", url).text, 'html.parser')
            print("Retrieved article " + url)
            content = soup.find("div", class_="group")
            paragraphs = content.find_all("p")
            if paragraphs == []:
                # Use the SECOND div with class "group"
                # Some CNBC articles contain a "Key Points" section
                # Which is also in a div with class "group"
                content = soup.find_all("div", class_="group")[1]
                paragraphs = content.find_all("p")
                
            for i, paragraph in enumerate(paragraphs):
                # Removal HTML tags from each paragraph
                paragraphs[i] = self.remove_html_tags(str(paragraph))

            new_article = Article(paragraphs=paragraphs, url=url, soup=soup)
            self.articles.append(new_article)

    def remove_html_tags(self, text):
        """Remove html tags from a string"""
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)

def lambda_handler(event, context):
    myCNBC = CNBC_Finance()
    bucket_name = 'dto-cnbc-finance'
    lambda_path = '/tmp/'
    s3 = boto3.resource('s3')
    
    print('Done retrieving articles. Start inserting into s3')
    
    for article in myCNBC.articles:
        print('inserting article to s3')
        encoded_string = article.text.encode("utf-8")
        filename = article.url[32:53] + '.txt'
        s3_path = f'{article.date.year}/{article.date.month}/{article.date.day}/' + filename
        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_string)
        print(f'Inserted {article.url} into S3 at {s3_path}')
    
    return "Finished!"
