#!/usr/bin/env python
# coding: utf-8



# # 使用BeautifulSoup解析网页获取信息
# 在这里，先用requests库访问网页，然后用bs4库解析信息，然后找到自己需要的电影信息，先保存到自己<br>
# 写的一个类中

# In[1]:


#网页解析库
from bs4 import BeautifulSoup
import lxml
#网络访问库
# from urllib.request import urlopen
# from urllib.request import Request
import requests
#需要的本地信息库
import os  #创建文件夹
import time


# <h5>爬虫的第一步：请求头</h5>
# 其实就是user-agent的设置,很多网站会阻拦没有浏览器请求头的请求<br>
# 太多东西会导致一些不必要的问题，比如编码错误

# In[2]:


db_url = 'https://movie.douban.com/cinema/nowplaying/maoming'
ua_header = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0'
}

today = str(time.localtime().tm_year)+'_'+str(time.localtime().tm_mon)+'_'+str(time.localtime().tm_mday)
folder_path = './img/db_poster/' + today
os.makedirs(folder_path, exist_ok=True)

print('Today is',today)
print("Create folder %s",folder_path)


# <h5>在上映页面爬取数据</h5>
# 首先，我们使用requests的方法访问上映影片的URL——get_movie_on_theatre<br>
# 然后，可以使用bs库来解析网页内容，我们可以在这里找到所有正在上映电影的URL

# In[3]:


#在上映界面爬出各个电影的地址,并返回
def get_movie_on_theatre(url):
    html = requests.get(url,headers=ua_header)
    print('Searching:',html.url)

    soup = BeautifulSoup(html.content,'lxml')
    all_link = soup.find_all('a',{'data-psource':'poster'})
    all_link = [a['href'] for a in all_link]

    [print(l) for l in all_link]
    print('Get',len(all_link),"movies' links")
    return all_link


# In[4]:


#电影信息类,存放电影的各种信息。提供两个方法：打印电影信息，生成电影信息列表
class mv_info():
    def __init__(self,title,director,writer,
                 actor,types,region,language,date,duration,summary,rating):
        self.title  = title
        self.director = director
        if writer != '':
            self.writer = writer
        else:
            self.writer = '未知'
        if actor != '':
            self.actor = actor
        else:
            self.actor = '未知'
        self.types = types
        self.region = region
        self.language = language
        self.date = date
        if duration != '':
            self.duration = duration
        else:
            self.duration = '未知'
        self.summary = summary
        self.rating = rating
    def show_info(self):#类的函数需要给一个self，以便指示类中的变量，如果不给的话，会在调用的时候，提示
                        #  参数错误
        print('电影名：',self.title)
        print('导演：',self.director)
        print('编剧：',self.writer)
        print('演员：',self.actor)
        print('类型：',self.types)
        print('国家：',self.region)
        print('语言：',self.language)
        print('上映日期：',self.date)
        print('时长：',self.duration)
        print('概要：',self.summary)
        print('分数：',self.rating)
    def produce_list(self):
        return [self.title,self.director,self.writer,self.actor,self.types,
               self.region,self.language,self.date,self.duration,self.summary,self.rating]


# <h5>在特定电影的页面爬取信息</h5>

# In[10]:


def search_movie_info(url):
    html = requests.get(url,headers=ua_header)
    soup = BeautifulSoup(html.content,'lxml')
    title = ''
    writer = ''
    actor = ''
    duration = ''

    title = soup.find('span',{'property':'v:itemreviewed'}).get_text()

    poster_url = soup.find('a',{'class':'nbgnbg'}).img['src']
    r = requests.get(poster_url,stream=True)
    with open('./img/db_poster/'+today+'/'+title+'.jpg','wb') as f:
        for chunk in r.iter_content(chunk_size=64):
            f.write(chunk)

    director = soup.find(string='导演').find_parent('span').find_parent('span').get_text()

    if soup.find(string='编剧') is not None:
        writer = soup.find(string='编剧').find_parent('span').find_parent('span').get_text()

    if soup.find(string='主演') is not None:
        actor = soup.find(string='主演').find_parent('span').find_parent('span').get_text()

    types = soup.find_all('span',{'property':'v:genre'})
    types = [s.get_text() for s in types]
    types = ''.join(types)

    region = soup.find(string='制片国家/地区:').find_next(string=True).strip()

    language = soup.find(string='语言:').find_next(string=True).strip()

    date =soup.find(string='上映日期:').find_next('span').get_text()
    if soup.find(string='片长:') is not None:
        duration = soup.find(string='片长:').find_next('span').get_text()

    summary = soup.find('span',{'property':'v:summary'}).get_text().strip()

    rate = soup.find('strong',{'class':'ll rating_num'}).get_text()

    movie = mv_info(title,director,writer,actor,types,region,
                    language,date,duration,summary,rate)
    print('Movie:',title,'get')
    return movie


# <h5>把爬到的电影信息写入数据库</h5>

# In[7]:


def write_database(all_movie_info):
    import pymysql
    import pandas as pd
    from sqlalchemy import create_engine
    mv_list = [m.produce_list() for m in all_movie_info]
    movie_df = pd.DataFrame(mv_list,
                columns=['title','director','writer','actor','types',
                        'region','language','date','duration','summary','rating'])
    engine = create_engine(
    "mysql+pymysql://LH:lianghao@139.199.88.96:3306/movie_theatre_info?charset=utf8")

    table_name = today
    movie_df.to_sql(table_name,engine,index=False,if_exists='replace')
    print('Write Database succeed')
    return movie_df


# <h5>主函数</h5>

# In[11]:


def main():
    movie_links = get_movie_on_theatre(db_url)
    all_movie_info = [search_movie_info(movie_link) for movie_link in movie_links]
    movie_df = write_database(all_movie_info)
    movie_df
if __name__=='__main__':
    main()


# # 单个电影的测试

# In[114]:


# 从网站把电影的基本信息爬下来
url = 'https://movie.douban.com/subject/30269016/?from=playing_poster'
mv_html = requests.get(url,headers=ua_header)
soup = BeautifulSoup(mv_html.content,'lxml')


# In[111]:


poster = soup.find('a',
                  {
                      'class':'nbgnbg'
                  })
print(poster,'\n')
print(poster.img,'\n')
print(poster.img['src'])

