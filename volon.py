
# coding: utf-8

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser
from elasticsearch import Elasticsearch 

# config
BASEURL = 'https://forum.vbulletin.com/forum/vbulletin-5-connect/vbulletin-5-suggestions'
MAXPAGE = 20
PAGINATION = False
# ElasticSearch
HOST = 'localhost'
PORT = 9200
INDEX = 'vbulletin'
DOCTYPE = 'post'

# get page URL from pagination
def getPageUrlFromPagination():
    soup = BeautifulSoup(requests.get(BASEURL).text, 'html.parser')
    arrUrl = {}
    for a in soup.find_all('a', href=True , attrs={'class':'js-pagenav-button b-button b-button--narrow js-shrink-event-child b-button--primary page js-pagenav-current-button js-pagenav-first-button'}):
        page = int(a.text)
        arrUrl[page] = (a['href'])
    for a in soup.find_all('a', href=True , attrs={'class':'js-pagenav-button b-button b-button--narrow js-shrink-event-child b-button--secondary'}):
        page = int(a.text)
        arrUrl[page] = (a['href'])
    return arrUrl

# get page URL from page pattern
def getPageUrlFromPattern(baseUrl, maxPage):
    arrUrl = {}
    arrUrl[1] = baseUrl
    for i in range(2,maxPage):
        arrUrl[i] = f'{baseUrl}/page{i}'
    return arrUrl

# utility library
def formatDate(strDate):
    dt = parser.parse(strDate)
    return dt.isoformat()

# parsing Library
def getSubject(tr):
    return tr.find('a', attrs={'class':'topic-title js-topic-title'}).text
def getAuthor(tr):
    return tr.find('div', attrs={'class':'topic-info h-clear h-hide-on-small h-hide-on-narrow-column'}).find('a').text
def getCreatedAt(tr):
    isoCreatedAt = None
    layer = tr.find('div', attrs={'class':'topic-info h-clear h-hide-on-small h-hide-on-narrow-column'})
    if layer:
        strCreatedAt = layer.find('span', attrs={'class':'date'}).text
        isoCreatedAt = formatDate(strCreatedAt)
    return isoCreatedAt
def getResponses(tr):
    intResponse = 0
    strResponses = tr.find('div', attrs={'class':'posts-count'})
    if strResponses:
        strResponses = strResponses.text
        arrResponses = strResponses.split(' ')
        intResponse = int(arrResponses[0].replace(",", ""))
    return intResponse
def getViews(tr):
    intViews = 0
    strViews = tr.find('div', attrs={'class':'views-count'})
    if strViews:
        strViews = strViews.text
        arrViews = strViews.split(' ')
        intViews = int(arrViews[0].replace(",", ""))
    return intViews 
def getLastPostBy(tr):
    strLastPostBy = ''
    cellLastpost = tr.find('td', attrs={'class':'cell-lastpost'})
    if cellLastpost:
        divLastpost=cellLastpost.find('div', attrs={'class':'lastpost-by'})
        if divLastpost:
            strLastPostBy = divLastpost.find('a')
            if strLastPostBy:
                strLastPostBy = strLastPostBy.text
    return strLastPostBy
def getLastPostTime(tr):
    isoLastPostTime = None
    cellLastpost = tr.find('td', attrs={'class':'cell-lastpost'})
    if cellLastpost:
        strLastPostTime = cellLastpost.find('span', attrs={'class':'post-date'})
        if strLastPostTime:
            strLastPostTime = strLastPostTime.text
            isoLastPostTime = formatDate(strLastPostTime)
    return isoLastPostTime

# iterate over page
if PAGINATION:
    arrPage = getPageUrlFromPagination()
else:
    arrPage = getPageUrlFromPattern(BASEURL, MAXPAGE)

# create connection with Elasticsearch
connES=Elasticsearch([{'host':HOST,'port':PORT}])
for i in range(1,MAXPAGE):
    link = arrPage[i]
    objSoupHtml = BeautifulSoup(requests.get(link).text, 'html.parser')
    for tr in objSoupHtml.find_all('tr', attrs={'class':'topic-item'}):
        objPost = {}
        objPost['Post_subject'] = getSubject(tr)
        objPost['Author'] = getAuthor(tr)
        objPost['created_at'] = getCreatedAt(tr)
        objPost['responses'] = getResponses(tr)
        objPost['views_count'] = getViews(tr)
        objPost['last_post_by'] = getLastPostBy(tr)
        objPost['last_post_time'] = getLastPostTime(tr)
        connES.index(index=INDEX, doc_type=DOCTYPE, body=objPost)
print('Completed')
