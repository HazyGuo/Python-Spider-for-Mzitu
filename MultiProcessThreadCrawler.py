import os
import re
import time
from lxml import html
import threading
from multiprocessing import Pool,Queue,Manager
import urllib.request
import MeituData

lock = threading.Lock()

def header(referer):
    headers = {
        'Host': 'i.meizitu.net',
        'Pragma': 'no-cache',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/59.0.3071.115 Safari/537.36',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Referer': '{}'.format(referer),
    }
    return headers


def requestpage(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/59.0.3071.115 Safari/537.36'
    }
    req = urllib.request.Request(url=url, headers=headers)
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode('utf-8')


def getthemepages(summarypage, queue, lock):
    selector = html.fromstring(summarypage)
    for node_a in selector.xpath('//ul[@id="pins"]/li/span/a'):
        url = node_a.get('href')
        title = node_a.text
        try:
            lock.acquire()
            queue.put([url, title], False)
        except:
            pass
        finally:
            lock.release()


def getpages(queue, lock):
    pageurl = 'http://www.mzitu.com'
    page = requestpage(pageurl)
    getthemepages(page, queue, lock)
    selector = html.fromstring(page)
    lastpage = selector.xpath('//a[@class="page-numbers"][last()]/@href')[0]
    matchresult = re.match('http://www.mzitu.com/page/(\d+)/?', lastpage)
    summarypages = ['http://www.mzitu.com/page/{}/'.format(i + 1) for i in range(1, int(matchresult.group(1)))]
    for p in summarypages:
        sumpage = requestpage(p)
        getthemepages(sumpage, queue, lock)


def findimg(themepage):
    selector = html.fromstring(themepage)
    return selector.xpath('//div[@class="main-image"]/p/a/img/@src')[0]


def getimgs(themeurl):
    result = []
    themepage = requestpage(themeurl)
    selector = html.fromstring(themepage)
    result.append(findimg(themepage))
    lastpagenum = selector.xpath('//div[@class="pagenavi"]/a[last()-1]/span')[0].text
    for pagenum in range(1, int(lastpagenum)):
        pageurl = '{}/{}'.format(themeurl, pagenum + 1)
        page = requestpage(pageurl)
        result.append(findimg(page))
    return result


def download(url, dir=''):
    filename = url.split('/')[-1]
    req = urllib.request.Request(url=url, headers=header(url))
    with urllib.request.urlopen(req) as res:
        file = open(os.path.join(dir, filename), 'wb')
        file.write(res.read())


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        pass


def runthread(pics = [], dir = ''):
    global lock
    while True:
        picurl = ''
        try:
            if lock.acquire():
                if len(pics) > 0:
                    picurl = pics.pop()
        finally:
            lock.release()
        if picurl:
            print('pop pic {} in process {} thread {}'.format(picurl, os.getpid(), threading.current_thread()))
            download(picurl, dir)
        else:
            break
    print('{} finish'.format(threading.current_thread()))


def runprocess(queue, lock):
    while True:
        try:
            record = queue.get(timeout=10)
            url, title = record
            title = re.sub(r'[\\/:\*\?"<>\|]', '_', title)
            dir = os.path.join('I:\\python\\crawler imgs', title)
            mkdir(dir)
            pics = getimgs(url)
            threads = []
            for i in range(4):
                thread = threading.Thread(target=runthread, args=(pics, dir))
                print('create thread {} in process {}'.format(thread.name,os.getpid()))
                thread.daemon = True
                thread.start()
                threads.append(thread)
            for th in threads:
                th.join()
        except:
            break
    print('process {} complete.'.format(os.getpid()))


def run():
    manager = Manager()
    procqueue = manager.Queue(5000)
    proclock = manager.Lock()
    process = Pool()
    num_cpus = os.cpu_count()
    print('将会启动进程数为：', num_cpus + 1)
    for i in range(num_cpus + 1):
        if i == 0:
            process.apply_async(getpages, args=(procqueue, proclock))
            time.sleep(10)
        else:
            process.apply_async(runprocess, args=(procqueue, proclock))
    process.close()
    process.join()

if __name__ == '__main__':
    run()