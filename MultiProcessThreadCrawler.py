import os
import re
import time
from lxml import html
import threading
from multiprocessing import Pool,Queue,Manager
import urllib.request
import logging
from logging.handlers import RotatingFileHandler

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


def initlogger():
    rotate = RotatingFileHandler('{}.log'.format(os.getpid()), 'w', 5 * 1024 * 1024, 5, delay=True)  # 最多备份5个日志文件,每个文件最大5M mode:w/a 写/追加
    rotate.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s  %(levelname)s  %(thread)d  %(message)s')
    rotate.setFormatter(formatter)
    logging.getLogger('').addHandler(rotate)
    logging.getLogger('').setLevel(logging.DEBUG)  # the loggerlevel is Warn in default, must reset it if want to print the info and debug level log


def requestpage(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/59.0.3071.115 Safari/537.36'
    }
    req = urllib.request.Request(url=url, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as resp:
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
    img = selector.xpath('//div[@class="main-image"]/p/a/img/@src')[0]
    logging.debug('find img {}'.format(img))
    return img


def getimgs(themeurl):
    logging.info('begin get img urls')
    result = []
    try:
        logging.debug('begin request page {}'.format(themeurl))
        themepage = requestpage(themeurl)
        logging.debug('begin analyze code')
        selector = html.fromstring(themepage)
        result.append(findimg(themepage))
        lastpagenum = selector.xpath('//div[@class="pagenavi"]/a[last()-1]/span')[0].text
        for pagenum in range(1, int(lastpagenum)):
            pageurl = '{}/{}'.format(themeurl, pagenum + 1)
            logging.debug('begin request page {}'.format(pageurl))
            page = requestpage(pageurl)
            logging.debug('finish request page')
            result.append(findimg(page))
    except:
        pass
    logging.info('finish get img urls from {}'.format(themeurl))
    return result


def download(url, dir=''):
    filename = url.split('/')[-1]
    req = urllib.request.Request(url=url, headers=header(url))
    with urllib.request.urlopen(req, timeout=5) as res:
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
                picurl = pics.pop()
                logging.debug('pop pic {}'.format(picurl))
        except:
            pass
        if picurl:
            logging.debug('begin download {}'.format(picurl))
            try:
                download(picurl, dir)
            except:
                pass
            logging.debug('complete download {}'.format(picurl))
        else:
            logging.info('thread finish.')
            break


def runprocess(queue, lock):
    initlogger()
    logging.info('process started.')
    while True:
        try:
            logging.debug('begin to get record from queue.')
            record = queue.get(timeout=10)
            logging.debug('finish get record {} from queue'.format(record))
            url, title = record
            title = re.sub(r'[\\/:\*\?"<>\|]', '_', title)
            dir = os.path.join('I:\\python\\crawler imgs', title)
            mkdir(dir)
            pics = getimgs(url)
            threads = []
            while len(pics) > 0:
                for th in threads:
                    if not th.is_alive():
                        threads.remove(th)
                while len(threads) < 4:
                    thread = threading.Thread(target=runthread, args=(pics, dir))
                    thread.daemon = True
                    thread.start()
                    logging.debug('start thread {}'.format(thread.name))
                    threads.append(thread)
                time.sleep(10)
        except:
            break
    logging.info('process complete.')


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