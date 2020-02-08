import logging
import sqlite3
import threading
import time
import requests
import re

class SinaBulletin(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        logging.basicConfig(level=logging.DEBUG,
                            filename="news.log",
                            datefmt='%Y/%m/%d %H:%M:%S',
                            format='%(asctime)s - %(levelname)s - %(module)s@%(lineno)d : %(message)s')
        self.logger = logging.getLogger()
        self.headers = {
            'Connection': 'keep-alive',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'Accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        }

    def run(self):
        while True:
            connection = sqlite3.connect('news.db')
            cursor = connection.cursor()
            results = None
            try:
                cursor.execute("SELECT code, first FROM codes WHERE state = ?", (1,))
                results = cursor.fetchall()
            except Exception as e:
                self.logger.error(e)
                self.logger.error("Cannot query codes")
            if results:
                for result in results:
                    code = result[0]
                    first = result[1]
                    r = requests.get("https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllBulletin/stockid/%s.phtml"%code[2:], headers=self.headers).content
                    response = str(r, 'gbk')
                    regex = re.compile("<a target='_blank' href='(.+?)'>(.+?)</a>")
                    res = regex.search(response)
                    links = []
                    titles = []
                    while res is not None:
                        links.append("https://vip.stock.finance.sina.com.cn/%s"%res.group(1))
                        titles.append(res.group(2))
                        response = response[res.end():]
                        res = regex.search(response)
                    print("Get %d items" % len(titles))
                    self.logger.info("Get %d items" % len(titles))
                    if titles:
                        for i in range(len(titles)):
                            try:
                                cursor.execute("SELECT id FROM news WHERE link = ?", (links[i],))
                                result = cursor.fetchone()
                            except Exception as e:
                                self.logger.error(e)
                                self.logger.error("Cannot query news %s" % links[i])
                            if not result:
                                print("Insert %s" % links[i])
                                self.logger.info("Insert %s" % links[i])
                                try:
                                    if first == 1:
                                        cursor.execute(
                                            "INSERT INTO news (time, link, content, pushed, code) VALUES (?,?,?,?,?)",
                                            (int(time.time()), links[i], titles[i], 1, code))
                                    else:
                                        cursor.execute(
                                            "INSERT INTO news (time, link, content, pushed, code) VALUES (?,?,?,?,?)",
                                            (int(time.time()), links[i], titles[i], -1, code))
                                    connection.commit()
                                except Exception as e:
                                    self.logger.error(e)
                                    self.logger.error("Cannot insert news %s" % links[i])
                                    connection.rollback()
                    try:
                        cursor.execute("UPDATE codes set first=? WHERE code = ?", (0, code))
                        connection.commit()
                    except Exception as e:
                        self.logger.error(e)
                        self.logger.error("Cannot update code")
                        connection.rollback()
            cursor.close()
            connection.close()

            time.sleep(7200)

sina_bulletin = SinaBulletin()

if __name__ == '__main__':
    sina_bulletin.run()