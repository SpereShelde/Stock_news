import logging
from telegram.ext import Updater, CallbackContext
import sqlite3
from eastmoney import east_money

open("boxHelper.log", "w").close()
east_money.start()
logging.basicConfig(level=logging.DEBUG,
                        filename="news.log",
                        datefmt='%Y/%m/%d %H:%M:%S',
                        format='%(asctime)s - %(levelname)s - %(module)s@%(lineno)d : %(message)s')
logger = logging.getLogger()

def push_news(context: CallbackContext):
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    results = None
    try:
        cursor.execute("SELECT time, content, link FROM eastmoney WHERE pushed = ?", (-1,))
        results = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query unpushed news")
    if results:
        for result in results:
            context.bot.send_message(chat_id='503215663',
                                 text='Time: %s\nContent: %s\nLink: %s'%(result[0], result[1], result[2]))
            try:
                cursor.execute("UPDATE eastmoney set pushed=? WHERE link = ?", (1,result[2]))
                connection.commit()
            except Exception as e:
                logger.error(e)
                logger.error("Cannot query unpushed news")
                connection.rollback()
    cursor.close()
    connection.close()
    print("Pushed %d news" % len(results))
    logger.info("Pushed %d news" % len(results))


updater = Updater('', use_context=True)

updater.job_queue.run_repeating(push_news, interval=10, first=0)

updater.start_polling()
updater.idle()
