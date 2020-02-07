import logging
from telegram.ext import Updater, CallbackContext, CommandHandler
import sqlite3
from eastmoney import east_money
from sina_bulletin import sina_bulletin
from sina_news import sina_news

open("news.log", "w").close()

east_money.start()
sina_news.start()
sina_bulletin.start()

logging.basicConfig(level=logging.DEBUG,
                        filename="news.log",
                        datefmt='%Y/%m/%d %H:%M:%S',
                        format='%(asctime)s - %(levelname)s - %(module)s@%(lineno)d : %(message)s')
logger = logging.getLogger()

def menu(update, context):
    menu = "Hi,%s. I'm here.\nWhat can I do for you today?\n/m View main menu\n/a Add stock code, like \"sh600519\"\n/rm Remove stock code, like \"sh600519\"\n/sk Skip key word\n/ca Cancel skipping key word" % update.message.from_user['first_name']
    context.bot.send_message(chat_id=update.message.from_user['id'], text=menu)

def add(update, context):
    code = update.message.text.rstrip().lstrip()[3:]
    id = update.message.from_user['id']
    if not code:
        context.bot.send_message(chat_id=id, text="Please append stock code after /add\nlike \"/add sh600519\"")
        return
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT id FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.")
        return
    result = None
    try:
        cursor.execute("SELECT id FROM codes WHERE code = ? and chat_id = ?", (code, id))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query stock code.")
    if result:
        try:
            cursor.execute("UPDATE codes set state=? WHERE code = ? and chat_id = ?", (1, code, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update stock code")
            connection.rollback()
    else:
        try:
            cursor.execute("INSERT INTO codes (code, state, chat_id, first) VALUES (?,?,?,?)", (code, 1, id, 1))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot insert stock code")
            connection.rollback()
    result = None
    try:
        cursor.execute("SELECT code FROM codes WHERE state = ? and chat_id = ?", (1, id))
        result = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query stock code")
    cursor.close()
    connection.close()
    words = "Successfully add new stock code %s\nNow I push news of these codes to you:" % code
    if result:
        for re in result:
            words += "\n%s" % re[0]
    context.bot.send_message(chat_id=id, text=words)

def remove(update, context):
    code = update.message.text.rstrip().lstrip()[4:]
    id = update.message.from_user['id']
    if not code:
        context.bot.send_message(chat_id=id, text="Please append stock code after /rm\nlike \"/rm sh600519\"")
        return
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT id FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.")
        return
    result = None
    try:
        cursor.execute("SELECT id FROM codes WHERE code = ? and chat_id = ?", (code, id))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query code")
    if result:
        try:
            cursor.execute("UPDATE codes set state=? WHERE code = ? and chat_id = ?", (-1, code, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update code")
            connection.rollback()
        words = "Successfully remove stock code %s\nNow I push news of those codes:" % code
    else:
        words = "I currently do not push news of \"%s\"\nInstead, I push those codes:" % code
    result = None
    try:
        cursor.execute("SELECT code FROM codes WHERE state = ? and chat_id = ?", (1, id))
        result = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query code")
    cursor.close()
    connection.close()
    if result:
        for re in result:
            words += "\n%s" % re[0]
    context.bot.send_message(chat_id=id, text=words)

def skip(update, context):
    word = update.message.text.rstrip().lstrip()[4:]
    id = update.message.from_user['id']
    if not word:
        context.bot.send_message(chat_id=id, text="Please append the word after /sk\nlike \"/sk 提问\"")
        return
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT id FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.")
        return
    result = None
    try:
        cursor.execute("SELECT id FROM skip_words WHERE word = ? and chat_id = ?", (word,id))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query skip word")
    if result:
        try:
            cursor.execute("UPDATE skip_words set state=? WHERE word = ? and chat_id = ?", (1, word, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update skip word")
            connection.rollback()
    else:
        try:
            cursor.execute("INSERT INTO skip_words (word, state, chat_id) VALUES (?,?,?)", (word, 1,id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot insert skip word")
            connection.rollback()
    result = None
    try:
        cursor.execute("SELECT word FROM skip_words WHERE state = ? and chat_id = ?", (1,id))
        result = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query skip word")
    cursor.close()
    connection.close()
    words = "Successfully add new skip word %s\nNow I skip those key words:" % word
    if result:
        for re in result:
            words += "\n%s" % re[0]
    context.bot.send_message(chat_id=id, text=words)

def cancel(update, context):
    word = update.message.text.rstrip().lstrip()[4:]
    id = update.message.from_user['id']
    if not word:
        context.bot.send_message(chat_id=id, text="Please append the word after /ca\nlike \"/ca 提问\"")
        return
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT id FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.")
        return
    result = None
    try:
        cursor.execute("SELECT id FROM skip_words WHERE word = ? and chat_id = ?", (word,id))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query skip word")
    if result:
        try:
            cursor.execute("UPDATE skip_words set state=? WHERE word = ? and chat_id = ?", (-1, word, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update skip word")
            connection.rollback()
        words = "Successfully remove skip word %s\nNow I skip those key words:" % word
    else:
        words = "I currently do not skip the word \"%s\"\nInstead, I skip those key words:" % word
    result = None
    try:
        cursor.execute("SELECT word FROM skip_words WHERE state = ? and chat_id = ?", (1, id))
        result = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query skip word")
    cursor.close()
    connection.close()

    if result:
        for re in result:
            words += "\n%s" % re[0]
    context.bot.send_message(chat_id=id, text=words)

def start(update, context):
    id = update.message.from_user['id']
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT id FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        try:
            cursor.execute("INSERT INTO users (chat_id, state, language) VALUES (?,?,?)",
                           (id, 1, 1))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot add new user %d" % id)
            connection.rollback()
        context.bot.send_message(chat_id=id, text="Congratulations!\nYou are now registered.\nI'm going to push news to you.\nType /sp to stop.")
        return
    else:
        try:
            cursor.execute("UPDATE users set state=? WHERE chat_id = ?", (1, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update user state.")
            connection.rollback()
        context.bot.send_message(chat_id=id,
                                 text="You already registered.\nType /sp to stop.")
        return

def stop(update, context):
    id = update.message.from_user['id']
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT id FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id,
                                 text="How can I stop if I haven't even started?")
        return
    else:
        try:
            cursor.execute("UPDATE users set state=? WHERE chat_id = ?", (-1, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update user state.")
            connection.rollback()
        context.bot.send_message(chat_id=id,
                                 text="Alright.\nI'll go offline for a while...\nType /st to start again.\nMiss you.")
        return

def push_news(context: CallbackContext):
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    try:
        cursor.execute("UPDATE news set pushed=? WHERE code IN (SELECT code FROM codes where first = ?)", (1, 1))
        cursor.execute("UPDATE codes set first=? WHERE first = ?", (0, 1))
        connection.commit()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot update unpushed news")
        connection.rollback()
    results = None
    try:
        cursor.execute("SELECT content, link, code from news WHERE pushed = ? ORDER BY time ASC", (-1,))
        results = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query unpushed news")
    if results:
        for result in results:
            content = result[0]
            link = result[1]
            code = result[2]
            try:
                cursor.execute("UPDATE news set pushed=? WHERE link = ?", (1, link))
                connection.commit()
            except Exception as e:
                logger.error(e)
                logger.error("Cannot update unpushed news")
                connection.rollback()

            users = None
            try:
                if code == "General":
                    cursor.execute("SELECT chat_id from codes WHERE state = ? and first = ? GROUP BY chat_id", (1, 0))
                else:
                    cursor.execute("SELECT chat_id from codes WHERE code = ? and first = ?", (code, 0))
                users = cursor.fetchall()
            except Exception as e:
                logger.error(e)
                logger.error("Cannot query users")
            if users:
                for user in users:
                    id = user[0]
                    words = None
                    try:
                        cursor.execute("SELECT word FROM skip_words WHERE state = ? and chat_id = ?", (1, id))
                        words = cursor.fetchall()
                    except Exception as e:
                        logger.error(e)
                        logger.error("Cannot query skip words")
                    skip = False
                    if words:
                        for word in words:
                            if word[0] in content:
                                skip = True
                                break
                    if not skip:
                        context.bot.send_message(chat_id=id, text='%s:\n%s\n%s' % (code, content, link))
    cursor.close()
    connection.close()
    # print("Pushed %d news but skip %d" % (len(results) - skip_items, skip_items))
    # logger.info("Pushed %d news but skip %d" % (len(results) - skip_items, skip_items))

updater = Updater('', use_context=True)
updater.job_queue.run_repeating(push_news, interval=30, first=0)
updater.dispatcher.add_handler(CommandHandler('m', menu))
updater.dispatcher.add_handler(CommandHandler('menu', menu))
updater.dispatcher.add_handler(CommandHandler('start', menu))
updater.dispatcher.add_handler(CommandHandler('a', add))
updater.dispatcher.add_handler(CommandHandler('rm', remove))
updater.dispatcher.add_handler(CommandHandler('sk', skip))
updater.dispatcher.add_handler(CommandHandler('ca', cancel))
updater.dispatcher.add_handler(CommandHandler('st', start))
updater.dispatcher.add_handler(CommandHandler('sp', stop))
updater.start_polling()
updater.idle()
