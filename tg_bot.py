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
    id = update.message.from_user['id']
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT state, language FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if result:
        if result[1] == 1: # English
            menu = "Hi,%s. I'm here.\n" \
                   "What can I do for you today?\n" \
                   "/m View main menu\n" \
                   "/a Add stock code, like \"sh600519\"\n" \
                   "/rm Remove stock code, like \"sh600519\"\n" \
                   "/vc View all stock codes" \
                   "/sk Add skip word\n" \
                   "/ca Remove skip word" \
                   "/vw View all skip words\n" \
                   "/st Start pushing news\n" \
                   "/sp Stop pushing news\n" \
                   "/cn 中文\n" % update.message.from_user['first_name']
            if result[0] == 1:
                menu += "You are receiving news push"
            else:
                menu += "You are not receiving news push"
        else:
            menu = "您好,%s. 很高兴为您服务.\n" \
                   "今天想做些什么呢\n" \
                   "/m 主菜单\n" \
                   "/a 添加股票代码, 格式如 \"sh600519\"\n" \
                   "/rm 移除股票代码, 格式如 \"sh600519\"\n" \
                   "/vc 查看所有股票代码" \
                   "/sk 添加屏蔽词\n" \
                   "/ca 移除屏蔽词\n" \
                   "/vw 查看所有屏蔽词\n" \
                   "/st 开始接受新闻推送\n" \
                   "/sp 停止接受新闻推送\n" \
                   "/en English" % update.message.from_user['first_name']
            if result[0] == 1:
                menu += "You are receiving news push"
            else:
                menu += "You are not receiving news push"
        context.bot.send_message(chat_id=update.message.from_user['id'], text=menu)
    else:
        menu = "Hi,%s. I'm here.\n" \
               "What can I do for you today?\n" \
               "/m View main menu\n" \
               "/a Add stock code, like \"sh600519\"\n" \
               "/rm Remove stock code, like \"sh600519\"\n" \
               "/vc View all stock codes" \
               "/sk Add skip word\n" \
               "/ca Remove skip word\n" \
               "/vw View all skip words\n" \
               "/st Start pushing news\n" \
               "/sp Stop pushing news\n" % update.message.from_user['first_name']
        context.bot.send_message(chat_id=id, text=menu)
        return
    cursor.close()
    connection.close()


def add(update, context):
    code = update.message.text.rstrip().lstrip()[3:]
    id = update.message.from_user['id']
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT language FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    lan = result[0]
    if not code:
        if lan == 1:
            context.bot.send_message(chat_id=id, text="Please append stock code after /a\nlike \"/add sh600519\"")
        else:
            context.bot.send_message(chat_id=id, text="请在 /a 命令后输入股票代码\n比如 \"/a sh600519\"")
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
    if lan == 1:
        words = "Successfully add new stock code %s\nNow I push news of these codes to you:" % code
    else:
        words = "成功添加股票代码 %s\n现在我推送这些代码的公司新闻:" % code
    if result:
        for re in result:
            words += "\n%s" % re[0]
    context.bot.send_message(chat_id=id, text=words)

def remove(update, context):
    code = update.message.text.rstrip().lstrip()[4:]
    id = update.message.from_user['id']
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT language FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    lan = result[0]
    if not code:
        if lan == 1:
            context.bot.send_message(chat_id=id, text="Please append stock code after /rm\nlike \"/rm sh600519\"")
        else:
            context.bot.send_message(chat_id=id, text="请在 /rm 命令后输入股票代码\n比如 \"/rm sh600519\"")
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
        if lan == 1:
            words = "Successfully remove stock code %s\nNow I push news of those codes:" % code
        else:
            words = "成功删除股票代码 %s\n现在我推送这些代码的公司:" % code
    else:
        words = "我现在并不推送 \"%s\"\n我只推送这些代码的公司:" % code
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
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    lan = result[0]
    if not word:
        if lan == 1:
            context.bot.send_message(chat_id=id, text="Please append the word after /sk\nlike \"/sk 提问\"")
        else:
            context.bot.send_message(chat_id=id, text="请在 /sk 命令后输入关键词\nlike \"/sk 提问\"")
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
    if lan == 1:
        words = "Successfully add new skip word %s\nNow I skip those key words:" % word
    else:
        words = "成功添加筛选关键词 %s\n我现在不推送包含以下关键词的新闻:" % word
    if result:
        for re in result:
            words += "\n%s" % re[0]
    context.bot.send_message(chat_id=id, text=words)

def cancel(update, context):
    word = update.message.text.rstrip().lstrip()[4:]
    id = update.message.from_user['id']
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT language FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    lan = result[0]
    if not word:
        if lan == 1:
            context.bot.send_message(chat_id=id, text="Please append the word after /ca\nlike \"/ca 提问\"")
        else:
            context.bot.send_message(chat_id=id, text="请在 /ca 命令后输入关键词\n比如 \"/ca 提问\"")
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
        if lan == 1:
            words = "Successfully remove skip word %s\nNow I skip those key words:" % word
        else:
            words = "成功删除筛选关键词 %s\n我现在不推送包含以下关键词的新闻:" % word
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
        cursor.execute("SELECT language FROM users WHERE chat_id = ?", (id,))
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
        context.bot.send_message(chat_id=id, text="Congratulations! You are now registered\nI'm going to push news to you.\nType /sp to stop.")
        return
    else:
        lan = result[0]
        try:
            cursor.execute("UPDATE users set state=? WHERE chat_id = ?", (1, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update user state.")
            connection.rollback()
        if lan == 1:
            context.bot.send_message(chat_id=id, text="You already registered.\nType /sp to stop.")
        else:
            context.bot.send_message(chat_id=id, text="您已经注册\n输入 /sp 停止推送")
        return

def stop(update, context):
    id = update.message.from_user['id']
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute("SELECT language FROM users WHERE chat_id = ?", (id,))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query user.")
    if not result:
        context.bot.send_message(chat_id=id, text="How can I stop if I haven't even started?")
        return
    else:
        lan = result[0]
        try:
            cursor.execute("UPDATE users set state=? WHERE chat_id = ?", (-1, id))
            connection.commit()
        except Exception as e:
            logger.error(e)
            logger.error("Cannot update user state.")
            connection.rollback()
        if lan == 1:
            context.bot.send_message(chat_id=id, text="Alright. I'll go offline for a while...\nType /st to start again. Miss you.")
        else:
            context.bot.send_message(chat_id=id,
                                     text="好吧. 我离开一段时间...\n输入 /st 召回我哦，会想你的")
        return

def push_news(context: CallbackContext):
    connection = sqlite3.connect('news.db')
    cursor = connection.cursor()
    results = None
    try:
        cursor.execute("SELECT content, link, code from news WHERE pushed = ? ORDER BY time ASC LIMIT 5", (-1,))
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

def view_codes(update, context):
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
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    lan = result[0]
    result = None
    try:
        cursor.execute("SELECT code FROM codes WHERE chat_id = ? and state = ?", (id, 1))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query stock code.")
    if result:
        if lan == 1:
            code = "I push news of these codes to you:"
        else:
            code = "我目前推送这些代码的公司新闻:"
        for re in result:
            code += "\n%s" % re[0]
    else:
        code = "Currently do not push any company news to you."
    context.bot.send_message(chat_id=id, text=code)

def view_words(update, context):
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
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    lan = result[0]
    result = None
    try:
        cursor.execute("SELECT word FROM skip_words WHERE chat_id = ? and state = ?", (id, 1))
        result = cursor.fetchone()
    except Exception as e:
        logger.error(e)
        logger.error("Cannot query skip words.")
    if result:
        if lan == 1:
            word = "I skip these key words for now:"
        else:
            word = "我目前不推送包含以下关键词的新闻:"
        for re in result:
            word += "\n%s" % re[0]
    else:
        code = "Currently do not skip any key words."
    context.bot.send_message(chat_id=id, text=word)

def english(update, context):
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
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    try:
        cursor.execute("UPDATE users set language=? WHERE chat_id = ?", (1, id))
        connection.commit()
        context.bot.send_message(chat_id=id, text="Ok, now we speak English.")
    except Exception as e:
        logger.error(e)
        logger.error("Cannot update unpushed news")
        connection.rollback()

def chinese(update, context):
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
        context.bot.send_message(chat_id=id, text="You are not registered.\nPlease type /st to register.\n你还没有注册，输入/st 即可注册")
        return
    try:
        cursor.execute("UPDATE users set language=? WHERE chat_id = ?", (2, id))
        connection.commit()
        context.bot.send_message(chat_id=id, text="好的，我们开始讲中文吧")
    except Exception as e:
        logger.error(e)
        logger.error("Cannot update unpushed news")
        connection.rollback()

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
updater.dispatcher.add_handler(CommandHandler('vc', view_codes))
updater.dispatcher.add_handler(CommandHandler('vw', view_words))
updater.dispatcher.add_handler(CommandHandler('en', english))
updater.dispatcher.add_handler(CommandHandler('cn', chinese))
updater.start_polling()
updater.idle()
