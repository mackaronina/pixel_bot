import math
import os
import re
import time
import traceback
from io import StringIO, BytesIO
from threading import Thread

import PIL.Image
import numpy as np
import schedule
import telebot
from curl_cffi import requests
from flask import Flask, request
from sqlalchemy import create_engine
from telebot import apihelper

ANONIM = 1087968824
ME = 7258570440
SERVICE_CHATID = -1002171923232
TOKEN = os.environ['BOT_TOKEN']
APP_URL = f'https://pixel-bot-5lns.onrender.com/{TOKEN}'

is_running = False
chunk_info = {}
old_chunks_diff = {}
last_time = 0


class ExHandler(telebot.ExceptionHandler):
    def handle(self, exc):
        sio = StringIO(traceback.format_exc())
        sio.name = 'log.txt'
        sio.seek(0)
        bot.send_document(ME, sio)
        return True


bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=10, parse_mode='HTML', exception_handler=ExHandler())
apihelper.RETRY_ON_ERROR = True
app = Flask(__name__)
bot.remove_webhook()
bot.set_webhook(url=APP_URL, allowed_updates=['message', 'callback_query', 'chat_member', 'message_reaction',
                                              'message_reaction_count'])

cursor = create_engine(
    f'postgresql://postgres.hdahfrunlvoethhwinnc:gT77Av9pQ8IjleU2@aws-0-eu-central-1.pooler.supabase.com:5432/postgres',
    pool_recycle=280)
DB_CHATS = [-1002171923232, -1002037657920]


def get_config_value(key):
    data = cursor.execute(f"SELECT value FROM key_value WHERE key = '{key}'").fetchone()
    if data is None:
        return None
    else:
        return data[0]


def set_config_value(key, value):
    if get_config_value(key) is None:
        cursor.execute(f"INSERT INTO key_value (key, value) VALUES ('{key}', '{value}')")
    else:
        cursor.execute(f"UPDATE key_value SET value = '{value}' WHERE key = '{key}'")
    old_chunks_diff.clear()


def fetch_me(url):
    url = f"{url}/api/me"
    with requests.Session() as session:
        attempts = 0
        while True:
            try:
                resp = session.get(url, impersonate="chrome110")
                data = resp.json()
                canvases = data["canvases"]
                for canvas in canvases.values():
                    if canvas["ident"] == "d":
                        return canvas
                return None
            except:
                if attempts > 5:
                    print(f"Could not get {url} in five tries, cancelling")
                    raise
                attempts += 1
                print(f"Failed to load {url}, trying again in 5s")
                time.sleep(3)
                pass


def fetch(sess, canvas_id, canvasoffset, ix, iy, colors, url, result, img, start_x, start_y, width,
          height):
    url = f"{url}/chunks/{canvas_id}/{ix}/{iy}.bmp"
    attempts = 0
    while True:
        try:
            rsp = sess.get(url, impersonate="chrome110")
            data = rsp.content
            offset = int(-canvasoffset * canvasoffset / 2)
            off_x = ix * 256 + offset
            off_y = iy * 256 + offset
            if len(data) == 0:
                raise Exception("No data")
            else:
                chunk_diff = 0
                for i, b in enumerate(data):
                    tx = off_x + i % 256
                    ty = off_y + i // 256
                    bcl = b & 0x7F

                    if 0 <= bcl < len(colors):
                        map_color = colors[bcl]
                        if not (start_x <= tx < (start_x + width) and start_y <= ty < (
                                start_y + height)):
                            continue
                        x = ty - start_y
                        y = tx - start_x
                        if img[x][y][3] < 255:
                            img[x][y][3] = 0
                            continue
                        if (img[x][y][0], img[x][y][1], img[x][y][2]) not in colors:
                            color = convert_color(img[x][y], colors)
                        else:
                            color = img[x][y]
                        if color[0] != map_color[0] or color[1] != map_color[1] or color[2] != map_color[2]:
                            result["diff"] += 1
                            chunk_diff += 1
                            img[x][y] = [map_color[0], map_color[1], map_color[2], 255]
                        else:
                            img[x][y] = [0, 255, 0, 255]
                        result["total_size"] += 1
                result["chunks_diff"][f"{off_x + 128}_{off_y + 128}"] = chunk_diff
            break
        except Exception as e:
            bot.send_message(ME, str(e))
            bot.send_message(ME, str(url))
            if attempts > 5:
                print(f"Could not get {url} in five tries, cancelling")
                result["error"] = True
                return
            attempts += 1
            print(f"Failed to load {url}, trying again in 3s")
            time.sleep(3)


def get_area(canvas_id, canvas_size, start_x, start_y, width, height, colors, url, img):
    global last_time
    result = {
        "error": False,
        "total_size": 0,
        "diff": 0,
        "chunks_diff": {},
        "alert_chunks": []
    }
    canvasoffset = math.pow(canvas_size, 0.5)
    offset = int(-canvasoffset * canvasoffset / 2)
    xc = (start_x - offset) // 256
    wc = (start_x + width - offset) // 256
    yc = (start_y - offset) // 256
    hc = (start_y + height - offset) // 256
    with requests.Session() as session:
        threads = []
        for iy in range(yc, hc + 1):
            for ix in range(xc, wc + 1):
                time.sleep(0.01)
                t = Thread(target=fetch, args=(
                    session, canvas_id, canvasoffset, ix, iy, colors, url, result, img, start_x, start_y, width,
                    height))
                t.start()
                threads.append(t)
        for t in threads:
            t.join()
    if result["error"]:
        raise Exception("Failed to load area")
    if time.time() - last_time > 3600:
        last_time = time.time()
        for k, v in result["chunks_diff"].items():
            if k in old_chunks_diff and v - old_chunks_diff[k] > 1000:
                x = k.split('_')[0]
                y = k.split('_')[1]
                result["alert_chunks"].append(f"{url}/#d,{x},{y},11")
            old_chunks_diff[k] = v
    return result


def convert_color(color, colors):
    dists = []
    for c in colors:
        d = math.sqrt(
            math.pow(int(color[0]) - c[0], 2) + math.pow(int(color[1]) - c[1], 2) + math.pow(int(color[2]) - c[2], 2))
        dists.append(d)
    return colors[dists.index(min(dists))]


def send_pil(im):
    bio = BytesIO()
    im.save(bio, 'PNG')
    bio.name = 'result.png'
    bio.seek(0, 0)
    return bio


def to_fixed(f: float, n=0):
    a, b = str(f).split('.')
    return '{}.{}{}'.format(a, b[:n], '0' * (n - len(b)))


def check_access(message):
    status = bot.get_chat_member(message.chat.id, message.from_user.id).status
    if message.chat.id not in DB_CHATS or (
            status != 'administrator' and status != 'creator' and message.from_user.id != ME and message.from_user.id != ANONIM):
        bot.reply_to(message, "Сосі")
        return False
    return True


def get_pil(fid):
    file_info = bot.get_file(fid)
    downloaded_file = bot.download_file(file_info.file_path)
    bio = BytesIO(downloaded_file)
    bio.name = 'result.png'
    bio.seek(0, 0)
    im = PIL.Image.open(bio)
    return im


def extract_arg(arg):
    return arg.split()[1:]


@bot.message_handler(commands=["map"])
def msg_map(message):
    if not check_access(message):
        return
    bot.reply_to(message, "Зроз, чекай")
    job_hour()


@bot.message_handler(commands=["set_site"])
def msg_site(message):
    if not check_access(message):
        return
    args = extract_arg(message.text)
    if len(args) < 1:
        bot.reply_to(message,
                     "Формат команди: /set_site [сайт]\nЦією командою вказується сайт, з мапою на якому буде порівнюватись шаблон\nПриклади:\n/set_site pixmap.fun\n/set_site pixelplanet.fun\n/set_site pixuniverse.fun")
        return
    bot.reply_to(message, "Перевірка з'єднання з сайтом...")
    try:
        fetch_me(args[0])
    except:
        bot.reply_to(message, "Не вдалось зв'єднатись, сосі")
        return
    set_config_value("URL", args[0])
    bot.reply_to(message, "Ок, все норм")


@bot.message_handler(commands=["set_coords"])
def msg_coords(message):
    if not check_access(message):
        return
    args = extract_arg(message.text)
    if len(args) < 1:
        bot.reply_to(message,
                     "Формат команди: /set_coords [x_y]\nЦією командою вказуються координати шаблону\nПриклади:\n/set_coords 3687_-13342\n/set_coords 7235_-9174\n/set_coords 3515_-13294")
        return
    try:
        x_y = args[0]
        x = int(x_y.split('_')[0])
        y = int(x_y.split('_')[1])
    except:
        bot.reply_to(message, "Координати хуйня, сосі")
        return
    set_config_value("X", x)
    set_config_value("Y", y)
    bot.reply_to(message, "Ок, все норм")


@bot.message_handler(commands=["set_shablon"])
def msg_shablon(message):
    if not check_access(message):
        return
    repl = message.reply_to_message
    if repl is None or repl.document is None:
        bot.reply_to(message,
                     "Формат команди: /set_shablon\nЦією командою необхідно відповісти на повідомлення з файлом шаблону")
        return
    if repl.document.mime_type != 'image/png':
        bot.reply_to(message, "Файл не у форматі png, сосі")
        return
    set_config_value("FILE", repl.document.file_id)
    bot.reply_to(message, "Ок, все норм")


@bot.message_handler(commands=["shablon"])
def msg_shablon_info(message):
    url = get_config_value("URL")
    x = int(get_config_value("X"))
    y = int(get_config_value("Y"))
    file = get_config_value("FILE")
    bot.send_document(message.chat.id, file, caption=f"<code>{x}_{y}</code>\n\n{url}",
                      reply_to_message_id=message.message_id)


@bot.message_handler(func=lambda message: True, content_types=['photo', 'video', 'document', 'text', 'animation'])
def msg_text(message):
    if message.text is not None:
        handle_text(message, message.text)
    elif message.caption is not None:
        handle_text(message, message.caption)


def handle_text(message, txt):
    low = txt.lower()
    if re.search(r'\bсбу\b', low):
        bot.send_sticker(message.chat.id,
                         'CAACAgIAAxkBAAEKWrBlDPH3Ok1hxuoEndURzstMhckAAWYAAm8sAAIZOLlLPx0MDd1u460wBA',
                         reply_to_message_id=message.message_id)


@bot.chat_member_handler()
def msg_chat(upd):
    if upd.new_chat_member.status == "member" and upd.old_chat_member.status == "left":
        bot.send_animation(upd.chat.id,
                           'CgACAgQAAyEFAASBdOsgAAIV-Wc0pgq0nWuUz2g9vOV_U8qwONWbAAK9BQAC3_skU_chjKqyZotRNgQ')


@bot.message_handler(func=lambda message: True, content_types=['photo', 'video', 'document', 'text', 'animation'])
def msg_text(message):
    if message.chat.id == SERVICE_CHATID and message.photo is not None:
        bot.send_message(message.chat.id, str(message.photo[-1].file_id) + ' ' + str(
            message.photo[-1].file_size) + ' ' + bot.get_file_url(message.photo[-1].file_id),
                         reply_to_message_id=message.message_id)
    if message.chat.id == SERVICE_CHATID and message.animation is not None:
        bot.send_message(message.chat.id, str(message.animation.file_id), reply_to_message_id=message.message_id)


@app.route('/' + TOKEN, methods=['POST'])
def get_message():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return 'ok', 200


@app.route('/')
def get_ok():
    return 'ok', 200


def updater():
    print('Поток запущен')
    while True:
        schedule.run_pending()
        time.sleep(1)


def job_hour():
    global is_running
    try:
        if is_running:
            return
        is_running = True
        url = get_config_value("URL")
        x = int(get_config_value("X"))
        y = int(get_config_value("Y"))
        file = get_config_value("FILE")
        img = np.array(get_pil(file), dtype='uint8')
        shablon_w = img.shape[1]
        shablon_h = img.shape[0]
        canvas = fetch_me(url)
        colors = [tuple(color) for color in canvas["colors"]]
        result = get_area(0, canvas["size"], x, y, shablon_w, shablon_h, colors, url, img)
        total_size = result["total_size"]
        diff = result["diff"]
        perc = (total_size - diff) / total_size
        img = PIL.Image.fromarray(img).convert('RGBA')
        img = send_pil(img)
        bot.send_message(ME, 'abba2')
        m = bot.send_document(SERVICE_CHATID, img)
        fil = m.document.file_id
        text = f"На {url} Україна співпадає з шаблоном на {to_fixed(perc * 100, 2)} %\nПікселів не за шаблоном: {diff}"
        text2 = None
        if len(result["alert_chunks"]) > 0:
            text2 = "За цими координатами помічено ворожу активність, бажано задефати:"
            for link in result["alert_chunks"]:
                text2 += f"\n{link}"
        for chatid in DB_CHATS:
            try:
                bot.send_message(chatid, text)
                bot.send_document(chatid, fil,
                                  caption="Зеленим пікселі за шаблоном, іншими кольорами - ні. Використовуй цю мапу щоб знайти пікселі, які потрібно замалювати")
                if text2 is not None:
                    bot.send_message(chatid, text2)
            except:
                pass


    except Exception as e:
        bot.send_message(ME, str(e))
    finally:
        is_running = False


if __name__ == '__main__':
    bot.send_message(ME, "ok")
    schedule.every(56).minutes.do(job_hour)
    thr = Thread(target=updater)
    thr.start()
    app.run(host='0.0.0.0', port=80, threaded=True)
