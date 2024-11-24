import math
import os
import re
import time
import traceback
from datetime import datetime
from io import StringIO, BytesIO
from threading import Thread

import PIL.Image
import numpy as np
import schedule
import telebot
from curl_cffi import requests
from flask import Flask, request
from sqlalchemy import create_engine
from telebot import apihelper, types
from telegraph import Telegraph

ANONIM = 1087968824
ME = 7258570440
SERVICE_CHATID = -1002171923232
TOKEN = os.environ['BOT_TOKEN']
APP_URL = f'https://pixel-bot-5lns.onrender.com/{TOKEN}'

is_running = False
old_chunks_diff = {}
chunks_info = []
blocked_messages = []
processed_messages = []
updated_at = datetime.fromtimestamp(time.time() + 2 * 3600)
telegraph_url = None

telegraph = Telegraph()
telegraph.create_account(short_name='Svinka')


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


def answer_callback_query(call, txt, show=False):
    try:
        bot.answer_callback_query(call.id, text=txt, show_alert=show)
    except:
        if show:
            try:
                bot.send_message(call.from_user.id, text=txt)
            except:
                pass


def check_in(array_to_check, list_np_arrays):
    for array in list_np_arrays:
        if array_to_check[0] != array[0] or array_to_check[1] != array[1] or array_to_check[2] != array[2]:
            continue
        else:
            return True
    return False


def new_color(color):
    R1 = int(color[0])
    G1 = int(color[1])
    B1 = int(color[2])
    R2, G2, B2 = (0, 255, 0)
    Blend = 0.95
    R = R1 + (R2 - R1) * Blend
    G = G1 + (G2 - G1) * Blend
    B = B1 + (B2 - B1) * Blend
    return np.array([R, G, B, 255], dtype="uint8")


def link(url, x, y, zoom):
    return f'<a href="https://{url}/#d,{x},{y},{zoom}">{x},{y}</a>'


def fetch_me(url):
    url = f"https://{url}/api/me"
    with requests.Session() as session:
        attempts = 0
        while True:
            try:
                resp = session.get(url, impersonate="chrome110")
                data = resp.json()
                canvases = data["canvases"]
                channel_id = list(data["channels"].keys())[0]
                for canvas in canvases.values():
                    if canvas["ident"] == "d":
                        return canvas, channel_id
                return None
            except:
                if attempts > 5:
                    raise
                attempts += 1
                time.sleep(3)
                pass


def fetch_ranking(url):
    url = f"https://{url}/ranking"
    with requests.Session() as session:
        attempts = 0
        while True:
            try:
                resp = session.get(url, impersonate="chrome110")
                data = resp.json()
                return data["dailyCRanking"]
            except:
                if attempts > 5:
                    raise
                attempts += 1
                time.sleep(3)
                pass


def fetch_channel(url, channel_id):
    url = f"https://{url}/api/chathistory?cid={channel_id}&limit=50"
    with requests.Session() as session:
        attempts = 0
        while True:
            try:
                resp = session.get(url, impersonate="chrome110")
                data = resp.json()
                return data["history"]
            except:
                if attempts > 5:
                    raise
                attempts += 1
                time.sleep(3)
                pass


def fetch(sess, canvas_id, canvasoffset, ix, iy, colors, base_url, result, img, start_x, start_y, width,
          height, new_colors):
    url = f"https://{base_url}/chunks/{canvas_id}/{ix}/{iy}.bmp"
    attempts = 0
    while True:
        try:
            rsp = sess.get(url, impersonate="chrome110")
            data = rsp.content
            offset = int(-canvasoffset * canvasoffset / 2)
            off_x = ix * 256 + offset
            off_y = iy * 256 + offset
            if len(data) != 65536:
                raise Exception("No data")
            else:
                chunk_diff = 0
                chunk_size = 0
                chunk_pixel = None

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
                            continue
                        if not check_in(img[x][y], colors):
                            color = convert_color(img[x][y], colors)
                        else:
                            color = img[x][y]
                        if color[0] != map_color[0] or color[1] != map_color[1] or color[2] != map_color[2]:
                            if chunk_diff == 0:
                                chunk_pixel = link(base_url, tx, ty, 25)
                            chunk_diff += 1
                            img[x][y] = map_color
                        else:
                            img[x][y] = new_colors[bcl]
                        chunk_size += 1

                if chunk_diff > 10000:
                    chunk_pixel = link(base_url, off_x + 128, off_y + 128, 10)
                result["diff"] += chunk_diff
                result["total_size"] += chunk_size
                chunks_info.append({
                    "key": f"{off_x}_{off_y}",
                    "diff": chunk_diff,
                    "pixel_link": chunk_pixel,
                    "change": 0
                })
                break
        except Exception as e:
            bot.send_message(ME, str(e))
            bot.send_message(ME, str(url))
            if attempts > 5:
                result["error"] = True
                return
            attempts += 1
            time.sleep(3)


def get_area(canvas_id, canvas_size, start_x, start_y, width, height, colors, url, img, new_colors):
    chunks_info.clear()
    result = {
        "error": False,
        "total_size": 0,
        "diff": 0,
        "change": 0
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
                    height, new_colors))
                t.start()
                threads.append(t)
        for t in threads:
            t.join()
    if result["error"]:
        raise Exception("Failed to load area")
    for chunk in chunks_info:
        if chunk["key"] in old_chunks_diff:
            chunk["change"] = chunk["diff"] - old_chunks_diff[chunk["key"]]
            result["change"] += chunk["change"]
        old_chunks_diff[chunk["key"]] = chunk["diff"]
    return result


def convert_color(color, colors):
    dists = []
    for c in colors:
        d = math.sqrt(
            (int(color[0]) - int(c[0])) ** 2 + (int(color[1]) - int(c[1])) ** 2 + (int(color[2]) - int(c[2])) ** 2
        )
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


def format_change(a):
    if a > 0:
        return f"+{a}"
    else:
        return str(a)


def format_time(a):
    if a < 10:
        return f"0{a}"
    else:
        return str(a)


def generate_telegraph():
    global telegraph_url
    text = "<p><h4>Сортування за кількістю пікселів:</h4>"
    txt, _ = generate_coords_text("diff", False)
    text += txt
    text += "<h4>Сортування за зміною пікселів:</h4>"
    txt, _ = generate_coords_text("change", False)
    text += txt
    text += "</p>"
    attempts = 0
    while True:
        try:
            response = telegraph.create_page(
                'Список всіх координат',
                html_content=text
            )
            telegraph_url = response['url']
            break
        except Exception as e:
            bot.send_message(ME, str(e))
            if attempts > 5:
                return
            attempts += 1
            time.sleep(3)


def generate_keyboard(sort_type, idk):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    callback_button = types.InlineKeyboardButton(text='Сортування', callback_data=f'sort {idk} {sort_type}')
    if telegraph_url is not None:
        callback_button2 = types.InlineKeyboardButton(text='Всі точки', url=telegraph_url)
        keyboard.add(callback_button, callback_button2)
    else:
        keyboard.add(callback_button)
    return keyboard


def generate_coords_text(sort_by, limit=True):
    is_empty = False
    if len(chunks_info) == 0:
        text = "Нічого не знайдено, сосі"
        is_empty = True
    else:
        sorted_chunks = sorted(chunks_info, key=lambda chunk: chunk[sort_by], reverse=True)
        sorted_chunks = [chunk for chunk in sorted_chunks if chunk["diff"] > 0]
        if len(sorted_chunks) == 0:
            text = "Нічого не знайдено, сосі"
            is_empty = True
        else:
            if limit:
                if not is_running:
                    text = f"Дані оновлено о {format_time(updated_at.hour)}:{format_time(updated_at.minute)}"
                else:
                    text = f"Дані в процесі оновлення"
                text += "\nЗа цими координатами знайдено пікселі не по шаблону:\n\n№ | Координати | Пікселі | Зміна"
            else:
                text = "№ | Координати | Пікселі | Зміна"
            for i, chunk in enumerate(sorted_chunks):
                if i == 20 and limit:
                    break
                if limit:
                    text += f"\n{i + 1}.  {chunk['pixel_link']}  {chunk['diff']}  {format_change(chunk['change'])}"
                else:
                    text += f"<br>{i + 1}.  {chunk['pixel_link']}  {chunk['diff']}  {format_change(chunk['change'])}"
            if len(sorted_chunks) - 20 > 0 and limit:
                text += f"\n\nНе показано точок: {len(sorted_chunks) - 20}"
    return text, is_empty


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
    set_config_value("CROPPED", False)
    bot.reply_to(message, "Ок, все норм")


@bot.message_handler(commands=["shablon"])
def msg_shablon_info(message):
    url = get_config_value("URL")
    x = int(get_config_value("X"))
    y = int(get_config_value("Y"))
    file = get_config_value("FILE")
    bot.send_document(message.chat.id, file, caption=f"<code>{x}_{y}</code>\n\n{url}",
                      reply_to_message_id=message.message_id)


@bot.message_handler(commands=["coords"])
def msg_coords_info(message):
    text, is_empty = generate_coords_text("diff")
    if not is_empty:
        keyboard = generate_keyboard("change", message.from_user.id)
        bot.reply_to(message, text, reply_markup=keyboard)
    else:
        bot.reply_to(message, text)


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


def callback_process(call):
    args = call.data.split()
    cmd = args[0]
    idk = int(args[1])
    if call.from_user.id != idk and idk != ANONIM:
        answer_callback_query(call, "Це повідомлення не для тебе")
        return
    if cmd == "sort":
        type_sort = args[2]
        text, is_empty = generate_coords_text(type_sort)
        answer_callback_query(call, "Ок чекай")
        time.sleep(1)
        if not is_empty:
            if type_sort == "diff":
                new_sort = "change"
            else:
                new_sort = "diff"
            keyboard = generate_keyboard(new_sort, idk)
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=text,
                                  reply_markup=keyboard)
        else:
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=text)


@bot.callback_query_handler(func=lambda call: True)
def callback_get(call):
    key = f'{call.message.chat.id} {call.message.message_id}'
    if key in blocked_messages:
        answer_callback_query(call, "Почекай")
        return
    blocked_messages.append(key)
    try:
        callback_process(call)
    finally:
        blocked_messages.remove(key)


@app.route('/' + TOKEN, methods=['POST'])
def get_message():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return 'ok', 200


@app.route('/')
def get_ok():
    return 'ok', 200


def updater(scheduler):
    while True:
        scheduler.run_pending()
        time.sleep(1)


def job_day():
    try:
        url = get_config_value("URL")
        ranking = fetch_ranking(url)
        first = None
        for i, country in enumerate(ranking):
            if country["cc"] == "ua":
                px = int(country['px'])
                text = f"За цей день хохли потужно натапали <b>{px:,}</b> пікселів і зайняли <b>{i + 1}</b> місце в топі"
                if first is not None:
                    text += f". Перше місце - <b>{first}</b>"
                for chatid in DB_CHATS:
                    try:
                        bot.send_message(chatid, text)
                        bot.send_sticker(chatid,
                                         'CAACAgIAAxkBAAEKWq5lDOyAX1vNodaWsT5amK0vGQe_ggACHCkAAspLuUtESxXfKFwfWTAE')
                    except:
                        pass
                break
            elif i == 0:
                first = country["cc"]
    except Exception as e:
        bot.send_message(ME, str(e))


def job_minute():
    try:
        while len(processed_messages) > 100:
            processed_messages.pop(0)
        url = get_config_value("URL")
        _, channel_id = fetch_me(url)
        history = fetch_channel(url, channel_id)
        for msg in history:
            if msg[4] in processed_messages or time.time() - msg[4] > 180:
                continue
            if msg[0] == "event" and "Threat successfully defeated" in msg[1]:
                text = f"<b>Почалося знижене кд, гойда!</b>"
                for chatid in DB_CHATS:
                    try:
                        bot.send_message(chatid, text)
                    except:
                        pass
            processed_messages.append(msg[4])
    except Exception as e:
        bot.send_message(ME, str(e))


def shablon_crop():
    cropped = eval(get_config_value("CROPPED"))
    if cropped:
        return
    x = int(get_config_value("X"))
    y = int(get_config_value("Y"))
    file = get_config_value("FILE")
    img = get_pil(file)
    box = img.getbbox()
    img = img.crop(box)
    x += box[0]
    y += box[1]
    m = bot.send_document(SERVICE_CHATID, send_pil(img))
    fil = m.document.file_id
    set_config_value("X", x)
    set_config_value("Y", y)
    set_config_value("FILE", fil)
    set_config_value("CROPPED", True)


def job_hour():
    global is_running, updated_at, telegraph_url
    try:
        if is_running:
            return
        is_running = True
        telegraph_url = None
        shablon_crop()
        url = get_config_value("URL")
        x = int(get_config_value("X"))
        y = int(get_config_value("Y"))
        file = get_config_value("FILE")
        img = np.array(get_pil(file), dtype='uint8')
        shablon_w = img.shape[1]
        shablon_h = img.shape[0]
        canvas, _ = fetch_me(url)
        colors = [np.array([color[0], color[1], color[2], 255], dtype="uint8") for color in canvas["colors"]]
        new_colors = [new_color(color) for color in colors]
        updated_at = datetime.fromtimestamp(time.time() + 2 * 3600)
        result = get_area(0, canvas["size"], x, y, shablon_w, shablon_h, colors, url, img, new_colors)
        total_size = result["total_size"]
        diff = result["diff"]
        change = result["change"]
        perc = (total_size - diff) / total_size
        img = PIL.Image.fromarray(img).convert('RGBA')
        img = send_pil(img)
        bot.send_message(ME, 'abba2')
        m = bot.send_document(SERVICE_CHATID, img)
        fil = m.document.file_id
        text = f"На {url} Україна співпадає з шаблоном на <b>{to_fixed(perc * 100, 2)} %</b>\nПікселів не за шаблоном: <b>{diff}</b>"
        if change != 0:
            text += f" <b>({format_change(change)})</b>"
        text2 = None
        sorted_chunks = sorted(chunks_info, key=lambda chunk: chunk["change"], reverse=True)
        if sorted_chunks[0]["change"] > 0:
            text2 = "За цими координатами помічено найбільшу ворожу активність:"
            for i, chunk in enumerate(sorted_chunks):
                if i == 3 or chunk["change"] <= 0:
                    break
                text2 += f"\n{chunk['pixel_link']}  +{chunk['change']}"
        for chatid in DB_CHATS:
            try:
                bot.send_message(chatid, text)
                bot.send_document(chatid, fil,
                                  caption="Зеленим пікселі за шаблоном, іншими кольорами - ні. Використовуй цю мапу щоб знайти пікселі, які потрібно замалювати")
                if text2 is not None:
                    bot.send_message(chatid, text2)
            except:
                pass
        generate_telegraph()
    except Exception as e:
        bot.send_message(ME, str(e))
    finally:
        is_running = False


if __name__ == '__main__':
    bot.send_message(ME, "ok")
    scheduler1 = schedule.Scheduler()
    scheduler1.every(60).minutes.do(job_hour)
    scheduler2 = schedule.Scheduler()
    scheduler2.every().day.at("23:00").do(job_day)
    scheduler3 = schedule.Scheduler()
    scheduler3.every(1).minutes.do(job_minute)
    Thread(target=updater, args=(scheduler1,)).start()
    Thread(target=updater, args=(scheduler2,)).start()
    Thread(target=updater, args=(scheduler3,)).start()
    app.run(host='0.0.0.0', port=80, threaded=True)
