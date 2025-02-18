import asyncio
import html
import json
import math
import re
import time
import traceback
from datetime import datetime
from io import StringIO, BytesIO
from threading import Thread
from urllib.parse import urlparse, parse_qs

import PIL.Image
import numpy as np
import pycountry
import schedule
import telebot
from bs4 import BeautifulSoup
from curl_cffi import requests
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine
from telebot import apihelper, types
from telegraph import Telegraph

from config import *

is_running = False
old_chunks_diff = {}
top_three = {}
chunks_info = []
blocked_messages = []
processed_messages = []
updated_at = datetime.fromtimestamp(time.time() + 2 * 3600)
telegraph_url = None


class ExHandler(telebot.ExceptionHandler):
    def handle(self, exc):
        sio = StringIO(traceback.format_exc())
        sio.name = 'log.txt'
        sio.seek(0)
        bot.send_document(ME, sio)
        return True


bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=10, parse_mode='HTML', disable_web_page_preview=True,
                      exception_handler=ExHandler())
apihelper.RETRY_ON_ERROR = True
app = Flask(__name__)
CORS(app)
bot.remove_webhook()
bot.set_webhook(url=APP_URL, allowed_updates=['message', 'callback_query', 'chat_member', 'message_reaction',
                                              'message_reaction_count'])

cursor = create_engine(
    f'postgresql://postgres.hdahfrunlvoethhwinnc:gT77Av9pQ8IjleU2@aws-0-eu-central-1.pooler.supabase.com:5432/postgres',
    pool_recycle=280)


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


def get_medal_user(user_id):
    data = cursor.execute(f"SELECT name, medal_list FROM medals WHERE id = {user_id}").fetchone()
    if data is None:
        return None
    else:
        return {
            'name': data[0],
            'medal_list': json.loads(data[1])
        }


def get_medal_users():
    data = cursor.execute(f"SELECT name, medal_list FROM medals").fetchall()
    if data is None:
        return []
    else:
        mas = [{'name': d[0], 'medal_list': json.loads(d[1])} for d in data]
        mas = sorted(mas, key=lambda user: len(user['medal_list']), reverse=True)
        return mas


def update_medal_user(user_id, name, medal_list):
    medal_list = [html.escape(medal, quote=True) for medal in medal_list]
    name = html.escape(name, quote=True)
    cursor.execute(
        f"UPDATE medals SET name = %s, medal_list = %s WHERE id = {user_id}", name,
        json.dumps(medal_list, ensure_ascii=False))


def create_medal_user(user_id, name, medal_list):
    medal_list = [html.escape(medal, quote=True) for medal in medal_list]
    name = html.escape(name, quote=True)
    cursor.execute(
        f"INSERT INTO medals (id, name, medal_list) VALUES ({user_id}, %s, %s)", name,
        json.dumps(medal_list, ensure_ascii=False))


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
    Blend = 0.9
    R = R1 + (R2 - R1) * Blend
    G = G1 + (G2 - G1) * Blend
    B = B1 + (B2 - B1) * Blend
    return np.array([R, G, B], dtype=np.uint8)


def link(canvas_char, url, x, y, zoom):
    return f'<a href="https://{url}/#{canvas_char},{x},{y},{zoom}">{x},{y}</a>'


async def fetch_via_proxy(url):
    async with requests.AsyncSession() as session:
        endpoint = url.split('pixelplanet.fun')[1]
        l = "https://proxypal.net"
        resp = await session.get(l, impersonate="chrome110")
        soup = BeautifulSoup(resp.text, 'lxml')
        token = soup.find('input', {'name': '_token'})['value']
        l = "https://proxypal.net/proxy"
        resp = await session.post(l, data={'_token': token, 'url': f'http://pixelplanet.fun{endpoint}'},
                                  impersonate="chrome110")
        r = parse_qs(urlparse(resp.url).query)['r'][0]
        cpo = r[:30][:-1] + 'g'
        if '&' in endpoint:
            l = f"https://azureserv.com{endpoint}&__cpo={cpo}"
        else:
            l = f"https://azureserv.com{endpoint}?__cpo={cpo}"
        resp = await session.get(l, impersonate="chrome110")
        return resp


def fetch_me(url, canvas_char="d"):
    url = f"http://{url}/api/me"
    data = None
    with requests.Session() as session:
        for attempts in range(5):
            try:
                if 'pixelplanet' in url:
                    resp = asyncio.run(fetch_via_proxy(url))
                else:
                    resp = session.get(url, impersonate="chrome110")
                data = resp.json()
                break
            except Exception as e:
                bot.send_message(ME, str(e))
                time.sleep(1)
        if data is None:
            raise Exception("Failed to fetch canvas")
        canvases = data["canvases"]
        channel_id = list(data["channels"].keys())[0]
        for key, canvas in canvases.items():
            if canvas["ident"] == canvas_char:
                canvas["id"] = key
                return canvas, channel_id
        raise Exception("Canvas not found")


def fetch_ranking(url):
    url = f"http://{url}/ranking"
    with requests.Session() as session:
        for attempts in range(5):
            try:
                if 'pixelplanet' in url:
                    resp = asyncio.run(fetch_via_proxy(url))
                else:
                    resp = session.get(url, impersonate="chrome110")
                data = resp.json()
                if "pixelya" in url:
                    return data["dailyCorRanking"]
                else:
                    return data["dailyCRanking"]
            except:
                time.sleep(1)
        raise Exception("Rankings failed")


def fetch_channel(url, channel_id):
    url = f"http://{url}/api/chathistory?cid={channel_id}&limit=50"
    with requests.Session() as session:
        for attempts in range(5):
            try:
                if 'pixelplanet' in url:
                    resp = asyncio.run(fetch_via_proxy(url))
                else:
                    resp = session.get(url, impersonate="chrome110")
                data = resp.json()
                return data["history"]
            except:
                time.sleep(1)
        raise Exception("Chat history failed")


async def fetch(sess, canvas_id, canvasoffset, ix, iy, colors, base_url, result, img, start_x, start_y, width,
                height, new_colors, canvas_char):
    url = f"http://{base_url}/chunks/{canvas_id}/{ix}/{iy}.bmp"
    for attempts in range(5):
        try:
            if 'pixelplanet' in url:
                rsp = await fetch_via_proxy(url)
            else:
                rsp = await sess.get(url, impersonate="chrome110")
            if rsp.status_code != 200:
                raise Exception("No data")
            data = rsp.content
            offset = int(-canvasoffset * canvasoffset / 2)
            off_x = ix * 256 + offset
            off_y = iy * 256 + offset
            chunk_diff = 0
            chunk_size = 0
            chunk_pixel_link = None
            chunk_pixel_point = None
            while len(data) < 65536:
                data += bytes((0,))
            for i, b in enumerate(data):
                tx = off_x + i % 256
                ty = off_y + i // 256
                bcl = b & 0x7F
                if not (start_x <= tx < (start_x + width)) or not (start_y <= ty < (start_y + height)):
                    continue
                x = ty - start_y
                y = tx - start_x
                color = img[x, y]
                if color[0] == 1:
                    continue
                map_color = colors[bcl]
                if color[0] != map_color[0] or color[1] != map_color[1] or color[2] != map_color[2]:
                    if chunk_diff == 0:
                        chunk_pixel_link = link(canvas_char, base_url, tx, ty, 25)
                        chunk_pixel_point = f"{tx}_{ty}"
                    chunk_diff += 1
                    img[x, y] = map_color
                else:
                    img[x, y] = new_colors[bcl]
                chunk_size += 1

            if chunk_diff > 10000:
                chunk_pixel_link = link(canvas_char, base_url, off_x + 128, off_y + 128, 10)
                chunk_pixel_point = f"{off_x + 128}_{off_y + 128}"
            result["diff"] += chunk_diff
            result["total_size"] += chunk_size
            chunks_info.append({
                "key": f"{off_x}_{off_y}",
                "diff": chunk_diff,
                "pixel_link": chunk_pixel_link,
                "pixel_point": chunk_pixel_point,
                "change": 0
            })
            return
        except:
            await asyncio.sleep(1)
    result["error"] = True


async def get_area(canvas_id, canvas_size, start_x, start_y, width, height, colors, url, img, new_colors, canvas_char):
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
    async with requests.AsyncSession() as session:
        threads = []
        for iy in range(yc, hc + 1):
            for ix in range(xc, wc + 1):
                threads.append(
                    fetch(session, canvas_id, canvasoffset, ix, iy, colors, url, result, img, start_x, start_y, width,
                          height, new_colors, canvas_char))
        await asyncio.gather(*threads)
    if result["error"]:
        raise Exception("Failed to load area")
    for chunk in chunks_info:
        if chunk["key"] in old_chunks_diff:
            chunk["change"] = chunk["diff"] - old_chunks_diff[chunk["key"]]
            result["change"] += chunk["change"]
        old_chunks_diff[chunk["key"]] = chunk["diff"]
    return result


async def get_area_small(canvas_id, canvas_size, start_x, start_y, width, height, colors, url):
    canvasoffset = math.pow(canvas_size, 0.5)
    offset = int(-canvasoffset * canvasoffset / 2)
    xc = (start_x - offset) // 256
    wc = (start_x + width - offset) // 256
    yc = (start_y - offset) // 256
    hc = (start_y + height - offset) // 256
    img = np.zeros((height, width, 3), dtype=np.uint8)
    async with requests.AsyncSession() as session:
        threads = []
        for iy in range(yc, hc + 1):
            for ix in range(xc, wc + 1):
                threads.append(
                    fetch_small(session, canvas_id, canvasoffset, ix, iy, colors, url, img, start_x, start_y, width,
                                height))
        await asyncio.gather(*threads)
    return img


async def fetch_small(sess, canvas_id, canvasoffset, ix, iy, colors, base_url, img, start_x, start_y, width,
                      height):
    url = f"http://{base_url}/chunks/{canvas_id}/{ix}/{iy}.bmp"
    for attempts in range(5):
        try:
            if 'pixelplanet' in url:
                rsp = await fetch_via_proxy(url)
            else:
                rsp = await sess.get(url, impersonate="chrome110")
            if rsp.status_code != 200:
                raise Exception("No data")
            data = rsp.content
            offset = int(-canvasoffset * canvasoffset / 2)
            off_x = ix * 256 + offset
            off_y = iy * 256 + offset
            while len(data) < 65536:
                data += bytes((0,))
            for i, b in enumerate(data):
                tx = off_x + i % 256
                ty = off_y + i // 256
                bcl = b & 0x7F
                if not (start_x <= tx < (start_x + width)) or not (start_y <= ty < (start_y + height)):
                    continue
                x = ty - start_y
                y = tx - start_x
                img[x, y] = colors[bcl]
            return
        except Exception as e:
            bot.send_message(ME, str(e))
            await asyncio.sleep(1)
    raise Exception("Failed to fetch small area")


def convert_color(color, colors, transparent_color):
    if color[3] < 255:
        return transparent_color
    if check_in(color, colors):
        return color
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
    if message.chat.id == ME:
        return True
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


def extract_text(arg):
    return ' '.join(arg.split()[1:])


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


def to_matrix(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]


def generate_telegraph():
    text = "<p><h4>Сортування за кількістю пікселів:</h4>"
    text += generate_coords_text_telegraph("diff")
    text += "<h4>Сортування за зміною пікселів:</h4>"
    text += generate_coords_text_telegraph("change")
    text += "</p>"
    telegraph = Telegraph()
    telegraph.create_account(short_name='Svinka')
    for attempts in range(5):
        try:
            response = telegraph.create_page(
                'Список всіх координат',
                html_content=text
            )
            return response['url']
        except:
            time.sleep(1)


def generate_keyboard(sort_type, idk):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    callback_button = types.InlineKeyboardButton(text='Сортування', callback_data=f'sort {idk} {sort_type}')
    if telegraph_url is not None:
        callback_button2 = types.InlineKeyboardButton(text='Всі точки', url=telegraph_url)
        keyboard.add(callback_button, callback_button2)
    else:
        keyboard.add(callback_button)
    return keyboard


def generate_coords_text(sort_by):
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
            if not is_running:
                text = f"Дані оновлено о {format_time(updated_at.hour)}:{format_time(updated_at.minute)}"
            else:
                text = f"Дані в процесі оновлення"
            text += "\nЗа цими координатами знайдено пікселі не по шаблону:\n\n№ | Координати | Пікселі | Зміна"
            for i, chunk in enumerate(sorted_chunks):
                if i == 20:
                    break
                text += f"\n{i + 1}.  {chunk['pixel_link']}  {chunk['diff']}  {format_change(chunk['change'])}"
            if len(sorted_chunks) - 20 > 0:
                text += f"\n\nНе показано точок: {len(sorted_chunks) - 20}"
    return text, is_empty


def generate_coords_text_telegraph(sort_by):
    if len(chunks_info) == 0:
        text = "Нічого не знайдено, сосі<br>"
    else:
        sorted_chunks = sorted(chunks_info, key=lambda chunk: chunk[sort_by], reverse=True)
        sorted_chunks = [chunk for chunk in sorted_chunks if chunk["diff"] > 0]
        if len(sorted_chunks) == 0:
            text = "Нічого не знайдено, сосі<br>"
        else:
            text = "№ | Координати | Пікселі | Зміна<br>"
            for i, chunk in enumerate(sorted_chunks):
                text += f"{i + 1}.  {chunk['pixel_link']}  {chunk['diff']}  {format_change(chunk['change'])}<br>"
    return text


@bot.message_handler(commands=["mtop", "medal_top"])
def msg_top(message):
    data = get_medal_users()
    text = 'Ці живчики мають найбільше медалей:\n\n'
    for i, user in enumerate(data):
        if i == 10 or len(user['medal_list']) < 1:
            break
        if i == 0:
            text += f"🏆 <b>{user['name']}</b>  {len(user['medal_list'])} 🎖\n"
        else:
            text += f"{i + 1}.  {user['name']}  {len(user['medal_list'])} 🎖\n"
    bot.reply_to(message, text)


def calc_medals(medal_list):
    res = {}
    data = get_medal_users()
    # count_users = len([user for user in data if len(user['medal_list']) > 0])
    all_medals = []
    for user in data:
        all_medals += [m.lower() for m in user['medal_list']]
    for medal in medal_list:
        koef = all_medals.count(medal.lower())
        if koef <= 2:
            icon = '🥇'
        elif koef <= 7:
            icon = '🥈'
        else:
            icon = '🥉'
        res[medal] = icon
    return res


@bot.message_handler(commands=["minfo", "medal_info"])
def msg_medal(message):
    if message.reply_to_message is None or message.reply_to_message.from_user.id < 0:
        user_id = message.from_user.id
    else:
        user_id = message.reply_to_message.from_user.id
    user = get_medal_user(user_id)
    if user is None or len(user['medal_list']) < 1:
        if user_id == message.from_user.id:
            text = "Ти лох без медалей\n\n<i>Цією командою можна відповісти на чиєсь повідомлення щоб подивитись чужі медалі</i>"
        else:
            text = "У цього лоха нема медалей"
    else:
        text = f"<b>Всього медалей у {user['name']}:  {len(user['medal_list'])} 🎖</b>\n\n"
        for medal, icon in calc_medals(user['medal_list']).items():
            text += f'{icon}  {medal}\n'
    bot.reply_to(message, text)


@bot.message_handler(commands=["mplus", "medal_plus"])
def msg_medal_plus(message):
    if not check_access(message):
        return
    medal_name = extract_text(message.text)
    if len(medal_name) < 1 or message.reply_to_message is None or message.reply_to_message.from_user.id < 0:
        bot.reply_to(message,
                     "Формат команди: /mplus [назва медалі]\nЦією командою можна видати медаль відповіддю на повідомлення людини, яка цю медаль отримає\nПриклади:\n/mplus За взяття хуя за щоку")
        return
    user_id = message.reply_to_message.from_user.id
    user = get_medal_user(user_id)
    if user is None:
        create_medal_user(user_id, message.reply_to_message.from_user.full_name, [medal_name])
    else:
        if medal_name.lower() in [m.lower() for m in user['medal_list']]:
            bot.reply_to(message, 'У нього вже є така медаль, сосі')
            return
        user['medal_list'].append(medal_name)
        update_medal_user(user_id, message.reply_to_message.from_user.full_name, user['medal_list'])
    bot.reply_to(message, "Медаль видано")


@bot.message_handler(commands=["mminus", "medal_minus"])
def msg_medal_minus(message):
    if not check_access(message):
        return
    medal_name = extract_text(message.text)
    if len(medal_name) < 1 or message.reply_to_message is None or message.reply_to_message.from_user.id < 0:
        bot.reply_to(message,
                     "Формат команди: /mminus [назва медалі]\nЦією командою можна забрати медаль відповіддю на повідомлення людини, у якої ця медаль є\nПриклади:\n/mminus За взяття хуя за щоку")
        return
    user_id = message.reply_to_message.from_user.id
    user = get_medal_user(user_id)
    if user is None:
        bot.reply_to(message, 'У нього нема медалей, сосі')
        return
    new_list = [m.lower() for m in user['medal_list']]
    new_name = medal_name.lower()
    if new_name not in new_list:
        bot.reply_to(message, 'У нього нема такої медалі, сосі')
        return
    del user['medal_list'][new_list.index(new_name)]
    update_medal_user(user_id, message.reply_to_message.from_user.full_name, user['medal_list'])
    bot.reply_to(message, "Медаль забрано")


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
        bot.reply_to(message, "Не вдалось за'єднатись, сосі")
        return
    set_config_value("URL", args[0])
    set_config_value("CROPPED", False)
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


@bot.message_handler(commands=["set_canvas"])
def msg_canvas(message):
    if not check_access(message):
        return
    args = extract_arg(message.text)
    if len(args) < 1:
        bot.reply_to(message,
                     "Формат команди: /set_canvas [буква]\nЦією командою вказується полотно на сайті. Букву, яка відповідає якомусь полотну, можна знайти в посиланні на це полотно. Наприклад, d - земля, m - місяць, b - мінімапа")
        return
    char = args[0].lower()
    if not re.search(r'\b\w\b', char):
        bot.reply_to(message, "Хуйню якусь написав, сосі")
        return
    set_config_value("CANVAS", char)
    set_config_value("CROPPED", False)
    bot.reply_to(message, "Ок, все норм")


@bot.message_handler(commands=["void_on"])
def void_on(message):
    ping_users = json.loads(get_config_value("PING_USERS"))
    if message.from_user.id in ping_users:
        bot.reply_to(message, "Ти і так пінгуєшся, сосі")
        return
    ping_users.append(message.from_user.id)
    set_config_value("PING_USERS", json.dumps(ping_users))
    bot.reply_to(message, "Ти тепер пінгуєшся під час зниженого кд")


@bot.message_handler(commands=["void_off"])
def void_off(message):
    ping_users = json.loads(get_config_value("PING_USERS"))
    if message.from_user.id not in ping_users:
        bot.reply_to(message, "Ти і так не пінгуєшся, сосі")
        return
    ping_users.remove(message.from_user.id)
    set_config_value("PING_USERS", json.dumps(ping_users))
    bot.reply_to(message, "Ти більше не пінгуєшся під час зниженого кд")


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
    if message.chat.id not in DB_CHATS:
        return
    if message.text is not None:
        handle_text(message, message.text)
    elif message.caption is not None:
        handle_text(message, message.caption)


def get_area_image(center_x, center_y, site, canvas_char):
    x = center_x - 200
    y = center_y - 150
    canvas, _ = fetch_me(site, canvas_char)
    colors = [np.array([color[0], color[1], color[2]], dtype=np.uint8) for color in canvas["colors"]]
    img = asyncio.run(get_area_small(canvas["id"], canvas["size"], x, y, 400, 300, colors, site))
    img = PIL.Image.fromarray(img)
    return img


def handle_text(message, txt):
    low = txt.lower()
    search_res = re.search(r'\w+\.fun/#\w,[-+]?[0-9]+,[-+]?[0-9]+,[-+]?[0-9]+', low)
    if re.search(r'\bсбу\b', low):
        bot.send_sticker(message.chat.id,
                         'CAACAgIAAxkBAAEKWrBlDPH3Ok1hxuoEndURzstMhckAAWYAAm8sAAIZOLlLPx0MDd1u460wBA',
                         reply_to_message_id=message.message_id)
    elif search_res is not None and message.photo is None:
        parselink = parse_pixel_url(search_res[0])
        if parselink is None:
            return
        img = get_area_image(parselink['x'], parselink['y'], parselink['site'], parselink['canvas'])
        for attempts in range(5):
            try:
                bot.send_photo(message.chat.id, send_pil(img), reply_to_message_id=message.message_id)
                return
            except:
                time.sleep(1)
        raise Exception("Failed to send photo")


@bot.chat_member_handler()
def msg_chat(upd):
    if upd.new_chat_member.status == "member" and upd.old_chat_member.status == "left":
        bot.send_animation(upd.chat.id,
                           'CgACAgQAAyEFAASBdOsgAAIV-Wc0pgq0nWuUz2g9vOV_U8qwONWbAAK9BQAC3_skU_chjKqyZotRNgQ')


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


@app.route('/shablon_picture')
def get_shablon_pictrue():
    file = get_config_value("FILE")
    file_info = bot.get_file(file)
    downloaded_file = bot.download_file(file_info.file_path)
    bio = BytesIO(downloaded_file)
    bio.name = f'result.png'
    bio.seek(0, 0)
    return send_file(bio, mimetype='image/png', as_attachment=True, download_name=bio.name)


def pin_to_html():
    msg = bot.get_chat(MAIN_CHATID).pinned_message
    if msg is not None and msg.text is not None:
        text = msg.html_text
    elif msg is not None and msg.caption is not None:
        text = msg.html_caption
    else:
        return ''
    regex = r"""(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"""
    urls = [x[0] for x in re.findall(regex, text)]
    for url in urls:
        pos = text.find(url)
        if pos != -1 and text[pos - 1] != '"':
            text = text.replace(url, f'<a href="{url}">{url}</a>')
    return text


def parse_pixel_url(url):
    try:
        urlparsed = urlparse(url, allow_fragments=True)
        if len(urlparsed.netloc) != 0:
            site = urlparsed.netloc
        else:
            site = urlparsed.path.replace('/', '')
        if '.fun' in site and len(urlparsed.fragment) != 0:
            canvas = urlparsed.fragment.split(',')[0]
            x = int(urlparsed.fragment.split(',')[1])
            y = int(urlparsed.fragment.split(',')[2])
            return {'x': x, 'y': y, 'site': site, 'canvas': canvas}
        else:
            return None
    except:
        return None


def remove_duplicates(lst):
    return list(dict.fromkeys(lst))


def points_from_pin():
    msg = bot.get_chat(MAIN_CHATID).pinned_message
    if msg is not None and msg.text is not None:
        text = msg.html_text
    elif msg is not None and msg.caption is not None:
        text = msg.html_caption
    else:
        return []
    points = []
    regex = r"""(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"""
    urls = [x[0] for x in re.findall(regex, text.lower())]
    for url in remove_duplicates(urls):
        urlparsed = parse_pixel_url(url)
        if urlparsed is not None:
            points.append(urlparsed)
    return points


@app.route('/shablon_info')
def get_shablon_info():
    x = int(get_config_value("X"))
    y = int(get_config_value("Y"))
    pic_hash = get_config_value("FILE")
    points = points_from_pin()
    text = pin_to_html()
    return jsonify({"x": x, "y": y, "text": text, "pic_hash": pic_hash, "points": points})


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
                    country = pycountry.countries.get(alpha_2=first)
                    if country is not None:
                        text += f". Перше місце - {country.flag}"
                    else:
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


def calc_score(chunk, max_change, max_diff, max_combo):
    a = 1
    if max_change > 0 and chunk["change"] > 0:
        a = round((chunk["change"] / max_change) * 100)
    b = 1
    if max_diff > 0 and chunk["diff"] > 0:
        b = round((chunk["diff"] / max_diff) * 100)
    c = 1
    if max_combo > 0 and chunk["combo"] > 0:
        c = round((chunk["combo"] / max_combo) * 100)
    return a * b * c


def get_hot_point():
    if len(chunks_info) == 0:
        return None
    chunks_copy = [chunk.copy() for chunk in chunks_info if chunk["diff"] > 0 and chunk["change"] > 0]
    if len(chunks_copy) == 0:
        return None
    for chunk in chunks_copy:
        chunk["combo"] = 0
        if chunk["key"] in top_three.keys():
            chunk["combo"] = top_three[chunk["key"]] - 1

    max_change = max([chunk["change"] for chunk in chunks_copy])
    max_diff = max([chunk["diff"] for chunk in chunks_copy])
    max_combo = max([chunk["combo"] for chunk in chunks_copy])

    return sorted(chunks_copy, key=lambda chunk: calc_score(chunk, max_change, max_diff, max_combo), reverse=True)[0]


def intersection_rectangles(x1, y1, x2, y2, x3, y3, x4, y4):
    x5 = max(x1, x3)
    y5 = max(y1, y3)
    x6 = min(x2, x4)
    y6 = min(y2, y4)
    if x5 > x6 or y5 > y6:
        return False
    return True


def check_rollback(msg_txt, site, cropped, canvas_char, shablon_x, shablon_y, w, h):
    if not cropped:
        return
    if "rolled back" in msg_txt or "loaded image" in msg_txt:
        result = re.findall(r'\+\*[1234567890-]*\*\+', msg_txt)
        x1 = int(result[0].replace('+', '').replace('*', ''))
        y1 = int(result[1].replace('+', '').replace('*', ''))
        x2 = int(result[2].replace('+', '').replace('*', ''))
        y2 = int(result[3].replace('+', '').replace('*', ''))
    elif "Canvas Cleaner" in msg_txt:
        result = re.findall(r',[1234567890-]*', msg_txt)
        x1 = int(result[0].replace(',', ''))
        y1 = int(result[1].replace(',', ''))
        x2 = int(result[2].replace(',', ''))
        y2 = int(result[3].replace(',', ''))
    else:
        return
    if not intersection_rectangles(x1, y1, x2, y2, shablon_x, shablon_y, shablon_x + w, shablon_y + h):
        return
    rollback_x = int((x1 + x2) / 2)
    rollback_y = int((y1 + y2) / 2)
    text = f'<b>Помічений ролбек</b>\n{link(canvas_char, site, rollback_x, rollback_y, 10)}'
    img = get_area_image(rollback_x, rollback_y, site, canvas_char)
    for attempts in range(5):
        try:
            bot.send_photo(MAIN_CHATID, send_pil(img), caption=text)
            return
        except:
            time.sleep(1)
    raise Exception("Failed to check rollback")


def check_void(msg_txt, canvas_char, url, ping_users):
    if "successfully defeated" not in msg_txt:
        return
    text = f"<b>Почалося знижене кд на {url}, гойда!</b>"
    img = None
    points = points_from_pin()
    chunk = get_hot_point()
    if len(points) > 0 and points[0]['site'] == url:
        point = points[0]
        text += f"\n\nТапаємо по закріпу: {link(point['canvas'], point['site'], point['x'], point['y'], 10)}"
        img = get_area_image(point['x'], point['y'], point['site'], point['canvas'])
    elif chunk is not None:
        text += f"\n\nНайгарячіша точка: {chunk['pixel_link']} ({chunk['diff']} пікселів)"
        img = get_area_image(int(chunk['pixel_point'].split('_')[0]),
                             int(chunk['pixel_point'].split('_')[1]),
                             url, canvas_char)
    text += "\n\nОтримай актуальний шаблон командою /shablon"
    ping_list = to_matrix(ping_users, 5)
    for attempts in range(5):
        try:
            if img is not None:
                m = bot.send_photo(MAIN_CHATID, send_pil(img), caption=text)
            else:
                m = bot.send_message(MAIN_CHATID, text)
            for ping_five in ping_list:
                txt = ''
                for user in ping_five:
                    txt += f'<a href="tg://user?id={user}">ㅤ</a>'
                bot.reply_to(m, txt)
                time.sleep(0.5)
            return
        except:
            time.sleep(1)
    raise Exception("Failed to check void")


def job_minute():
    try:
        while len(processed_messages) > 100:
            processed_messages.pop(0)
        url = get_config_value("URL")
        ping_users = json.loads(get_config_value("PING_USERS"))
        canvas_char = get_config_value("CANVAS")
        cropped = eval(get_config_value("CROPPED"))
        shablon_x = int(get_config_value("X"))
        shablon_y = int(get_config_value("Y"))
        w = int(get_config_value("WIDTH"))
        h = int(get_config_value("HEIGHT"))
        canvas, channel_id = fetch_me(url, canvas_char)
        history = fetch_channel(url, channel_id)
        for msg in history:
            if 'pixelya' in url:
                msg_time = msg[9]
                msg_sender = msg[0]
                msg_txt = msg[2].lower()
            else:
                msg_time = msg[4]
                msg_sender = msg[0]
                msg_txt = msg[1].lower()
            if msg_time in processed_messages or time.time() - msg_time > 120:
                continue
            if msg_sender == "event":
                check_void(msg_txt, canvas_char, url, ping_users)
            elif msg_sender == "info":
                check_rollback(msg_txt, url, cropped, canvas_char, shablon_x, shablon_y, w, h)
            processed_messages.append(msg_time)
    except:
        sio = StringIO(traceback.format_exc())
        sio.name = 'log.txt'
        sio.seek(0)
        bot.send_document(ME, sio)


def shablon_crop():
    cropped = eval(get_config_value("CROPPED"))
    if cropped:
        return
    x = int(get_config_value("X"))
    y = int(get_config_value("Y"))
    file = get_config_value("FILE")
    url = get_config_value("URL")
    canvas_char = get_config_value("CANVAS")
    img = get_pil(file)
    box = img.getbbox()
    img = img.crop(box)
    x += box[0]
    y += box[1]

    img = np.array(img, dtype=np.uint8)
    canvas, _ = fetch_me(url, canvas_char)
    colors = [np.array([color[0], color[1], color[2], 255], dtype=np.uint8) for color in canvas["colors"]]
    transparent_color = np.array([1, 1, 1, 0], dtype=np.uint8)
    img = np.apply_along_axis(lambda pix: convert_color(pix, colors, transparent_color), 2, img)
    pil_img = PIL.Image.fromarray(img)
    del img
    width, height = pil_img.size

    for attempts in range(5):
        try:
            m = bot.send_document(SERVICE_CHATID, send_pil(pil_img))
            fil = m.document.file_id
            set_config_value("X", x)
            set_config_value("Y", y)
            set_config_value("FILE", fil)
            set_config_value("CROPPED", True)
            set_config_value("WIDTH", width)
            set_config_value("HEIGHT", height)
            return
        except:
            time.sleep(1)
    raise Exception("Failed to send file")


def job_hour():
    global is_running, updated_at, telegraph_url, top_three
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
        canvas_char = get_config_value("CANVAS")
        img = np.array(get_pil(file).convert('RGB'), dtype=np.uint8)
        shablon_w = img.shape[1]
        shablon_h = img.shape[0]
        canvas, _ = fetch_me(url, canvas_char)
        colors = [np.array([color[0], color[1], color[2]], dtype=np.uint8) for color in canvas["colors"]]
        new_colors = [new_color(color) for color in colors]
        updated_at = datetime.fromtimestamp(time.time() + 2 * 3600)
        result = asyncio.run(
            get_area(canvas["id"], canvas["size"], x, y, shablon_w, shablon_h, colors, url, img, new_colors,
                     canvas_char))
        total_size = result["total_size"]
        diff = result["diff"]
        change = result["change"]
        perc = (total_size - diff) / total_size
        pil_img = PIL.Image.fromarray(img)
        del img
        bot.send_message(ME, 'abba2')
        fil = None
        for attempts in range(5):
            try:
                m = bot.send_document(SERVICE_CHATID, send_pil(pil_img))
                fil = m.document.file_id
                break
            except:
                time.sleep(1)
        if fil is None:
            raise Exception("Failed to send file")
        text = f"На {url} Україна співпадає з шаблоном на <b>{to_fixed(perc * 100, 2)} %</b>\nПікселів не за шаблоном: <b>{diff}</b>"
        if change != 0:
            text += f" <b>({format_change(change)})</b>"
        text2 = None
        sorted_chunks = sorted(chunks_info, key=lambda chunk: chunk["change"], reverse=True)
        sorted_chunks = [chunk for chunk in sorted_chunks if chunk["change"] > 0]
        new_top_three = {}
        if len(sorted_chunks) > 0:
            text2 = "За цими координатами помічено найбільшу ворожу активність:"
            for i, chunk in enumerate(sorted_chunks):
                if i == 3:
                    break
                key = chunk['key']
                if key in top_three.keys():
                    if top_three[key] == 1:
                        text2 += f"\n❗️{chunk['pixel_link']}  +{chunk['change']}"
                    else:
                        text2 += f"\n‼️{chunk['pixel_link']}  +{chunk['change']}"
                    new_top_three[key] = top_three[key] + 1
                else:
                    text2 += f"\n{chunk['pixel_link']}  +{chunk['change']}"
                    new_top_three[key] = 1
        top_three = new_top_three
        for chatid in DB_CHATS:
            try:
                bot.send_message(chatid, text)
                bot.send_document(chatid, fil,
                                  caption="Зеленим пікселі за шаблоном, іншими кольорами - ні. Використовуй цю мапу щоб знайти пікселі, які потрібно замалювати")
                if text2 is not None:
                    bot.send_message(chatid, text2)
            except:
                pass
        telegraph_url = generate_telegraph()
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
    try:
        requests.post('https://nekocringebot.onrender.com/send_map', impersonate="chrome110")
    except:
        pass
    app.run(host='0.0.0.0', port=80, threaded=True)
