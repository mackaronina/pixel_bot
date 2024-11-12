import math
import os
import time
import traceback
from io import StringIO
from threading import Thread

import PIL.Image
import numpy as np
import schedule
import telebot
from curl_cffi import requests
from flask import Flask, request
from sqlalchemy import create_engine
from telebot import apihelper

ME = 7258570440
SERVICE_CHATID = -1002171923232
token = os.environ['BOT_TOKEN']
APP_URL = f'https://pixel-bot-5lns.onrender.com/{token}'
PPFUN_URL = "https://pixmap.fun"
COLORS = [
    (202, 227, 255),
    (255, 255, 255),
    (255, 255, 255),
    (228, 228, 228),
    (196, 196, 196),
    (136, 136, 136),
    (78, 78, 78),
    (0, 0, 0),
    (244, 179, 174),
    (255, 167, 209),
    (255, 84, 178),
    (255, 101, 101),
    (229, 0, 0),
    (154, 0, 0),
    (254, 164, 96),
    (229, 149, 0),
    (160, 106, 66),
    (96, 64, 40),
    (245, 223, 176),
    (255, 248, 137),
    (229, 217, 0),
    (148, 224, 68),
    (2, 190, 1),
    (104, 131, 56),
    (0, 101, 19),
    (202, 227, 255),
    (0, 211, 221),
    (0, 131, 199),
    (0, 0, 234),
    (25, 25, 115),
    (207, 110, 228),
    (130, 0, 128)
]


class ExHandler(telebot.ExceptionHandler):
    def handle(self, exc):
        sio = StringIO(traceback.format_exc())
        sio.name = 'log.txt'
        sio.seek(0)
        bot.send_document(ME, sio)
        return True


bot = telebot.TeleBot(token, threaded=True, num_threads=10, parse_mode='HTML', exception_handler=ExHandler())
apihelper.RETRY_ON_ERROR = True
app = Flask(__name__)
bot.remove_webhook()
bot.set_webhook(url=APP_URL, allowed_updates=['message', 'callback_query', 'chat_member', 'message_reaction',
                                              'message_reaction_count'])

cursor = create_engine(
    f'postgresql://postgres.hdahfrunlvoethhwinnc:gT77Av9pQ8IjleU2@aws-0-eu-central-1.pooler.supabase.com:5432/postgres',
    pool_recycle=280)
db = []


class Matrix:
    def __init__(self):
        self.start_x = None
        self.start_y = None
        self.width = None
        self.height = None
        self.matrix = {}

    def add_coords(self, x, y, w, h):
        if self.start_x is None or self.start_x > x:
            self.start_x = x
        if self.start_y is None or self.start_y > y:
            self.start_y = y
        end_x_a = x + w
        end_y_a = y + h
        if self.width is None or self.height is None:
            self.width = w
            self.height = h
        else:
            end_x_b = self.start_x + self.width
            end_y_b = self.start_y + self.height
            self.width = max(end_x_b, end_x_a) - self.start_x
            self.height = max(end_y_b, end_y_a) - self.start_y

    def create_image(self):
        img = PIL.Image.new('RGBA', (self.width, self.height), (255, 0, 0, 0))
        pxls = img.load()
        for x in range(self.width):
            for y in range(self.height):
                try:
                    color = self.matrix[x + self.start_x][y + self.start_y]
                    pxls[x, y] = color
                except (IndexError, KeyError, AttributeError):
                    pass
        return img

    def set_pixel(self, x, y, color):
        if self.start_x <= x < (self.start_x + self.width) and self.start_y <= y < (self.start_y + self.height):
            if x not in self.matrix:
                self.matrix[x] = {}
            self.matrix[x][y] = color


def fetch(sess, canvas_id, canvasoffset, ix, iy, target_matrix):
    url = f"{PPFUN_URL}/chunks/{canvas_id}/{ix}/{iy}.bmp"
    attempts = 0
    while True:
        try:
            rsp = sess.get(url, impersonate="chrome110")
            data = rsp.content
            offset = int(-canvasoffset * canvasoffset / 2)
            off_x = ix * 256 + offset
            off_y = iy * 256 + offset
            if len(data) == 0:
                clr = COLORS[0]
                for i in range(256 * 256):
                    tx = off_x + i % 256
                    ty = off_y + i // 256
                    target_matrix.set_pixel(tx, ty, clr)
            else:
                i = 0
                for b in data:
                    tx = off_x + i % 256
                    ty = off_y + i // 256
                    bcl = b & 0x7F

                    if 0 <= bcl <= 31:
                        target_matrix.set_pixel(tx, ty, COLORS[bcl])
                    i += 1
            break
        except Exception as e:
            print(e)
            if attempts > 3:
                print(f"Could not get {url} in three tries, cancelling")
                raise
            attempts += 1
            print(f"Failed to load {url}, trying again in 3s")
            time.sleep(3)
            pass


def get_area(canvas_id, canvas_size, x, y, w, h):
    target_matrix = Matrix()
    target_matrix.add_coords(x, y, w, h)
    canvasoffset = math.pow(canvas_size, 0.5)
    offset = int(-canvasoffset * canvasoffset / 2)
    xc = (x - offset) // 256
    wc = (x + w - offset) // 256
    yc = (y - offset) // 256
    hc = (y + h - offset) // 256
    print(f"Loading from {xc} / {yc} to {wc + 1} / {hc + 1} PixelGetter")
    with requests.Session() as s:
        for iy in range(yc, hc + 1):
            for ix in range(xc, wc + 1):
                fetch(s, canvas_id, canvasoffset, ix, iy, target_matrix)
        return target_matrix


def convert_color(color):
    dists = []
    for c in COLORS:
        d = math.sqrt(math.pow(color[0] - c[0], 2) + math.pow(color[1] - c[1], 2) + math.pow(color[2] - c[2], 2))
        dists.append(d)
    return COLORS[dists.index(min(dists))]


def get_difference():
    img = np.array(PIL.Image.open(r"shablon.png"), dtype='int16')
    shablon_x = 3515
    shablon_y = -13294
    shablon_w = img.shape[1]
    shablon_h = img.shape[0]
    matrix = get_area(0, 65536, shablon_x, shablon_y, shablon_w, shablon_h)
    img1 = np.array(matrix.create_image(), dtype='int16')
    total_size = shablon_w * shablon_h
    diff = 0
    for x in range(shablon_h):
        for y in range(shablon_w):
            if img[x][y][3] == 0:
                continue
            if (img[x][y][0], img[x][y][1], img[x][y][2]) not in COLORS:
                color = convert_color(img[x][y])
            else:
                color = img[x][y]
            if color[0] != img1[x][y][0] or color[1] != img1[x][y][1] or color[2] != img1[x][y][2]:
                diff += 1
    return (total_size - diff) / total_size, diff


def to_fixed(f: float, n=0):
    a, b = str(f).split('.')
    return '{}.{}{}'.format(a, b[:n], '0' * (n - len(b)))


def handle_text(message, txt):
    print('Сообщение получено')
    if message.chat.id not in db:
        db.append(message.chat.id)
        cursor.execute(f'INSERT INTO ukr_chats (id) VALUES ({message.chat.id})')


@bot.message_handler(func=lambda message: True, content_types=['photo', 'video', 'document', 'text', 'animation'])
def msg_text(message):
    if message.chat.id == SERVICE_CHATID and message.photo is not None:
        bot.send_message(message.chat.id, str(message.photo[-1].file_id) + ' ' + str(
            message.photo[-1].file_size) + ' ' + bot.get_file_url(message.photo[-1].file_id),
                         reply_to_message_id=message.message_id)
    if message.chat.id == SERVICE_CHATID and message.animation is not None:
        bot.send_message(message.chat.id, str(message.animation.file_id), reply_to_message_id=message.message_id)
    if message.text is not None:
        handle_text(message, message.text)
    elif message.caption is not None:
        handle_text(message, message.caption)


@app.route('/' + token, methods=['POST'])
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


is_void = False


def job_minutes():
    global is_void
    r = session.get(f'{PPFUN_URL}/void', impersonate='chrome110')
    if r.status_code == 200:
        bot.send_message(ME, 'test')
        if "Time until next void: 0 hours, 0 minutes, 0 seconds" in r.text:
            if not is_void:
                bot.send_message(ME, 'test2')
                is_void = True
                perc, diff = get_difference()
                bot.send_message(ME, 'test3')
                text = f"На пм войд, гойда\n\nУкраїна співпадає з шаблоном на {to_fixed(perc * 100, 2)} %\nПікселів не за шаблоном: {diff}"
                for chatid in db:
                    try:
                        bot.send_message(chatid, text)
                    except:
                        pass
        else:
            is_void = False


def init_db():
    data = cursor.execute('SELECT id FROM ukr_chats')
    data = data.fetchall()
    if data is not None:
        for dat in data:
            db.append(dat[0])
    print(db)


if __name__ == '__main__':
    init_db()
    with requests.Session() as session:
        resp = session.get(f'{PPFUN_URL}/void', impersonate='chrome110')
        bot.send_message(ME, str(resp.status_code))
    schedule.every(3).minutes.do(job_minutes)
    t = Thread(target=updater)
    t.start()
    app.run(host='0.0.0.0', port=80, threaded=True)
