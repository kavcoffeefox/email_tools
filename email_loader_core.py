# -*- coding: utf-8 -*-
import imaplib
import string
import email
import email.message
import mimetypes
import logging
import traceback
import datetime
import os
import sqlite3
import argparse
from configparser import ConfigParser

CONFIG_FILE = "config.ini"
DEFAULT_DATE_FORMAT = "%d.%m.%Y"
DATE_FORMAT_FOR_IMAP = "%d-%b-%Y"
PATH_FOR_DB = "mydb.sqllite"


class ParamNotSet:
    pass


class MsgLoader:
    """
        Класс управляющий скачиванием писем с ящика
    """

    def __init__(self):
        self.mail = ""
        self.msgParser = MsgParser()
        self._downloaded_msg = []
        self.db = ManagerStatDB()
        self.db.create_connection(PATH_FOR_DB)
        self.login = ""

    def connect_mailbox(self, login: str, password: str, mailbox="inbox"):
        self.login = login
        mailserver = login.split('@')[1]
        if login.split("@")[1] in ["bk.ru", "inbox.ru"]:
            mailserver = "mail.ru"
        try:
            self.mail = imaplib.IMAP4_SSL('imap.' + mailserver)
            status, data = self.mail.login(login, password)
            logger.info("Подключение к ящику {mailbox} успешно".format(mailbox=login))
            print("Начинаем скачивать письма с {}".format(login))
            self.set_mailbox(mailbox)
            return True
        except:
            logger.info("Подключение к ящику {mailbox} не удалось".format(mailbox=login))
            return False

    def set_mailbox(self, mailbox):
        status, mess = self.mail.select(mailbox)
        if status != "OK":
               logger.info("Не удалось выбрать папку {}: {}".format(mailbox, mess))
        else:
            logger.info("Выбрана папка {}".format(mailbox))

    def get_msg_by_date_interval(self, p_startdate, p_enddate=datetime.datetime.today()):
        if p_startdate > p_enddate:
            logger.info("Дата начала периода больше даты конца! Работа будет продолжена, но даты будут поменяны местами!")
            p_startdate, p_enddate = p_enddate, p_startdate
        status, data = self.mail.uid('search', None, "ALL")
        list_msg = []
        mailParser = MsgParser()
        if status == "OK":
            for uid_msg in data[0].split()[::-1]:
                msg = self.get_msg_by_uid(uid_msg)
                date_msg = datetime.datetime.strptime(create_date(mailParser.get_msg_date(msg)), "%a, %d %b %Y %H:%M:%S")
                if date_msg > p_enddate:
                    continue
                if date_msg >= p_startdate:
                    list_msg.append((uid_msg, self.login, msg))
                    self.db.add_downloaded_msg(self.login, uid_msg, date_msg)
                else:
                    break
            return list_msg

    def get_msg_for_uid_list(self, p_uid_list):
        """
              Скачивание писем по имеющимся uid
        """
        temp_list_msg = []
        status, data = self.mail.uid('search', None, "ALL")
        mail_uids = data[0].split()
        if status == "OK":
            for uid in p_uid_list:
                if uid in mail_uids:
                    temp_list_msg.append(self.get_msg_by_uid(uid))
        return temp_list_msg

    def get_all_msg(self, count=ParamNotSet()):
        temp_list_msg = []
        status, data = self.mail.uid('search', None, "ALL")
        if isinstance(count, ParamNotSet):
            for uid in data[0].split():
                temp_msg = self.get_msg_by_uid(uid)
                temp_list_msg.append((uid, self.login, temp_msg))
        else:
            for uid in data[0].split()[:-1][0:int(count)]:
                temp_msg = self.get_msg_by_uid(uid)
                temp_list_msg.append((uid, self.login, temp_msg))
        return temp_list_msg


    def get_msg_for_last_uid(self, start_uid):
        status, data = self.mail.uid('search', None, "ALL")
        return self.get_msg_for_uid_interval(start_uid, data[0].split()[-1])

    def get_msg_for_uid_interval(self, start_uid=ParamNotSet(), end_uid=ParamNotSet()):
        """
        Поиск писем по uid соответствующим заданному интервалу
        """
        temp_list_mag = []
        status, data = self.mail.uid('search', None, "ALL")
        if status == "OK":
            if not isinstance(start_uid, ParamNotSet) and not isinstance(end_uid, ParamNotSet):
                pass
            elif not isinstance(start_uid, ParamNotSet) and isinstance(end_uid, ParamNotSet):
                pass
            elif isinstance(start_uid, ParamNotSet) and not isinstance(end_uid, ParamNotSet):
                pass
            else:
                pass
            for uid in data[0].split():
                if int(start_uid) <= int(uid) <= int(end_uid):
                    temp_msg = self.get_msg_by_uid(uid)
                    temp_list_mag.append((uid, self.login, temp_msg))
                    self.db.add_downloaded_msg(self.login, uid, self.msgParser.get_msg_date(temp_msg))
        return temp_list_mag

    def get_msg_from_period(self, startdate: datetime, enddate=datetime.date.today(), downloaddir="inbox"):
        list_msg = []
        self.mail.select(downloaddir)
        while startdate.strftime("%d-%b-%Y") <= enddate.strftime("%d-%b-%Y"):
            status, msgs = self.get_all_msg_by_date(startdate)
            if status:
                list_msg = list_msg + msgs
            startdate = (startdate + datetime.timedelta(1))
        self._downloaded_msg = list_msg
        return list_msg

    def get_all_msg_by_date(self, date, downloaddir="inbox"):
        msg_list = []
        self.mail.select(downloaddir)
        result, data = self.mail.uid('search', None, '(ON {date})'.format(date=date.strftime("%d-%b-%Y")))
        if result == "OK":
            for uid_msg in data[0].split():
                result, msg = self.get_msg_by_uid(uid_msg)
                if result:
                    msg_list.append(msg)  # Тело письма в необработанном виде
                #     logger.debug("Письмо с uid {uid} - успешно скачано".format(uid=uid_msg))
                # else:
                #     logger.error("Письмо с uid {uid} - скачать не удалось!!!".format(uid=uid_msg))
            return True, msg_list
        else:
            logger.info("Не удалось найти письма за {date} ".format(date=date))
            return False, msg_list

    def get_msg_by_uid(self, uid):
        print("Начали качать письмо " + str(uid))
        result, data = self.mail.uid('fetch', uid, '(RFC822)')  # Получаем тело письма (RFC822) для данного ID
        print("Закончили качать письмо " + str(uid))
        if result == "OK":
            return data[0][1]  # Тело письма в необработанном виде
        else:
            logger.error("Не удалось прочитать письмо с заданным uid! uid: {}  result: {}".format(uid, result))
            return False


class MsgParser:
    """
        Данный класс парсит письма, приводит их в более управляемы и читаемый вид
    """
    def __init__(self):
        self.header = ""
        self.sender = ""
        self.db = ManagerStatDB()
        self.db.create_connection(PATH_FOR_DB)

    def read_msg(self, p_msg):
        try:
            email_message = email.message_from_string(p_msg)
        except TypeError:
            email_message = email.message_from_bytes(p_msg)
        return email_message

    def get_msg_date(self, p_msg):
        """
        TODO: обработка ошибок на неудачу при разборе кодировки заголовка
        """
        email_msg = self.read_msg(p_msg)
        return str(email.header.make_header(email.header.decode_header(email_msg['Date'])))

    def save_msg(self, p_msg, path=""):
        if not os.path.exists(path):
            os.makedirs(path)
        name_msg = "uid_"+str(int(p_msg[0])) + "_" + str(p_msg[1])
        with open(os.path.join(path, name_msg), "wb") as f:
            f.write(p_msg[2])


    def save_msg_payload(self, p_msg, path="", dirname="temp"):
        uid_msg, login, msg = p_msg
        try:
            email_message = email.message_from_string(msg)
        except TypeError:
            email_message = email.message_from_bytes(msg)

        # logger.info("{}".format(
        #     "--- нашли письмо от: " + str(email.header.make_header(email.header.decode_header(email_message['From'])),
        #     errors='replace')))
        path = os.path.join(path, dirname)
        if not os.path.exists(path):
            os.makedirs(path)
        for part in email_message.walk():
            filename = part.get_filename()
            if filename is not None:
                filename = str(email.header.make_header(email.header.decode_header(filename)))
            if not filename:
                continue
            save_path = os.path.join(path, filename)
            # self.logger.info("------  нашли вложение {}".format(filename))
            try:
                with open(save_path, 'wb') as fp:
                    fp.write(part.get_payload(decode=1))
                self.db.add_downloaded_payload(name=filename, size=os.path.getsize(save_path),
                                               last_path=save_path, login=login, msg_uid=uid_msg)
                logger.info("------ Сохранение вложения \"{}\" завершено".format(filename))
            except FileExistsError as e:
                logger.error("Ошибка при создании файла приложения: {}".format(e))


class ManagerStatDB:
    """
        Класс обеспечивающий работу с базой данных для статистики
    """
    def __init__(self):
        self.connection = None

    def create_tables(self):
        create_downloaded_msg_table = """
            CREATE TABLE IF NOT EXISTS downloaded_msg (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              login TEXT NOT NULL, 
              uid TEXT NOT NULL, 
              date DATETIME NOT NULL
            );
            """
        self.execute_query(create_downloaded_msg_table)

        create_downloaded_payloads_table = """
        CREATE TABLE IF NOT EXISTS downloaded_payloads (
          id INTEGER PRIMARY KEY AUTOINCREMENT, 
          name TEXT NOT NULL, 
          size INTEGER, 
          last_path TEXT NOT NULL, 
          login TEXT NOT NULL,
          msg_uid TEXT NOT NULL
        );
        """
        self.execute_query(create_downloaded_payloads_table)

    def get_last_uid(self, login: str):
        get_last_uid_query = """
        SELECT *
        FROM downloaded_msg
        WHERE date=(SELECT MAX(date) FROM downloaded_msg WHERE login='{login}') 
        """.format(login=login)
        return self.execute_read_query(get_last_uid_query)[0][2]

    def add_downloaded_msg(self, login, uid, date):
        insert_msg = """
        INSERT INTO
          downloaded_msg (login, uid, date)
        VALUES
          ('{login}', "{uid}", '{date}');
        """.format(login=login, uid=int(uid), date=date)
        self.execute_query(insert_msg)

    def add_downloaded_payload(self, name, size, last_path, login, msg_uid):
        insert_payload = """
                INSERT INTO
                  downloaded_payloads (name, size, last_path, login, msg_uid)
                VALUES
                  ('{name}', '{size}', '{last_path}', '{login}', "{msg_uid}");
                """.format(name=name, size=size, last_path=last_path, login=login, msg_uid=int(msg_uid))
        self.execute_query(insert_payload)

    def create_connection(self, path):
        try:
            self.connection = sqlite3.connect(path)
            logger.debug("Установлено соединение с БД для статистики")
            self.create_tables()
            return True
        except sqlite3.Error as e:
            logger.error("Ошибка БД {}".format(e))
            return False

    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit();
        except sqlite3.Error as e:
            logger.error("Ошибка БД {}".format(e))

    def execute_read_query(self, query):
        cursor = self.connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except sqlite3.Error as e:
            logger.error(e)


def create_date(p_date_str):
    return p_date_str[0:25].strip()


def import_mail_login(path: str, sep=":"):
    mails_dict = {}
    if os.path.exists(path):
        with open(path, "rt") as f:
            for line_with_mail in f:
                login, password = line_with_mail.replace("\n", "").replace("\t", "").replace("\r", "").split(sep)[0:2]
                if login != "" or password != "":
                    mails_dict[login] = password
    return mails_dict


def create_config(config_file_name: str):
    """
       А тут про конфиги
    """
    config = ConfigParser()
    if not os.path.exists(config_file_name):
        with open(CONFIG_FILE, "wt") as f:
            config.add_section("SETTINGS")
            config.set("SETTINGS", "file_with_mails", "mail.txt")
            config.set("SETTINGS", "path_for_save_payloads", "")
            config.set("SETTINGS", "path_for_save_msg", "")
            config.add_section("MODE_PARAMS")
            config.set("MODE_PARAMS", "start_date", "")
            config.set("MODE_PARAMS", "end_date", "")
            config.set("MODE_PARAMS", "save_msg_mode_activate", "False")
            config.write(f)
    else:
        config.read(CONFIG_FILE)
    return config


if __name__ == "__main__":
    """
    Тут про логирование
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.FileHandler('email_loader_log.log')
    logger_handler.setLevel(logging.DEBUG)
    logger_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_handler)

    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-pdm', '--period_date_mode', action='store_true', dest='is_period_date_mode',
                        help='флаг для работы в режиме периода')
    parser.add_argument('-lum', '--last_uid_mode', action='store_true', dest='is_last_uid_mode',
                        help='флаг для скачивания начиная с последнего скаченного письма')
    parser.add_argument('-am', '--all_msg', action='store_true', dest='is_all_msg_download',
                        help='флаг для скачивания начиная с последнего скаченного письма')
    parser.add_argument('-ccf', '--create_config_file', action='store_true', dest='isNeedCreateDCF',
                        help='флаг указывает на необходимость заново создать конфигурационный файл с значениями по умолчанию')
    args_list = parser.parse_args()

    if args_list.isNeedCreateDCF:
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)
            create_config(CONFIG_FILE)

    config = create_config(CONFIG_FILE)
    path_for_email = config.get("SETTINGS", "file_with_mails", fallback=False)
    EDL = MsgLoader()
    MP = MsgParser()
    if not path_for_email:
        logger.error("Не указан файл с логинами и паролями к ящикам")
        exit(-1)
    mail_dict_name = config.get("SETTINGS", "file_with_mails", fallback=False)
    dict_mail = import_mail_login(mail_dict_name)
    if args_list.is_last_uid_mode:
        db = ManagerStatDB()
        db.create_connection(PATH_FOR_DB)
        for mail in dict_mail.keys():
            EDL.connect_mailbox(mail, dict_mail[mail])
            last_uid = db.get_last_uid(mail)
            if not last_uid:
                logger.error("Не удалось получить uid последнего письма из базы, для {}".format(mail))
                continue
            for msg in EDL.get_msg_for_last_uid(last_uid):
                MP.save_msg_payload(msg, dirname=mail)
    elif args_list.is_period_date_mode:
        startdate = config.get("MODE_PARAMS", "start_date", fallback=False)
        enddate = config.get("MODE_PARAMS", "end_date", fallback=False)
        if startdate and enddate:
            startdate = datetime.datetime.strptime(startdate, DEFAULT_DATE_FORMAT)
            enddate = datetime.datetime.strptime(enddate, DEFAULT_DATE_FORMAT)
            for mail in dict_mail.keys():
                EDL.connect_mailbox(mail, dict_mail[mail])
                for msg in EDL.get_msg_by_date_interval(startdate, enddate):
                    MP.save_msg(msg, path=mail+"/msg")
                    MP.save_msg_payload(msg, dirname=mail)

    elif args_list.is_all_msg_download:
        count = config.get("MODE_PARAMS", "count_msg_for_all_download", fallback=False)
        for mail in dict_mail.keys():
            if not EDL.connect_mailbox(mail, dict_mail[mail]):
                continue
            if count:
                for msg in EDL.get_all_msg(count):
                    MP.save_msg(msg, path=os.path.join(mail, "msg"))
            else:
                for msg in EDL.get_all_msg():
                    MP.save_msg(msg, path=os.path.join(mail, "msg"))
    else:
        pass
