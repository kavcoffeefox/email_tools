# -*- coding: utf-8 -*-
import imaplib
import email
import email.message
import mimetypes
import logging
import logging.config
import traceback
import datetime
import os
import sqlite3
import argparse
import re
from configparser import ConfigParser

CONFIG_FILE = "config.ini"
DEFAULT_DATE_FORMAT = "%d.%m.%Y"
DATE_FORMAT_FOR_IMAP = "%d-%b-%Y"
PATH_FOR_DB = "mydb.sqllite"
LOG_CONF_FILE_NAME = "logging.conf"


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
        self.login = ""

    def connect_mailbox(self, login: str, password: str, mailbox="inbox"):
        self.login = login
        mailserver = login.split('@')[1]
        if login.split("@")[1] in ["bk.ru", "inbox.ru"]:
            mailserver = "mail.ru"
        try:
            self.mail = imaplib.IMAP4_SSL('imap.' + mailserver)
            self.mail.login(login, password)
            logger.info("Подключение к ящику {mailbox} успешно".format(mailbox=login))
            self.set_mailbox(mailbox)
            return True
        except:
            logger.error("Подключение к ящику {mailbox} не удалось. Он будет пропущен".format(mailbox=login))
            logger.debug("Ошибка: ", exc_info=True)
            return False

    def set_msg_parser(self, p_msg_parser):
        if isinstance(p_msg_parser, MsgParser):
            self.msgParser = p_msg_parser

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
                    logger.debug("Письмо с uid {uid} - успешно скачано".format(uid=uid_msg))
                else:
                    logger.error("Письмо с uid {uid} - скачать не удалось!!!".format(uid=uid_msg))
            return True, msg_list
        else:
            logger.info("Не удалось найти письма за {date} ".format(date=date))
            return False, msg_list

    def get_msg_by_uid(self, uid):
        result, data = self.mail.uid('fetch', uid, '(RFC822)')  # Получаем тело письма (RFC822) для данного ID
        if result == "OK":
            return data[0][1]  # Тело письма в необработанном виде
        else:
            logger.error("Не удалось прочитать письмо с заданным uid! uid: {}  result: {}".format(uid, result))
            logger.debug(exc_info=True)
            return False

    def download_all_msg(self, count=ParamNotSet()):
        status, data = self.mail.uid('search', None, "ALL")
        if isinstance(count, ParamNotSet):
            for p_uid in data[0].split():
                self.download_msg(p_uid)
        else:
            for p_uid in data[0].split()[:-1][0:int(count)]:
                self.download_msg(p_uid)

    def download_msg(self, uid, is_download_msg=True, is_download_payload=True):
        result, data = self.mail.uid('fetch', uid, '(RFC822)')  # Получаем тело письма (RFC822) для данного ID
        temp_msg = data[0][1]
        if result == "OK":
            if is_download_msg:
                self.msgParser.save_msg((uid, self.login, temp_msg))
                logger.debug("Письмо {uid} сохранено".format(uid=uid))
            if is_download_payload:
                self.msgParser.save_msg_payload((uid, self.login, temp_msg))
        else:
            logger.error("Не удалось прочитать письмо с заданным uid! uid: {}  result: {}".format(uid, result))
            logger.debug("Ошибка: ", exc_info=True)
            return False

    def create_imap_date(self, p_datestr):
        str_date = re.findall(r'\d+\s\w+\s\d\d\d\d\s\d\d:\d\d:\d\d', p_datestr)
        print(str_date)
        return datetime.datetime.strptime(str_date[0], "%d %b %Y %H:%M:%S")

    def download_msg_by_period(self, p_startdate, p_enddate=datetime.datetime.today(),
                               is_download_msg=True, is_download_payload=True):
        if p_startdate > p_enddate:
            p_startdate, p_enddate = p_enddate, p_startdate
            logger.info(
                "Дата начала периода больше даты конца! Работа будет продолжена, но даты будут поменяны местами!")
        status, data = self.mail.uid('search', None, "ALL")
        mailParser = MsgParser()
        if status == "OK":
            for uid_msg in data[0].split()[::-1]:
                msg = self.get_msg_by_uid(uid_msg)
                date_msg = self.create_imap_date(mailParser.get_msg_date(msg))
                if date_msg > p_enddate:
                    logger.debug("Письмо {uid} проигнорировано".format(uid=uid_msg))
                    continue
                elif date_msg >= p_startdate:
                    if is_download_msg:
                        self.msgParser.save_msg((uid_msg, self.login, msg))
                        logger.debug("Письмо {uid} сохранено".format(uid=uid_msg))
                    if is_download_payload:
                        self.msgParser.save_msg_payload((uid_msg, self.login, msg))
                else:
                    break

class MsgParser:
    """
        Данный класс парсит письма, приводит их в более управляемы и читаемый вид
    """
    def __init__(self):
        self.header = ""
        self.sender = ""
        self.path_for_payload = ""
        self.path_for_msg = ""

    def set_downloaded_msg_path(self, p_path_name):
        if not os.path.exists(p_path_name):
            temp = p_path_name.replace("/", " ").replace("\\", " ").split()
            os.makedirs(os.path.join(*temp))
            self.path_for_msg = os.path.join(*temp)
        else:
            self.path_for_msg = p_path_name

    def set_downloaded_payload_path(self, p_path_name):
        if not os.path.exists(p_path_name):
            temp = p_path_name.replace("/", " ").replace("\\", " ").split()
            os.makedirs(os.path.join(*temp))
            self.path_for_payload = os.path.join(*temp)
        else:
            self.path_for_payload = p_path_name

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

    def save_msg(self, p_msg):
        name_msg = "uid_"+str(int(p_msg[0])) + "_" + str(p_msg[1])
        save_path = os.path.join(self.path_for_msg, str(p_msg[1]))
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        with open(os.path.join(save_path, name_msg), "wb") as f:
            f.write(p_msg[2])


    def save_msg_payload(self, p_msg):
        uid_msg, login, msg = p_msg
        try:
            email_message = email.message_from_string(msg)
        except TypeError:
            email_message = email.message_from_bytes(msg)
        header_from = email.header.make_header(email.header.decode_header(email_message['From']))
        path = os.path.join(self.path_for_payload, login)
        if not os.path.exists(path):
            os.makedirs(path)
        for part in email_message.walk():
            filename = part.get_filename()
            if filename is not None:
                filename = str(email.header.make_header(email.header.decode_header(filename)))
            if not filename:
                continue
            logger.info("--- Нашли письмо от: {h_from}. Дата: {h_date}".format(h_from=str(header_from),
                                                                               h_date=self.get_msg_date(msg)))
            save_path = os.path.join(path, os.path.basename(filename))
            try:
                with open(save_path, 'wb') as fp:
                    fp.write(part.get_payload(decode=1))
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
    temp_config = ConfigParser()
    if not os.path.exists(config_file_name):
        with open(CONFIG_FILE, "wt") as f:
            temp_config.add_section("SETTINGS")
            temp_config.set("SETTINGS", "file_with_mails", "mail.txt")
            temp_config.set("SETTINGS", "path_for_save_payloads", "")
            temp_config.set("SETTINGS", "path_for_save_msg", "")
            temp_config.add_section("MODE_PARAMS")
            temp_config.set("MODE_PARAMS", "start_date", "")
            temp_config.set("MODE_PARAMS", "end_date", "")
            temp_config.set("MODE_PARAMS", "save_msg_mode_activate", "False")
            temp_config.set("MODE_PARAMS", "count_msg_for_all_download", "")
            temp_config.write(f)
    else:
        temp_config.read(CONFIG_FILE)
    return temp_config


if __name__ == "__main__":
    """
    Тут про логирование
    """
    if not os.path.exists(LOG_CONF_FILE_NAME):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger_handler = logging.FileHandler('email_loader.log')
        logger_handler.setLevel(logging.DEBUG)
        logger_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        logger_handler.setFormatter(logger_formatter)
        logger.addHandler(logger_handler)
    else:
        logging.config.fileConfig(LOG_CONF_FILE_NAME)
        logger = logging.getLogger()

    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-pdm', '--period_date_mode', action='store_true', dest='is_period_date_mode',
                        help='''флаг для работы в режиме периода''')
    parser.add_argument('-am', '--all_msg', action='store_true', dest='is_all_msg_download',
                        help='''флаг для скачивания всех писем начиная с конца 
                        (количество писем может быть ограничено параметром count_msg_for_all_download)
                        count_msg_for_all_download = 10 - озаначает, что с каждой почты будет скачано последние 10
                        писем''')
    parser.add_argument('-ccf', '--create_config_file', action='store_true', dest='isNeedCreateDCF',
                        help='''флаг указывает на необходимость заново создать конфигурационный файл''')
    parser.add_argument('-lw', '--last_week', action="store_true", dest="is_lw")
    parser.add_argument('-lm', '--last_month', action="store_true", dest="is_lm")
    parser.add_argument('-ld', '--last_day', action="store_true", dest="is_ld")
    parser.add_argument('-t', '--test', action="store_true", dest="is_test")
    args_list = parser.parse_args()

    if args_list.isNeedCreateDCF:
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)
            create_config(CONFIG_FILE)

    config = create_config(CONFIG_FILE)
    path_for_email = config.get("SETTINGS", "file_with_mails", fallback=False)
    EDL = MsgLoader()
    MP = MsgParser()
    """ 
        Устанавливаем параметры сохранения писем и вложений согласно конфигу
    """
    path_for_save_payloads = config.get("SETTINGS", "path_for_save_payloads", fallback=False)
    path_for_save_msg = config.get("SETTINGS", "path_for_save_msg", fallback=False)
    if path_for_save_msg:
        MP.set_downloaded_msg_path(path_for_save_msg)
    if path_for_save_payloads:
        MP.set_downloaded_payload_path(path_for_save_payloads)
    EDL.set_msg_parser(MP)

    if not path_for_email:
        logger.error("Не указан файл с логинами и паролями к ящикам")
        exit(-1)
    mail_dict_name = config.get("SETTINGS", "file_with_mails", fallback=False)
    dict_mail = import_mail_login(mail_dict_name)
    """
        Выбор режима работы программы согласно агрументам командной строки
    """
    if args_list.is_period_date_mode:
        logger.info("Работа в режиме скачавания писем согласно периода запущена!")
        startdate = config.get("MODE_PARAMS", "start_date", fallback=False)
        enddate = config.get("MODE_PARAMS", "end_date", fallback=False)
        if startdate and enddate:
            startdate = datetime.datetime.strptime(startdate, DEFAULT_DATE_FORMAT)
            enddate = datetime.datetime.strptime(enddate, DEFAULT_DATE_FORMAT)
            for mail in dict_mail.keys():
                EDL.connect_mailbox(mail, dict_mail[mail])
                for msg in EDL.get_msg_by_date_interval(startdate, enddate):
                    MP.save_msg(msg)
                    MP.save_msg_payload(msg)

    elif args_list.is_all_msg_download:
        logger.info("Работа в режиме скачивания всех писем с ящика запущена!")
        count = config.get("MODE_PARAMS", "count_msg_for_all_download", fallback=False)
        for mail in dict_mail.keys():
            if not EDL.connect_mailbox(mail, dict_mail[mail]):
                continue
            if count:
                logger.info("Установлено ограничение на {}!".format(count))
                EDL.download_all_msg(count)
            else:
                EDL.download_all_msg()
    elif args_list.is_test:
        logger.info("Работа в режиме скачавания писем согласно периода запущена!")
        startdate = config.get("MODE_PARAMS", "start_date", fallback=False)
        enddate = config.get("MODE_PARAMS", "end_date", fallback=False)
        if startdate and enddate:
            startdate = datetime.datetime.strptime(startdate, DEFAULT_DATE_FORMAT)
            enddate = datetime.datetime.strptime(enddate, DEFAULT_DATE_FORMAT)
            logger.info("Начальная дата {}, конечная дата {}!".format(startdate, enddate))
            for mail in dict_mail.keys():
                if not EDL.connect_mailbox(mail, dict_mail[mail]):
                    continue
                EDL.download_msg_by_period(startdate, enddate)

    else:
        print("Для запуска программы используйте параметры командной строки. (-h для списка параметров)")
