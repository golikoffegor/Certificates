import certificates_q as q
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


from ets.ets_mysql_lib import MysqlConnection as mc
import pandas as pd


def add_quotes(a):
    return f"'{str(a)}'"


def make_line(itr):
    return add_quotes("', '".join(itr))


def get_data_from_db(cnx, query, as_dict=False, **kwargs):
    connect = mc(connection=cnx)
    with connect.open() as c:
        result = c.execute_query(query.format(**kwargs), dicted=as_dict)
    return result


def check_sert(organization_cert):
    query_to_type = {
        'Проверка сертификатов': q.ch_cert_in_sign,
    }

    for type, query in query_to_type.items():
        yield type, pd.DataFrame(get_data_from_db(mc.MS_SIGNATURES_CONNECT, query, as_dict=True, **organization_cert))


def make_report(gen_data, data_for_q):
    for type, pd_data in gen_data:
        check_data_df = pd_data.to_dict(orient='list')
        df_join = data_for_q.set_index('serial_number').join(pd_data.set_index('serial_number'))
        df_join['status'].fillna('-', inplace=True)

        writer = pd.ExcelWriter('Проверено.xlsx')
        df_join.to_excel(writer, sheet_name='checked_values', index=True)
        writer.save()

        if check_data_df['status'].count('Недействительный') or check_data_df['status'].count('Новый') or check_data_df['status'].count('Отклонённый'): 
            addr_from = "e.golikovv@fabrikant.ru"                # Адресат
            addr_to = "uc@etpz.ru"                               # Получатель
            password = "cp!S2PaP"                                # Пароль
            msg = MIMEMultipart()                                # Создаем сообщение
            msg['From'] = addr_from                              # Адресат
            msg['To'] = addr_to                                  # Получатель
            msg['Subject'] = 'Проверка сертификатов'             # Тема сообщения
            body = "В результате проверки найдены совпадения"    # Текст сообщения
            msg.attach(MIMEText(body, 'plain'))                  # Добавляем в сообщение текст
            server = smtplib.SMTP('mail.fabrikant.ru', 587)      # Создаем объект SMTP
            server.starttls()                                    # Начинаем шифрованный обмен по TLS
            server.login(addr_from, password)                    # Получаем доступ
            server.send_message(msg)                             # Отправляем сообщение
            server.quit()                                        # Выходим
        

def main():
    WORK_DIR = r"Z:/Сертификаты"
    os.chdir(WORK_DIR)
    listed_values = []
    data_for_q = {'serial_number':listed_values}
    excel_data_df = pd.read_excel('Certificates_for_check.xlsx', sheet_name='Certificates', usecols = "A")
    excel_data = excel_data_df.to_dict(orient='list')
    for val in excel_data.values():
        for v in val:
            v = str(v)
            v = ''.join(v.split()).upper()
            listed_values.append(v)

    cert_data = {}
    val_excel = [i for i in data_for_q.values()]
    val_excel = val_excel[0]
    cert_data['sert_str'] = make_line([i for i in val_excel])
    data_for_q = pd.DataFrame.from_dict(data_for_q)

    data_generator = check_sert(cert_data)
    make_report(data_generator, data_for_q)

if __name__ == '__main__':
    main()









