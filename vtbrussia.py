import sqlite3
import urllib.request
import re


#Определение реальных страниц сайта, по ссылкам из соц сети
    #connect > подключение к БД (например, conn = sqlite3.connect('twi.db'))
    #tdname > имя таблицы с данными, строка
    #domain > домен сайта, строка (например, 'vtbrussia.ru)
    #t > тип исчтоника данных (tw - Twitter)
def set_real_expurls(connect, tdname, domain, t = 'tw'):
    c = connect.cursor()
    c.execute('''SELECT ''' + tdname + '''.''' + t + '''id, url.src FROM ''' + tdname + ''' 
    JOIN url ON ''' + tdname + '''.expanded_url = url.id
    WHERE ''' + tdname + '''.real_expanded_url IS NULL''')
    for element in c:
        url = urllib.request.urlopen(element[1]).geturl()
        urls = url.split('/')
        if urls[2] == domain:
            try:
                if urls[-1][0] == "?" or urls[-1][0] == "#":
                    url = '/'.join(urls[0:-1])
            except:
                pass
            if urls[3] =='shared':
                page_code = urllib.request.urlopen(url).read()
                url = 'http://' + domain + re.findall('window.location.href = "(.*?)";', str(page_code))[0]
            if url[-1] !='/':
                url +='/'
        elif urls[2] == 'linkis.com':
            page_code = urllib.request.urlopen(url).read()
            url = re.findall('<meta property="og:url" content="(.*?)" />', str(page_code))[0]
        else:
            print ('не домен ВТБР', url)
            continue
        c2 = connect.cursor()
        c2.execute('INSERT OR IGNORE INTO url (src) VALUES ( ? )', ( url, ) )
        connect.commit()
        c2.execute('SELECT id FROM url WHERE src=? LIMIT 1', ( url, ))
        real_ex_url_id = c2.fetchone()[0]
        c2.execute('UPDATE ' + tdname + ' SET real_expanded_url=?  WHERE ' + t + 'id=?;', ( str(real_ex_url_id), str(element[0])) )
        connect.commit()
#Определение реальных страниц сайта, по ссылкам из соц сети