import tweepy


#Авторизация
def tw_oauth(authfile):
    with open(authfile, "r") as f:
        ak = f.readlines()
    f.close()
    auth1 = tweepy.auth.OAuthHandler(ak[0].replace("\n",""), ak[1].replace("\n",""))
    auth1.set_access_token(ak[2].replace("\n",""), ak[3].replace("\n",""))
    return tweepy.API(auth1)
#Авторизация

#Получение результатов из курсора генератором с перерывами при таймауте
def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)
#Получение результатов из курсора генератором с перерывами при таймауте

#Поиск и возврат ID твиттов
def tw_search(api, q, count = 20):
    idlist = []
    for tweet in limit_handled(tweepy.Cursor(api.search, q, count = 20).items()):
        idlist.append(tweet.id)
    return idlist
#Поиск и возврат ID твиттов





import sqlite3
import urllib.request


#Создание БД для твиттов
def createbd(connect, name, recreate = False, url = False):
    c = connect.cursor()
    try:
        if recreate == True:
            c.execute('DROP TABLE IF EXISTS ' + name + ';')
        c.execute('''CREATE TABLE ''' + name + ''' (twid INTEGER NOT NULL UNIQUE PRIMARY KEY,
                                        twtext TEXT,
                                        date TEXT,
                                        expanded_url INTEGER,
                                        real_expanded_url INTEGER,
                                        author_screen_name TEXT,
                                        section TEXT,
                                        retweet_count INTEGER,
                                        favorite_count INTEGER,
                                        fullinfo BOOLEAN);''')
        connect.commit()
        
        if url == True:
            if recreate == True:
                c.execute('DROP TABLE IF EXISTS url;')
            c.execute('''CREATE TABLE url (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                                            src TEXT UNIQUE);''')
    except:
        print('Ошибка создания таблицы')
#Создание БД для твиттов
        
        
#Добавление списка ИД твиттов в БД
def addTwIdtoDB(connect, name, twidlist):
    if len(twidlist)==0:
        print ('Нет данных для вставки!')
        return
    values = ''
    if len(twidlist)==1:
        values = '(' + str(twidlist[0]) + ')'
    else:
        for i in range(len(twidlist)-1):
            values += '(' + str((twidlist[i])) + '), '
        values += '(' + str(twidlist[i+1]) + ')'
        
    c = connect.cursor()

    c.execute('INSERT OR REPLACE INTO ' + name + '(twid) VALUES ' + values)
    connect.commit()
#Добавление списка ИД твиттов в БД


#Поиск твиттов по запросу через API и добавление списка новых ИД твиттов в БД
def tw_search_and_add(api, q, connect, tdname,  count = 20, pres = False, returnlist = False):
    idlist = []
    c = connect.cursor()
    for tweet in limit_handled(tweepy.Cursor(api.search, q='vtbrussia.ru', count = 20).items()):
        result = c.execute('SELECT twid FROM ' + tdname + ' WHERE twid = ' + str(tweet.id) + ';').fetchone()
        if result == None:
            idlist.append(tweet.id)
        else:
            break
    if idlist != []:
        addTwIdtoDB(connect, tdname, idlist)
    if pres == True:
        print ('Добавлено ', str(len(idlist)), ' новых твиттов')
    if returnlist == True:
        return idlist
    return 
#Поиск твиттов по запросу через API и добавление списка новых ИД твиттов в БД


# Определение раздела сайта по ссылке и домену
def find_url_section(ex_url, domain):
    url = urllib.request.urlopen(ex_url).geturl().split('/')
    try:
        section = url[url.index(domain)+1]
    except:
        section = ''
    return section
# Определение раздела сайта по ссылке и домену


#Добавление полной twitter информации в БД по поиску Twitter API для списка ID
def tw_info_add(api, idlist, connect, tdname, qr="&quot;", domain=False):
    c = connect.cursor()
    count = len(idlist)//100 + 1
    for j in range(count):
        idlistpart = idlist[j*100:(j+1)*100]
        for i in api.statuses_lookup(idlistpart):
            try:
                try:
                    ex_url = i._json['entities']['urls'][0]['expanded_url']
                except:
                    ex_url = i._json['retweeted_status']['entities']['urls'][0]['expanded_url']
                c.execute('INSERT OR IGNORE INTO url (src) VALUES ( ? )', ( ex_url, ) )
                connect.commit()
                c.execute('SELECT id FROM url WHERE src=? LIMIT 1', ( ex_url, ))
                ex_url_id = c.fetchone()[0]
                twtext = i.text.replace('"', qr) #Если текст твита содержит двойные кавчки, они заменяются на значение из параметра qr
                sql = '''UPDATE ''' + tdname + ''' SET
                        twtext="''' + twtext + '''",
                        date="''' + str(i.created_at) + '''",
                        expanded_url="''' + str(ex_url_id) + '''",
                        author_screen_name="''' + i.author.screen_name + '''",
                        retweet_count=''' + str(i.retweet_count) + ''',
                        favorite_count=''' + str(i.favorite_count) + '''

                        WHERE twid=''' + str(i.id) + ''';'''
                c.execute(sql)
                connect.commit()
                try:
                    #Попытка определить корневой раздел сайта по ссылке из твитта
                    section = find_url_section(ex_url, domain)
                    sql = '''UPDATE ''' + tdname + ''' SET
                    section="''' + section + '''",
                    fullinfo=1 
                    WHERE twid='''+ str(i.id) + ''';'''
                    c.execute(sql)
                    connect.commit()
                    #Попытка определить корневой раздел сайта по ссылке из твитта
                    #Если попытка удалась, информация о твитте считается полной
                except:
                    print ('Не удалось запросить внедний адрес для получения раздела сайта по URL')
            except:
                print ('Ошибка!!! Не залочена ли БД?\n TwitterId = ' + str(i.id))
#Добавление полной twitter информации в БД по поиску Twitter API для списка ID






from datetime import datetime
import matplotlib.pyplot as plt


#Построение диаграмм твитов по дням (круговая/гистограмма)
    #connect > подключение к БД (формат connect = sqlite3.connect('vtb.db'))
    #tdname > имя таблицы с основными данными (строка)
    #pie > вывод груговой диаграммы (True/False)
    #bar > вывод гистограммы (True/False)
    #sectionbar > вывод гистограммы с разбивкой по разделам (True/False)
    #dtwsectlabels > список секций для гистограммы с разбивкой (формат dtwsectlabels = ['sport', 'culture', 'healthcare', 'tech']),
        #необязательный параметр. При отстутствии выбирает разделы из данных. При наличии все твиты из разделов, отсутствующих
        #в списке, будут занесены в "Other"
    #f > фильтр (формат f = {'section':'sport', 'real_expanded_url':'165'}), используем имена колонок в БД
def graph_tw_days_full(connect, tdname, pie = True, bar = True, sectionbar = True, dtwsectlabels = False, f = False):
    c = connect.cursor()
    #Фильтр
    where = ''
    if f!=False:
        where = ' WHERE '
        i = 0
        for key, value in f.items():
            if i>0:
                where += ' AND '
            where += key + "='" + value + "'"
            i+=1
    #Фильтр
            
    sql = "SELECT date, section FROM " + tdname + where
    res = c.execute(sql)
    twnum = [0, 0, 0, 0, 0, 0, 0]
    twsections = {}
    twnumsect = []
    if dtwsectlabels != False:
        twsectlabels = dtwsectlabels
        twsectlabels.extend(["Other"])
        for i in range(len(twsectlabels)):
            twsections[twsectlabels[i]]=i
            twnumsect.append([0, 0, 0, 0, 0, 0, 0])
    else:
        twsectlabels = []
    for tw in res.fetchall():
        colnum = datetime.strptime(tw[0], "%Y-%m-%d %H:%M:%S").weekday()
        twnum[colnum] += 1
        if dtwsectlabels == False:
            if (twsections.get(tw[1],-1) == -1):
                twsections[tw[1]] = len(twsections)
                twnumsect.append([0, 0, 0, 0, 0, 0, 0])
                twsectlabels.append(tw[1])
            twnumsect[twsections[tw[1]]][colnum]+=1
        else:
            if (twsections.get(tw[1],-1) == -1):
                twnumsect[twsections["Other"]][colnum] += 1
            else:
                twnumsect[twsections[tw[1]]][colnum] += 1
            

    GNUM = int(pie) + int(bar) + int(sectionbar)
    fig = plt.figure(figsize=(15,5*((GNUM+1)//2)))
    if GNUM >= 3:
        coordinats = [221, 222, 223, 224]
    else:
        coordinats = [121, 122]
    
    i = 0
    if pie == True:
        ax1 = fig.add_subplot(coordinats[i])
        i+=1
        labels = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        explode = [0, 0, 0, 0, 0, 0, 0]
        explode[twnum.index(max(twnum))] = 0.1
        colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral', 'red', 'green', 'magenta']
        ax1.pie(twnum, labels = labels, explode = explode, autopct = '%1.1f%%', shadow=True, colors = colors)
        
    if bar == True:
        ax2 = fig.add_subplot(coordinats[i])
        i+=1
        ax2.bar([0,1,2,3,4,5,6], twnum)
        ax2.set_xticklabels(['Mo', 'Tu ', 'We', 'Th', 'Fr', 'Sa', 'Su'])
        ax2.set_xticks([0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5])
        
    if sectionbar == True:
        ax3 = fig.add_subplot(coordinats[i])
        i+=1
        width = 1/(len(twsections)+1)
        ax3.set_xticklabels(['Mo', 'Tu ', 'We', 'Th', 'Fr', 'Sa', 'Su'])
        ax3.set_xticks([0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5])
        c = ["#0398fe", "#d84a91", "#01c1c3", "#7C7A9C", "black", "yellowgreen", "gold"]
        for i in range(len(twsections)):
            ax3.bar([0+width*i,1+width*i,2+width*i,3+width*i,4+width*i,5+width*i,6+width*i],
                    twnumsect[i], width, color=c[i], label=twsectlabels[i])
            plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
#Построение диаграмм твитов по дням (круговая/гистограмма)


#получение списка URL'ов сайта, отсортированным по частоте упоминания в твитах
def get_list_top_urls(connect, tdname, count = True):
    c=connect.cursor()

    urls=c.execute('''select url.src
                        from ''' + tdname + '''
                        join url ON ''' + tdname + '''.real_expanded_url = url.id''')
    d = {}
    for url in urls:
        if d.get(url[0], -1) == -1:
            d[url[0]] = 1
        else:
            d[url[0]] += 1

    #Сортировка по возрастанию
    def selSort(d):
        L = list(d.keys())
        for i in range(len(L) - 1):
            maxIndx = i
            maxVal = d[L[i]]
            j = i+1
            while j < len(L):
                if maxVal < d[L[j]]:
                    maxIndx = j
                    maxVal = d[L[j]]
                j += 1
            if maxIndx != i:
                temp = L[i]
                L[i] = L[maxIndx]
                L[maxIndx] = temp
        return L
    #Сортировка по возрастанию

    urls = selSort(d)

    if count == True:
        counts = []
        for url in urls:
            counts.append(d[url])
        return (urls, counts)
    else:
        return (urls)
#получение списка URL'ов сайта, отсортированным по частоте упоминания в твитах


#Построение гистограммы частоты урлов по частоте упоминания в твитах, используя get_list_top_urls()
def hist_top_urls(counts_top_urls, c = False):
    if c == False or c > len(counts_top_urls[1]):
        c = len(counts_top_urls[1])
    x = counts_top_urls[1][:c]
    fig = plt.figure(figsize=(16,5))
    ax = fig.add_subplot(111)
    ax.bar(range(len(x)), x)
    plt.show()
#Построение гистограммы частоты урлов по частоте упоминания в твитах, используя get_list_top_urls()
    
    
#Печать списка URL'ов сайта, отсортированным по частоте упоминания в твитах, используя get_list_top_urls()
def p_list_top_urls(list_top_urls, c = False):
    if c == False or c > len(list_top_urls[0]):
        c = len(list_top_urls[0])
        
    for i in range(c):
        print (i, ' > ', list_top_urls[0][i], ' [', list_top_urls[1][i] ,']', sep = '')
#Печать списка URL'ов сайта, отсортированным по частоте упоминания в твитах, используя get_list_top_urls()