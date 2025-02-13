# ibet

---
<a id="ibet-scraping-section"></a>
## ~~~ 1. Сбор данных ~~~
--- 

Данные собираются поочередно с трех сайтов и и передаются в Redis либо Clickhouse.
В Redis передаются данные по Live ставкам для быстрого расчета и отображения.

В Clickhouse передаются все данные для дэшборда и аналитики
  
Для примера выкладываю скрипт сбора данных с сайта с Parimatch с бесконечной прокрутуой. 

Из интерсного - данные обновляются только при выводе на дисплей, обойти эту защиту нельзя. 
Техническое решение - виртуальный дисплей + уменьшение масштаба


<details>
  <summary><strong>🖼️ Внешний вид сайта</strong></summary>

  ![Внешний вид сайта](https://raw.githubusercontent.com/sazhiromru/images/refs/heads/main/pari-live-section.PNG)
</details>

<details>
  <summary><strong>📜 Parimatch</strong></summary>

```python
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
import time
import re
from selenium.webdriver.common.by import By
from random import uniform
from datetime import datetime, timedelta
import os
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium import webdriver
import clickhouse_connect
import pandas as pd

os.environ["DISPLAY"] = ":1.0"

def initialize_driver():

    profile_path = '/home/venediktovga/.mozilla/firefox/a6k6fc50.default-release'
    options = Options()
    options.set_preference("profile", profile_path)
    

    options.set_preference("layout.css.devPixelsPerPx", "0.25")  # Уменьшаем Масштаб для быстрого сканирования данных
    options.set_preference("browser.sessionstore.privacy_level", 2)

    options.add_argument("--disable-infobars")  
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox") 
    options.add_argument("--disable-dev-shm-usage")  
    options.add_argument("--start-maximized")
    options.set_preference("permissions.default.image", 2) 
    options.set_preference("browser.cache.disk.enable", False)
    options.set_preference("browser.cache.memory.enable", False)
    options.set_preference("browser.sessionstore.privacy_level", 2)

    service = Service("/usr/local/bin/geckodriver")  # Используем Мозила для скорости + плагины для user agent
    service.log_path = os.devnull


    driver = webdriver.Firefox(service=service, options=options)
    driver.set_window_size(1920, 1080) 
    print('Запустили драйвер Firefox')
    driver.set_page_load_timeout(70)  
    time.sleep(2) 
    driver.maximize_window()  

    return driver

#архив результатов за вчерашний день для аналитики

def source_page(driver):
    date = (datetime.now() - timedelta(days = 1))
    url_part = date.strftime('%Y-%m-%d')
    url = f'https://pari.ru/results?date={url_part}'
    driver.get(url)
    time.sleep(14)
    return date

def extract(driver,date):
    data = driver.page_source
    soup = BeautifulSoup(data, 'html.parser')
    matches = soup.find_all('div', class_='results-event--Me6XJ')
    results_list = []
    for match in matches:
        row = []
        #дата
        row.append(date)
        #категория
        category = match.find_previous('div', class_ = re.compile(r'results-sport__caption-container--e43SF'))
        row.append(category.text)
        #подраздел
        try:
            subcategory =  match.find_previous('div', class_ = re.compile(r'overflowed-text--JHSWr results-competition__caption--zmv7q'))
            row.append(subcategory.text)
        except Exception:
            row.append('no subcategory')
        teams = match.find_all('div', class_=re.compile(r'results-event-team__name'))
        team_list = [team.get_text(strip = True) for team in teams]
        #команда 1 и 2
        row.extend(team_list)
        #событие
        try:
            row.append(' — '.join([team_list[0],team_list[1]]))
        except Exception:
            row.append('-')
        #счет   
        try:
            scores = match.find_all('div', class_=re.compile(r'results-scoreBlock__score--XvlMM _summary--Jt8Ej _bold--JaGTY'))
            score_list = [str(score.get_text(strip = True)) for score in scores]
            if len(score_list) == 2:
                row.extend(score_list)
            else:
                row.append('-')
                row.append('-')
        except Exception:
            row.append('-')
            row.append('-')
        #исход
        try:
            if int(score_list[0])> int(score_list[1]):
                row.append('1')
            elif int(score_list[1])> int(score_list[0]):
                row.append('2')
            elif int(score_list[0]) == int(score_list[1]):
                row.append('X')
            else:
                row.append('-')
        except Exception:
            row.append('-')
        try:
            row.append(int(score_list[0]) - int(score_list[1]))
            row.append(int(score_list[1]) - int(score_list[0]))
        except Exception:
            row.append(-1000)
            row.append(-1000)
        try:
            row.append((int(score_list[0])+int(score_list[1])))
        except Exception:
            row.append(-1000)
        results_list.append(row)
    column_names = ["date", "category", "subcategory", "team1", "team2", "event", "score1", "score2", "stavka", "f1", "f2", "total"]
    df = pd.DataFrame(results_list, columns = column_names)
    df.to_csv('results.csv', index = False, encoding='UTF-8-sig')
    upload(results_list)

def scroll_container(driver,date):
    count_repeat_break = 0
    count = 0
    while True:
        extract(driver,date)
        print(f'данные прогона {count} загружены')
        # Проверяем окончание промотки через то же повторение элементов
        elements = driver.find_elements(By.CLASS_NAME, "results-event--Me6XJ")
        last_element = elements[-1] if elements else None  
        if last_element:
            driver.execute_script("arguments[0].scrollIntoView({block: 'start', inline: 'nearest'});", last_element)
            count+=1
        print(f'спуск номер {count}')
        time.sleep(uniform(9,11))
        elements = driver.find_elements(By.CLASS_NAME, "results-event--Me6XJ")
        new_last_element = elements[-1]
        if new_last_element == last_element:
            count_repeat_break+=1
        else:
            count_repeat_break = 0
        if count_repeat_break == 3:
            break
    client = clickhouse_connect.get_client(host='10.140.0.7', port=8123, username='default', password='******')
    client.insert('ibet.results_date', [(datetime.now(),)], column_names='date')

#Загружаем напрямую в CLickhouse

def upload(extracted_data):
    client = clickhouse_connect.get_client(host='10.140.0.7', port=8123, username='default', password='******')
    client.insert('ibet.results',extracted_data, column_names = ["date", "category", "subcategory", "team1", "team2", "event", "score1", "score2", "stavka", "f1", "f2", "total"]
)

def main():
    try:
        driver = initialize_driver()
        date = source_page(driver)
        scroll_container(driver,date)

    finally:
        driver.quit()

if __name__ == '__main__':
    main()
```

</details>
<br></br>

---
<a id="ibet-wrangling-section"></a>
## ~~~ 2. Обработка данных ~~~
--- 


### Merge  

По live ставкам данные поступает через REDIS в течение 10 секунд, в зависимости от страницы либо глубины поиска на сайте.

Цель быстро сопоставить события и найти коэффиценты, которые максимально отличаются.

Сложность в транлитерации между кририллицей и иностранными букмекерами + все собирают названия различных команд и лиг по разному.

Решение - в транслитерации и сопоставлению названий команд/игроков отдельно по словам и по буквам специальными отдельными функциями.

Так же координатор принимает данные через Redis и загружает в CLickhouse

<details>
  <summary><strong>📜 Merge </strong></summary>

```python
import redis
import time
from datetime import datetime,timedelta
from random import uniform
import json
import pandas as pd
from io import StringIO
from kafka import KafkaProducer

# Тут все довольно сложно

redis_client = redis.StrictRedis(
    host='localhost', 
    port=6379, 
    password='*****', 
    decode_responses=True
)

# Во первых для синхронизации мы отправляем тайминги, рандомные, раз в 4 минуты +-60 секунд скраперы начинают синхронный сбор данных
def publish_timing():
    result = []
    big_pause_delta = uniform(140,250)
    big_pause = (datetime.now() + timedelta(seconds = big_pause_delta)).timestamp()
    delta1 = uniform(15,21)
    time1 = (datetime.now() + timedelta(seconds=delta1)).timestamp()
    result.extend([big_pause,time1])
    message = json.dumps(result)
    redis_client.publish('timings', message)
    print(f'отправлено сообщение {message}')
    return big_pause

# перед началом сбора ждем готовности скраперов, получаем отмашку
def gotovo():
    pubsub = redis_client.pubsub()
    pubsub.subscribe('gotovo')
    count = 0
    for message in pubsub.listen():
        if message['type'] == 'message' and message['data'] == 'gotovo':
            count+=1
            print(f'polycheno otvetov {count}')
            if count == 3:
                return True
        elif message['type'] == 'message' and message['data'] == 'ne gotovo':
            return False

# Функции делят команды на отельные слова, делают транслитерацию при необходимости, сравнивают слова побуквенно. 
# Сравнение стандартными библиотеками для оценки схожести не работает!
def are_words_equal_with_tolerance(word1, word2):

    if abs(len(word1) - len(word2)) > 2:
        return False  #

    # Считаем количество несовпадающих символов
    diff_count = sum(1 for a, b in zip(word1, word2) if a != b)
    
    # Добавляем к числу отличий разницу в длине слов, если слова разной длины
    diff_count += abs(len(word1) - len(word2))
    
    # Если различаются более чем на 2 символа, то считаем, что они не совпадают
    return diff_count <= 2

def compare_names(name1, name2):
    # Разделяем имена на части по символам "-" или "—"
    parts1 = [part.strip() for part in name1.replace('—', '-').split('-') if len(part.strip())>=3]
    words1_1 = [word.strip() for word in parts1[0].split(' ') if len(word.strip())>4]
    words1_2 = [word.strip() for word in parts1[1].split(' ') if len(word.strip())>4]
    parts2 = [part.strip() for part in name2.replace('—', '-').split('-') if len(part.strip())>=3]
    words2_1 = [word.strip() for word in parts2[0].split(' ') if len(word.strip())>4]
    words2_2 = [word.strip() for word in parts2[1].split(' ') if len(word.strip())>4]
    check1 = False
    check2 = False

    for words1 in words1_1:
        for words2 in words2_1:
            if are_words_equal_with_tolerance(words1,words2) == True:
                check1 = True
                break
    for words1 in words1_2:
        for words2 in words2_2:
            if are_words_equal_with_tolerance(words1,words2) == True:
                check2 = True
                break
            
    return check1 and check2

# Далее простой merge - собираем из двух ДФ один
def pari_olimp(df1, df2):
    columns_pari = ['event', '1', 'X', '2', 'F1', 'F2', 'Tb', 'Tm', 'F', 'T', 'timestamp', 'category', 'subcategory']
    columns_olimp = ['event_olimp', '1_olimp', 'X_olimp', '2_olimp', 'F1_olimp', 'F2_olimp', 'Tb_olimp', 'Tm_olimp', 'F_olimp', 'T_olimp', 'timestamp_olimp']
    columns_pinn = ['timestamp_pinn', 'cate_pinn', 'event_pinn', 'event_reverse_pinn', '1_pinn', 'X_pinn', '2_pinn', 'F1_pinn', 'F2_pinn', 'Tb_pinn', 'Tm_pinn', 'F_pinn', 'T_pinn']
    

    df2.columns = columns_olimp


    match_indexes = []
    try:
        for index1, row1 in df1.iterrows():

            for index2, row2 in df2.iterrows():
                if compare_names(row1['event'], row2['event_olimp']) == True:
                    match_indexes.append([index1,index2]) 
    except Exception as e:
        print(row1,row2)         


    df = pd.DataFrame(columns=columns_pari+columns_olimp)
    for index1, index2 in match_indexes:
        row1 = df1.iloc[index1]
        row2 = df2.iloc[index2]
        combined = pd.DataFrame({
            **row1.to_dict(),    
            **row2.to_dict()     
        }, index=[0])
        dataframes = [df, combined]
        valid_dataframes = [df for df in dataframes if not df.empty and not df.isna().all().all()]
        df = pd.concat(valid_dataframes, ignore_index=True)
    df.drop_duplicates(subset=['event'], keep= 'last',inplace= True)
    df_olimp = df[['event', 'event_olimp', '1', '1_olimp', 'X', 'X_olimp', '2', '2_olimp', 'F1', 'F1_olimp', 'F2', 'F2_olimp', 'Tb', 'Tb_olimp', 'Tm', 'Tm_olimp', 'F', 'F_olimp', 'T', 'T_olimp', 'timestamp', 'timestamp_olimp', 'category', 'subcategory']]   

    return df_olimp

# Далее простой merge - собираем из двух ДФ один
def pari_pinn(df1, df2):
    columns_pari = ['event', '1', 'X', '2', 'F1', 'F2', 'Tb', 'Tm', 'F', 'T', 'timestamp', 'category', 'subcategory']
    columns_pinn = ['timestamp_pinn', 'cate_pinn', 'event_pinn', 'event_reverse_pinn', '1_pinn', 'X_pinn', '2_pinn', 'F1_pinn', 'F2_pinn', 'Tb_pinn', 'Tm_pinn', 'F_pinn', 'T_pinn']
    

    df2.columns = columns_pinn

    match_indexes = []
    for index1, row1 in df1.iterrows():
        for index2, row2 in df2.iterrows():
            if compare_names(row1['event'], row2['event_pinn']) == True or compare_names(row1['event'], row2['event_reverse_pinn']) == True:
                match_indexes.append([index1,index2])          
            if compare_names(row1['event'], row2['event_pinn']) == False and compare_names(row1['event'], row2['event_reverse_pinn']) == True:
                df2.at[index2, 'event_pinn'] = row2['event_reverse_pinn']
                df2.at[index2, '1_pinn'], df2.at[index2, '2_pinn'] = row2['2_pinn'], row2['1_pinn']
                df2.at[index2, 'F1_pinn'], df2.at[index2, 'F2_pinn'] = row2['F2_pinn'], row2['F1_pinn']



    df2.columns = columns_pinn
    df = pd.DataFrame(columns=columns_pari+columns_pinn)
    for index1, index2 in match_indexes:
        row1 = df1.iloc[index1]
        row2 = df2.iloc[index2]
        combined = pd.DataFrame({
            **row1.to_dict(),    
            **row2.to_dict()     
        }, index=[0])
        dataframes = [df, combined]
        valid_dataframes = [df for df in dataframes if not df.empty and not df.isna().all().all()]
        df = pd.concat(valid_dataframes, ignore_index=True)
    df.drop_duplicates(subset=['event'], keep= 'last',inplace= True)   
    df_pinn = df[['event', 'event_pinn', '1', '1_pinn', 'X', 'X_pinn', '2', '2_pinn', 'F1', 'F1_pinn', 'F2', 'F2_pinn', 'Tb', 'Tb_pinn', 'Tm', 'Tm_pinn', 'F', 'F_pinn', 'T', 'T_pinn', 'timestamp', 'timestamp_pinn', 'category','subcategory','cate_pinn']
    ]

    index_to_drop = []

    for index, row in df_pinn.iterrows():
        if str(row['category']).lower() != str(row['cate_pinn']).lower():
            index_to_drop.append(index)
    print('убираем строки:', index_to_drop)
    df_pinn = df_pinn.drop(index = index_to_drop)
    df_pinn = df_pinn.drop(columns = 'cate_pinn')  
    
    return df_pinn

# из собранных воедино ДФ ищем разницу коэффицентов по совпадающим событиям
def compare(df, event, event_2, _1, _1_2, X, X_2, _2, _2_2, F1, F1_2, F2, F2_2, Tb, Tb_2, Tm, Tm_2, F, F_2, T, T_2, timestamp, timestamp_2, label,subcategory):
    
    numeric_columns = [_1, _1_2, X, X_2, _2, _2_2, F1, F1_2, F2, F2_2, Tb, Tb_2, Tm, Tm_2, F, F_2, T, T_2, timestamp, timestamp_2]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col],errors='coerce')

    stavki = []

    for index, row in df.iterrows():
        for pair in [[_1,_1_2], [X, X_2], [_2, _2_2]]:
            try:
                coef = max(row[pair[0]]/row[pair[1]], row[pair[1]]/(row[pair[0]]))
                if  coef>1.15 and abs(row[timestamp] - row[timestamp_2])<30 and (1.1<=row[pair[0]]<=2 or 1.1<=row[pair[1]]<=3):
                    row_stavki = []
                    row_stavki.append(row[event])
                    row_stavki.append(row[label])
                    row_stavki.append(str(pair[0]).replace('_',''))
                    row_stavki.append('-')
                    row_stavki.extend([row[pair[0]], row[pair[1]]])
                    if row[pair[0]]>row[pair[1]]:
                        row_stavki.append('Parimatch')
                    else:
                        row_stavki.append('pinnacle_or_olimp')
                    coef = round(coef, 2)
                    row_stavki.append(coef)
                    row_stavki.append(row[timestamp])
                    row_stavki.append(row[timestamp_2])
                    stavki.append(row_stavki)
            except Exception as e:
                print(f'возникла ошибка {e}')
                pass
            
        for pair in [[F1, F1_2],[F2,F2_2]]:
            if row[F] == row[F_2]:
                try:
                    row_stavki = []
                    coef = max(row[pair[0]]/row[pair[1]], row[pair[1]] / row[pair[0]])
                    if  coef>1.15 and abs(row[timestamp] - row[timestamp_2])<30 and (1.3<=row[pair[0]]<=2 or 1.1<=row[pair[1]]<=3):
                        row_stavki = []
                        row_stavki.append(row[event])
                        row_stavki.append(row[label])
                        row_stavki.append(str(pair[0]).replace('_',''))
                        row_stavki.append(row[F])
                        row_stavki.extend([row[pair[0]], row[pair[1]]])
                        if row[pair[0]]>row[pair[1]]:
                            row_stavki.append('Parimatch')
                        else:
                            row_stavki.append('pinnacle_or_olimp')
                        coef = round(coef, 2)
                        row_stavki.append(coef)
                        row_stavki.append(row[timestamp])
                        row_stavki.append(row[timestamp_2])
                        stavki.append(row_stavki)
                except Exception as e:
                    print(f'возникла ошибка {e}')
                    pass

        for pair in [[Tb, Tb_2],[Tm,Tm_2]]:
            if row[T] == row[T_2]:
                try:
                    row_stavki = []
                    coef = max(row[pair[0]]/row[pair[1]], row[pair[1]] / row[pair[0]])
                    if  coef>1.15 and abs(row[timestamp] - row[timestamp_2])<30 and (1.1<=row[pair[0]]<=2 or 1.1<=row[pair[1]]<=3):
                        row_stavki = []
                        row_stavki.append(row[event])
                        row_stavki.append(row[label])
                        row_stavki.append(str(pair[0]).replace('_',''))
                        row_stavki.append(row[T])
                        row_stavki.extend([row[pair[0]], row[pair[1]]])
                        if row[pair[0]]>row[pair[1]]:
                            row_stavki.append('Parimatch')
                        else:
                            row_stavki.append('pinnacle_or_olimp')
                        coef = round(coef, 2)
                        row_stavki.append(coef)
                        row_stavki.append(row[timestamp])
                        row_stavki.append(row[timestamp_2])
                        stavki.append(row_stavki)
                except Exception as e:
                    print(f'возникла ошибка {e}')
                    pass
    df = pd.DataFrame(stavki, columns = ['Event','Category','Stavka', 'F_T', 'Coef1', 'Coef2','Platform', 'Ratio', 'Timestamp','Timestamp_2'])  
    df.sort_values(by=['Event','Ratio'], inplace=True)    
    df.drop_duplicates(subset='Event', keep='last',inplace=True,ignore_index=True) 
    df.sort_values(by=['Ratio'], inplace=True, ascending=False)   
    return(df)

def listen():
    pubsub = redis_client.pubsub()
    pubsub.subscribe('df')
    df_pari, df_pinn, df_olimp = None, None, None
    count_parimatch, count_pinnacle, count_olimpbet = 0, 0, 0
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            print('получили сообщение')
            string_df = json.loads(message['data'])
            
            if string_df['source'] == 'pinnacle':
                count_pinnacle += 1
                print(f'получили пинакл дф {count_pinnacle}')
                df_pinnacle = string_df['data']
                df_pinn = pd.read_csv(StringIO(df_pinnacle))
                print(df_pinn.shape)
                print(df_pinn.head())
                
            if string_df['source'] == 'parimatch':
                count_parimatch += 1
                print(f'получили париматч дф {count_parimatch}')
                df_parimatch = string_df['data']
                df_pari = pd.read_csv(StringIO(df_parimatch))
                print(df_pari.shape)
                print(df_pari.head())
                
            if string_df['source'] == 'olimpbet':
                count_olimpbet += 1
                print(f'получили олимпбет {count_olimpbet}')
                df_olimpbet = string_df['data']
                df_olimp = pd.read_csv(StringIO(df_olimpbet))
                print(df_olimp.shape)
                print(df_olimp.head())
                
            if count_parimatch == 1 and count_olimpbet == 1 and count_pinnacle == 1:
                print('все данные получены')
                

                df_pari_olimp = None
                df_pari_pinn = None

                if df_pari is not None and df_olimp is not None:
                    df_pari_olimp = pari_olimp(df_pari, df_olimp)
                if df_pari is not None and df_pinn is not None:
                    df_pari_pinn = pari_pinn(df_pari, df_pinn)
                    
                if df_pari_pinn is not None:  
                    df_pinn_final = compare(
                        df_pari_pinn, 
                        event='event', 
                        event_2='event_pinn', 
                        _1='1', 
                        _1_2='1_pinn', 
                        X='X', 
                        X_2='X_pinn', 
                        _2='2', 
                        _2_2='2_pinn', 
                        F1='F1', 
                        F1_2='F1_pinn', 
                        F2='F2', 
                        F2_2='F2_pinn', 
                        Tb='Tb', 
                        Tb_2='Tb_pinn', 
                        Tm='Tm', 
                        Tm_2='Tm_pinn', 
                        F='F', 
                        F_2='F_pinn', 
                        T='T', 
                        T_2='T_pinn', 
                        timestamp='timestamp', 
                        timestamp_2='timestamp_pinn', 
                        label='category',
                        subcategory = 'subcategory'
                    )
                    print('Есть сравнение пари и пинн')
                    df_pinn_final['Platform'] = df_pinn_final['Platform'].str.replace('pinnacle_or_olimp', 'Pinn_Pari')
                    df_pinn_final['Platform'] = df_pinn_final['Platform'].str.replace('Parimatch', 'Pari_Pinn')
                    print(df_pinn_final.head())
                    print(df_pinn_final.shape)

                if df_pari_olimp is not None:
                    df_olimp_final = compare(
                        df_pari_olimp, 
                        event='event', 
                        event_2='event_olimp', 
                        _1='1', 
                        _1_2='1_olimp', 
                        X='X', 
                        X_2='X_olimp', 
                        _2='2', 
                        _2_2='2_olimp', 
                        F1='F1', 
                        F1_2='F1_olimp', 
                        F2='F2', 
                        F2_2='F2_olimp', 
                        Tb='Tb', 
                        Tb_2='Tb_olimp', 
                        Tm='Tm', 
                        Tm_2='Tm_olimp', 
                        F='F', 
                        F_2='F_olimp', 
                        T='T', 
                        T_2='T_olimp', 
                        timestamp='timestamp', 
                        timestamp_2='timestamp_olimp', 
                        label='category',
                        subcategory = 'subcategory'
                    )
                    print('Есть сравнение пари и олимп')
                    df_olimp_final['Platform'] = df_olimp_final['Platform'].str.replace('pinnacle_or_olimp','Olimp_Pari')
                    df_olimp_final['Platform'] = df_olimp_final['Platform'].str.replace('Parimatch','Pari_Olimp')
                    print(df_olimp_final.head())
                    print(df_olimp_final.shape)
                
                # Break the loop after processing
                return df_pinn_final,df_olimp_final

# загружаем в KAFKA найденные несовпадающие коэффиценты
def send_dataframe_to_kafka(df, topic = 'ibet', bootstrap_servers = '10.140.0.2:9092'):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for _, row in df.iterrows():

        message = row.to_dict()
        producer.send(topic, message)
        
    producer.flush()
    producer.close()

#Собираем все здесь. 
# Сначала ждем готовность. 
# Затем отправляем время синхронного скрапинга. 
# После получения всех данных сопоставляем значения. 
# Затем загружаем данные в Clickhouse и повторяем

def main(): 
    if gotovo():
        try:
            print('готово')
            time.sleep(1)
            while True:
                big_pause = publish_timing()
                print(f'отправили тайминг {datetime.now()}')
                print('начали слушать')
                pinn,olimp = listen()
                send_dataframe_to_kafka(pinn)
                send_dataframe_to_kafka(olimp)
                print(f'начали спать в {datetime.now()}')
                while time.time()<big_pause+10:
                    time.sleep(1)
                print(f'закончили спать в {datetime.now()}')
        except KeyboardInterrupt:
            print('астанавитесь')   
    else:
        raise ValueError('Не готово')


if __name__ == '__main__':
    main()

```
</details>  
<br>

### VNC 

Сайты используют Google Capthca, в среднем раз в сутки.
Обойти это невозможно, единственный способ - пройти ее раз в сутки.
Для этого используем VNC

Устанавливаем XFCE4 в качестве рабочего стола, gnome icon для иконок и tightvnc для отображения.
RealVNC для соединения с удаленным рабочим столом
Так же создаем профиль для браузера

<details>
  <summary><strong>🖼️ VNC </strong></summary>
  
  ![installation](https://raw.githubusercontent.com/sazhiromru/images/refs/heads/main/ibet/vnc_server_install.PNG)
  ![start VNC](https://github.com/sazhiromru/images/blob/main/ibet/VNCserver_started.PNG?raw=true)
  ![VNC to localhost](https://github.com/sazhiromru/images/blob/main/ibet/localhost_vnc.PNG?raw=true)
  ![Real VNC](https://github.com/sazhiromru/images/blob/main/ibet/connectin_real_vncPNG.PNG?raw=true)
  ![remote desktop](https://github.com/sazhiromru/images/blob/main/ibet/remote_desktop_vnc.PNG?raw=true)
</details>  

<br>


<br>
