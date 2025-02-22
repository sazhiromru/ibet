
---
<a id="ibet-scrape"></a>
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
<a id="ibet-merge"></a>
## ~~~ 2. Обработка данных ~~~
--- 


### Merge  

По live ставкам данные поступают через REDIS от трех инстансев на координатор.

Цель быстро сопоставить события и найти коэффиценты, которые максимально отличаются.

Сложность в транлитерации между кририллицей и иностранными букмекерами + все собирают названия различных команд и лиг по разному.

Решение - в транслитерации и сопоставлению названий команд/игроков отдельно по словам и по буквам специальными отдельными функциями.

Далее загружаем данные в CLickhouse через Kafka

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

---
<a id="ibet-VNC"></a>
## ~~~ 3. VNC ~~~
--- 

Сайты используют Google Capthca, в среднем раз в сутки.
Обойти это невозможно, единственный способ - пройти ее раз в сутки.
Для этого используем VNC

Устанавливаем XFCE4 в качестве рабочего стола, gnome icon для иконок и tightvnc для отображения.
RealVNC для соединения с удаленным рабочим столом
Так же создаем профиль для браузера

<details>
  <summary><strong>🖼️ VNC </strong></summary>
  
  ![installation](https://raw.githubusercontent.com/sazhiromru/images/refs/heads/main/ibet/vnc_server_install.PNG)
  ![start VNC](https://github.com/sazhiromru/images/blob/main/ibet/VNCserverstarted.PNG?raw=true)
  ![VNC to localhost](https://github.com/sazhiromru/images/blob/main/ibet/localhost_vnc.PNG?raw=true)
  ![Real VNC](https://github.com/sazhiromru/images/blob/main/ibet/connectin_real_vnc.PNG?raw=true)
  ![remote desktop](https://github.com/sazhiromru/images/blob/main/ibet/remote_desktop_vnc.PNG?raw=true)
</details>  

<br>

---
<a id="ibet-redis"></a>
## ~~~ 4. Redis ~~~
--- 
Базовая настройка работы Redis - самое простое действие. В нашем случае он выступает как быстрый брокер сообщений. Гарантия доставки нам в данном случае не нужна, а скорость и ресурсоемкость - очень пригодятся

- установка через apt, далее redis.conf
- отключаем protectionmode
- ставим пароль
- выбираем политику управления памятью
- прописываем ip через bind
- выбираем объем оперативной памяти который redis может занимать под сообщения
- и все готово!
Учитывая низкую загрузку, нам подойдет любая политика, но по логике выбираем FIFO через настройку allkeys-lru

<details>
  <summary><strong>🖼️ Redis </strong></summary>
  
  ![settings](https://github.com/sazhiromru/images/blob/main/ibet/redis%20setting%20right%20screen.PNG?raw=true)
  ![redis bind](https://github.com/sazhiromru/images/blob/main/ibet/redis_bind.PNG?raw=true)

</details>  

<br>
<br>

---
<a id="ibet-kafka"></a>
## ~~~ 5. Kafka ~~~
--- 
Kafka - гораздо сложнее. Ключ здесь - по шагам следовать документации.
Устанавливаем, распаковываем через tar, создаем кластер, записываем его ID, форматируем директорию логов специальной командой, и запускаем сервер.
Дальше самое интересное, настроить работу и взаимодействие двух серверов через KRAFT.
Примеров по этому не так много (вообще почти нет), приведу свой конфиг:

```properties
process.roles=broker,controller
node.id=1
listener.security.protocol=PLAINTEXT
listener.security.protocol.map=BROKER:PLAINTEXT, CONTROLLER:PLAINTEXT
listeners=BROKER://10.140.0.7:9092,CONTROLLER://10.140.0.7:9093
advertised.listeners=BROKER://10.140.0.7:9092
log.retention.hours=24
num.partitions=1
default.replication.factor=1
log.segment.bytes=1073741824
log.retention.bytes=1073741824
kafka.cluster.id=L-bJf_UEQ6ioPQQhc-IZPg
log.dirs=/opt/kafka/logs
controller.quorum.voters=1@10.140.0.7:9093,2@10.140.0.2:9093
controller.listener.names=CONTROLLER
inter.broker.listener.name=BROKER
```

Каждый сервер благодаря kraft может быть и брокером сообщений, и контроллером для управления метаданными.  

Коротко - создаем alias для Broker и Controller. В данном случае они на одном сервере но разных портах.  

У нас минимальная конфигурация, и нет ни разделения ни репликации.  

Папка логов так же задается здесь, и к ней надо дать право записи kafka либо поменять владельца на kafka.  

Через listener.security.protocol.map=BROKER:PLAINTEXT, CONTROLLER:PLAINTEXT задаем протокол шифрования, и у нас его нет plaintext - обычный текст. Advertised.listeners - это адрес по которому находят наш сервер.  
В кворум вносим оба Нода.   

Для второго нода настройки почти аналогичные, меняются ip и номер node

<br>
<br>

---
<a id="ibet-clickhouse"></a>
## ~~~ 6. ClickHouse ~~~
---   

Запуск и настройка Clickhouse для стабильной работы на сервере с двумя ядрами и 4гб памяти - целая эпопея.
Примеров нет, есть несколько гайдов, но там никак не регулируются ни threads ни ядра
Суть - сильно ограничить использования ядер, памяти и сделать эти настройки согласованными:

```xml
<!-- Ограничение на число параллельных запросов -->
<max_concurrent_queries>16</max_concurrent_queries>

<max_server_memory_usage>3221225472</max_server_memory_usage>
<max_thread_pool_size>10000</max_thread_pool_size>

<!-- Настройки потоков -->
<background_buffer_flush_schedule_pool_size>1</background_buffer_flush_schedule_pool_size>
<background_pool_size>2</background_pool_size>
<background_merges_mutations_concurrency_ratio>1</background_merges_mutations_concurrency_ratio>
<background_merges_mutations_scheduling_policy>round_robin</background_merges_mutations_scheduling_policy>
<background_move_pool_size>1</background_move_pool_size>
<background_fetches_pool_size>1</background_fetches_pool_size>
<background_common_pool_size>2</background_common_pool_size>
<background_schedule_pool_size>1</background_schedule_pool_size>
<background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
<background_distributed_schedule_pool_size>1</background_distributed_schedule_pool_size>

<!-- Загрузка таблиц -->
<tables_loader_foreground_pool_size>0</tables_loader_foreground_pool_size>
<tables_loader_background_pool_size>0</tables_loader_background_pool_size>
<async_load_databases>false</async_load_databases>

<max_server_memory_usage_to_ram_ratio>1</max_server_memory_usage_to_ram_ratio>

<!-- Настройки хранения данных -->
<storage_configuration>
    <disks>
        <default>
            <keep_free_space_bytes>1073741824</keep_free_space_bytes>
        </default>
    </disks>
</storage_configuration>
```

С ЭТИМИ НАСТРОЙКАМИ CLICKHOUSE СЛОМАЕТСЯ!  

  ![Конфликт с настройками MergeTree](https://github.com/sazhiromru/images/blob/main/ibet/clickhouse_error.PNG?raw=true)  
  
Нужно отдельно разобраться в логах с причинами, и найти в документации, что такое снижение ядер конфликтует с настройками движка MergeTree по умолчанию, и их надо отдельно добавить из документации:

```xml
    <merge_tree>
        <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
        <number_of_free_entries_in_pool_to_execute_mutation>1</number_of_free_entries_in_pool_to_execute_mutation>
        <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>1</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
        <number_of_free_entries_in_pool_to_lower_max_size_of_merge>1</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
    </merge_tree>
```
Далее, для сохранения данных через Kafka требуется три таблицы в clickhouse:
- таблица для приема данных через KafkaEngine

```sql
CREATE TABLE kafka
(
    Event String,
    Category String,
    Subcategory String,
    Stavka String,
    F_T String,
    Coef1 Float64,
    Coef2 Float64,
    Platform String,
    Ratio Float64,
    Timestamp Float64,
    Timestamp_2 Float64
) ENGINE = Kafka
(
    '10.140.0.7:9092',
    'ibet',
    'group1',
    'JSONEachRow'
);
```

- Материальный вид который пересохраняет данные из KafkaEngine в постоянную таблицу
```sql

CREATE MATERIALIZED VIEW saver TO stavki AS
SELECT
    Event AS event,
    Category AS category,
    Subcategory AS subcategory,
    Stavka AS stavka,
    F_T AS f_t,
    Coef1 AS coef1,
    Coef2 AS coef2,
    Platform AS platform,
    Ratio AS ratio,
    toDateTime(CAST(Timestamp AS UInt32)) AS timestamp,
    toDateTime(CAST(Timestamp_2 AS UInt32)) AS timestamp_2,
    toInt32(toDateTime(CAST(Timestamp AS UInt32)) - toDateTime(CAST(Timestamp_2 AS UInt32))) AS time_delta
FROM kafka;
```
- Ну и финальная таблица
```sql
CREATE TABLE stavki (
    event String,
    category String,
    subcategory String,
    stavka String,
    f_t String,
    coef1 Float64,
    coef2 Float64,
    platform String,
    ratio Float64,
    timestamp DateTime,
    timestamp_2 DateTime,
    time_delta Int32 
 ) ENGINE = MergeTree()
ORDER BY timestamp;
```
<br>
<br>

---
<a id="ibet-postgres"></a>
## ~~~ 7. Postgres ~~~
---   
Нужно установить postgres, создать таблицу для airflow, добавить пользователя airflow, и дать ему права на внесение записей. Затем в конфиге airflow поменять базу данных на postgres.
ДАЖЕ ПРИ МИНИМАЛЬНОЙ НАГРУЗКЕ НЕОБХОДИМО СМЕНИТЬ БАЗУ ДАННЫХ AIRFLOW!  
С БД по умолчанию даже два dag перестают функционировать нормально, и связь с airflow scheduler постоянно теряется
<details>
  <summary><strong>🖼️ Postgres </strong></summary>
  
  ![settings](https://github.com/sazhiromru/images/blob/main/ibet/postgre%20airflow-setting.PNG?raw=true)
  ![airflow config](https://github.com/sazhiromru/images/blob/main/ibet/postgre_airflowsetup.PNG?raw=true)

</details>  

Так же, нужно скорректировать настройки pg_hba.conf чтобы локальные пользователи могли заходить без пароля, т е чтобы airflow работало без пароля
<details>
  <summary><strong>🖼️ Postgres conf </strong></summary>
  
  ![conf](https://github.com/sazhiromru/images/blob/main/ibet/postgres-airflow.PNG?raw=true)

</details>
<br>
<br>


---
<a id="ibet-airflow"></a>
## ~~~ 8. Airflow ~~~
---  
С Airflow, как и c Kafka, очень важно внимательно смотреть документацию. Программа способна удивлять, для глюков и багов есть специальный огромный раздел. Например, у вас может не срабатывать команда Bash при подключении по SSH. После суток поиска ответ находиться в документации - надо поставить после команды пробел. Просто так. Потому что потому. Так бывает, ставьте пробел - заработает.   
И ведь заработало. 
В общем:

- устанавливаем по документации
- подсоединяем базу данных postgres со всеми настройками описанными выше, и LocalExecutor
- создаем пользователя и отлключаем загрузку примеров
- создаем службу
- немного уменьшаем heartbeat в настройках чтобы снизить нагрузку на сервер - кстати помогло
- устанавливаем ssh-connect и создаем все неоьходимые соединения
- даем программе все папки которые она использует для логов и временных файлов
- создаем все необхоимые Dag файлы и настраиваем таймеры
И всё, мы готовы

<details>
  <summary><strong>🖼️ Airflow </strong></summary>
  
  ![installation](https://github.com/sazhiromru/images/blob/main/ibet/airflow_installation.PNG?raw=true)
  ![postgres](https://github.com/sazhiromru/images/blob/main/ibet/postgre_airflowsetup.PNG?raw=true)
  ![user](https://github.com/sazhiromru/images/blob/main/ibet/airflow_disable%20examples.PNG?raw=true)
  ![service](https://github.com/sazhiromru/images/blob/main/ibet/airflow_temp_solution.PNG?raw=true)
  ![conf](https://github.com/sazhiromru/images/blob/main/ibet/airflow_heartbeat.PNG?raw=true)
  ![log problem](https://github.com/sazhiromru/images/blob/main/ibet/airflow-private%20temp.PNG?raw=true)
  ![dags](https://github.com/sazhiromru/images/blob/main/ibet/airflow-dags.PNG?raw=true)

</details>  

<br>
<br>

---

<a id="ibet-bash"></a>
## ~~~ 9. GCP и BASH ~~~
---  
По работе с bash, нескольколько примеров:
- настройка ssh ключей и google cli
- отключение наследования в windows
- chown для записи логов программ
- настройка Firewal для корректного логина по ssh и работы с Grafana и Airflow-webserver
- редактирование visudo чтобы airflow мог запускать python без sudo
- установка переменных в .bashrc чтобы kafka и airflow работали
<details>
  <summary><strong>🖼️ Bash </strong></summary>
  
  ![ssh](https://github.com/sazhiromru/images/blob/main/ibet/bash_key_add.PNG?raw=true)
  ![google cli](https://github.com/sazhiromru/images/blob/main/ibet/google1.PNG?raw=true)
  ![редактирование ключа](https://github.com/sazhiromru/images/blob/main/heritage_disable.PNG?raw=true)
  ![logs](https://github.com/sazhiromru/images/blob/main/ibet/logs_dir.PNG?raw=true)
  ![service](https://github.com/sazhiromru/images/blob/main/ibet/service.PNG?raw=true)
  ![firewal](https://github.com/sazhiromru/images/blob/main/ibet/firewall.PNG?raw=true)
  ![visudo](https://github.com/sazhiromru/images/blob/main/ibet/airflow_visudo.PNG?raw=true)
  ![bashrc](https://github.com/sazhiromru/images/blob/main/ibet/bashrc.PNG?raw=true)

</details>  

<br>
<br>

---

<a id="ibet-grafana"></a>
## ~~~ 10. Grafana ~~~
---  

По Grafana показать особо нечего. Это очень своеобразная система, сильно отличается и от Metabase и тем более от Power BI.
Настраиваем все визуалы, делаем динамическое изменение цветов для нескольких параметров с помощью override, публикуем через share

<details>
  <summary><strong>🖼️ Grafana </strong></summary>
  
  ![grafana](https://github.com/sazhiromru/images/blob/main/ibet/grafana%20overrides.PNG?raw=true)


</details>  
Пример выше. В созданной таблице вместо числового столбца создаем буквенные через CASE, добавляем через override изменение цвета и mapping.
