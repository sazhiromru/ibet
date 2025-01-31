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
    client = clickhouse_connect.get_client(host='10.140.0.7', port=8123, username='default', password='Qwer3asdf')
    client.insert('ibet.results_date', [(datetime.now(),)], column_names='date')

#Загружаем напрямую в CLickhouse

def upload(extracted_data):
    client = clickhouse_connect.get_client(host='10.140.0.7', port=8123, username='default', password='Qwer3asdf')
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
<a id="wrangling-section"></a>
## ~~~ 2. Обработка данных ~~~
--- 


### Merge  

Внешним слиянием соединяем три csv, округялем цифры, приводим валюту к доллару, убираем дубликаты, ошибки и находим самые выгодные сделки по продаже китай->рф и рф->китай
<details>
  <summary><strong>📜 Merge </strong></summary>

```python
import pandas as pd
import numpy as np
from datetime import datetime

timestamp = datetime.now().strftime('%d-%m-%Y')

path_c5 = f'c5game_{timestamp}.csv'
path_market = f'market_{timestamp}.csv'
path_buff = f'buff_{timestamp}.csv'
path_buff_buyorders = f'buff_buyorders_{timestamp}.csv'

#автоматического обновления курса нет, но курс существенно юань/доллар не меняется много лет. 
cny_usd = 0.14
profit_coef = 0.9025

df_c5 = pd.read_csv(path_c5, encoding = 'utf-16')
df_c5.drop_duplicates()
df_c5.rename(columns = {'c5_item':'Item'},inplace = True)
#Создан фрейм с5 с колонками 'Item', 'c5_price'

df_buff_buyorders = pd.read_csv(path_buff_buyorders, encoding = 'utf-16')
df_buff_buyorders.rename(columns = {'buff_item':'Item', 'buff_price':'price'}, inplace = True)
#Создан фрейм buff_buyorders с колонками 'Item', 'buyorders_price'

df_market = pd.read_csv(path_market, encoding = 'utf-16')
#Создан фрейм market с колонками 'Item', 'market_price'

df_buff = pd.read_csv(path_buff, encoding = 'utf-16')
df_buff.rename(columns = {'buff_item':'Item'},inplace = True )
#Создан фрейм buff с колонками 'Item', 'buff_price'

df_final = pd.merge(df_buff, df_market, on = 'Item', how = 'outer')
df_final = pd.merge(df_final, df_c5, on = 'Item', how = 'outer')
# внешний мердж

df_final['c5_price'] =  df_final['c5_price'].astype(float).fillna(0)
df_final['buff_price'] =  df_final['buff_price'].astype(float).fillna(0)
df_final['market_price'] =  df_final['market_price'].astype(float).fillna(0)

df_final['buff_price'] = df_final['buff_price'].astype(float).apply(lambda x : x*cny_usd)
df_final['c5_price'] = df_final['c5_price'].astype(float).apply(lambda x : x*cny_usd)
# выполнены округление и очистка, приведение йен к долларам

'''Сравнение цен по продаже китай -> маркет, выбор лучших сделок на c5game and buff163'''
df_direct = df_final.copy(deep = True)

df_direct['market_price'] = df_direct['market_price'].astype(float).apply(lambda x: x*profit_coef)
df_direct['market_c5'] = (df_direct['market_price']/df_direct['c5_price'])
df_direct['market_buff'] = (df_direct['market_price']/df_direct['buff_price'])
df_direct.replace([np.inf, -np.inf], 0, inplace=True)

df_direct['best_price'] = np.maximum(df_direct['market_buff'], df_direct['market_c5'])
df_direct['label'] = np.where(df_direct['market_buff']>df_direct['market_c5'],'buff','c5')
df_direct = df_direct.sort_values(by = 'best_price', ascending=False)
#

columns_to_round = ['c5_price', 'market_price', 'buff_price', 'market_c5', 'market_buff', 'best_price']
df_direct[columns_to_round] = df_direct[columns_to_round].round(4)
df_direct['Item'] = df_direct['Item'].str.strip()
df_direct.drop_duplicates(subset=['Item'], inplace=True)
df_direct.reset_index(drop=True, inplace=True)

df_direct.drop(columns=['Date','Date_x','Date_y'], inplace=True)
df_direct['Date'] = timestamp

df_direct.drop_duplicates(inplace=True)

df_direct.to_csv(f'direct_{timestamp}.csv',index = False, encoding = 'utf-16')

'''Сравнение цен по продаже маркет -> китай, выбор лучших сделок по ордерам на покупку на бафф'''
df_reverse = pd.merge(df_market, df_buff_buyorders, on = 'Item', how = 'outer')
df_reverse = df_reverse.fillna(0)
df_reverse['buyorders_price'] = df_reverse['buyorders_price'].astype(float).apply(lambda x: x*cny_usd)
df_reverse['coef'] = df_reverse['buyorders_price'] / df_reverse['market_price']
df_reverse.replace(np.inf,0, inplace = True)
df_reverse.sort_values(by = 'coef', ascending=False, inplace = True)

df_reverse.drop(columns=['Date_x','Date_x'], inplace=True)
df_reverse['Date'] = timestamp
df_reverse.drop_duplicates(inplace = True)

df_reverse.to_csv(f'reverse_{timestamp}.csv',index = False, encoding = 'utf-16')
```
</details>  
<br>
