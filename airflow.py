import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Параметры по умолчанию
default_args = {
    'owner': 'e.krasnobaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.today(),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def e_krasnobaeva_2():
    
    @task(retries=3)
    def get_data(link):
        data = pd.read_csv(link)
        login = 'e-krasnobaeva'
        year = int(1994 + hash(f'{login}') % 23)
        df = data.query('Year == @year')
        return df
    
    @task(retries=3)
    def top_game(df):
        global_sales = df.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending=False)
        top_game = global_sales.iloc[0]['Name']
        return top_game
    
    @task(retries=3)
    def top_genre(df):
        max_eu_sales = df['EU_Sales'].max()
        genre_EU = df[df['EU_Sales'] == max_eu_sales]
        top_genre_EU = genre_EU['Genre'].unique().tolist()
        return top_genre_EU
    
    @task(retries=3)
    def top_na_platform(df):
        platform_na = df.query('NA_Sales > 1').groupby('Platform')['Name'].count()
        max_platform_count = platform_na.max()
        top_platform_NA = platform_na[platform_na == max_platform_count].index.tolist()
        return top_platform_NA
    
    @task(retries=3)
    def jp_publisher(df):
        publisher_JP = df.groupby('Publisher')['JP_Sales'].mean()
        max_publisher_JP = publisher_JP.max()
        top_publisher_JP = publisher_JP[publisher_JP == max_publisher_JP].index.tolist()
        return top_publisher_JP
    
    @task(retries=3)
    def game_eu_jp(df):
        count_game = df.query('EU_Sales > JP_Sales')['Name'].nunique()
        return count_game
    
    @task()
    def print_data(df, top_game, top_genre_EU, top_platform_NA, top_publisher_JP, count_game):
        year = df['Year'].iloc[0]
        print(f'Best selling game for {year}:\n{top_game}')
        
        print(f'Best selling genres in EU for {year}:')
        for genre in top_genre_EU:
            print(genre)
        
        print(f'Top platforms in NA for {year}:')
        for platform in top_platform_NA:
            print(platform)
        
        print(f'Publishers with highest average sales in JP for {year}:')
        for publisher in top_publisher_JP:
            print(publisher)

        print(f'Number of games that sold better in EU than in JP for {year}:\n{count_game} games')

    # Загрузка данных
    link = "https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv"
    df = get_data(link)
    
    # Вызов задач
    top_game_res = top_game(df)
    top_genre_res = top_genre(df)
    top_platform_res = top_na_platform(df)
    top_publisher_res = jp_publisher(df)
    count_game_res = game_eu_jp(df)
    
    # Печать результатов
    print_data(df, top_game_res, top_genre_res, top_platform_res, top_publisher_res, count_game_res)

# Создание DAG-а
e_krasnobaeva_2 = e_krasnobaeva_2()





