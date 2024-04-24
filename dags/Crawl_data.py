import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import threading
from datetime import datetime,timedelta
from selenium.common.exceptions import TimeoutException
from sqlalchemy import create_engine,text
from cassandra.cluster import Cluster
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as sf
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from selenium .webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent


engine = create_engine('mysql+mysqlconnector://root:1234@103.130.215.35:3307/BDS')
# engine.connect()
def get_driver():
    user_agent = UserAgent()
#get a random user agent
    user_string = user_agent.random
    opts = uc.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument(f'--user-agent={user_string}')
    # opts.add_argument("--headless=new")
    # opts.headless = True
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-client-side-phishing-detection")
        # disable popups on startupscl
    driver = uc.Chrome(options=opts,version_main=122,headless=True)
    return driver


# creat spark session
spark = SparkSession.builder.config("spark.driver.memory", "8g").config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
                                                                        .config("spark.cassandra.connection.host", "192.168.2.13")\
                                                                        .config("spark.cassandra.auth.username", "cassandra")\
                                                                        .config("spark.cassandra.auth.password", "cassandra") \
                                                                        .getOrCreate()

## address in HN
Hn=['Hoàn Kiếm', 'Đống Đa', 'Ba Đình', 'Hai Bà Trưng', 'Hoàng Mai', "Thanh Xuân", 'Long Biên', 'Nam Từ Liêm', 'Bắc Từ Liêm', 'Tây Hồ', 'Cầu Giấy', 'Hà Đông',
    "Ba Vì", 'Chương Mỹ', 'Phúc Thọ', 'Đan Phượng', 'Đông Anh', 'Gia Lâm', 'Hoài Đức', 'Mê Linh', 'Mỹ Đức', 'Phú Xuyên', 'Quốc Oai', 'Sóc Sơn', 'Thạch Thất', 'Thanh Oai', 'Thường Tín', 'Ứng Hòa', 'Thanh Trì',' Sơn Tây']

## address in HCM
hcm=['Quận 1',' Quận 3', 'Quận 4', 'Quận 5',' Quận 6', 'Quận 7', 'Quận 8', 'Quận 10', 'Quận 11', 'Quận 12', 'Tân Bình', 'Bình Tân', 'Bình Thạnh', 'Tân Phú','Gò Vấp', 'Phú Nhuận','Bình Chánh', 'Hóc Môn', 'Cần Giờ', 'Củ Chi', 'Nhà bè','Thủ Đức']


## URL crawl_data
hcm_url = 'https://batdongsan.com.vn/nha-dat-ban-tp-hcm/p{}?sortValue=1'
hn_url = 'https://batdongsan.com.vn/nha-dat-ban-ha-noi/p{}?sortValue=1'
hcm_rent_url = 'https://batdongsan.com.vn/nha-dat-cho-thue-tp-hcm/p{}?sortValue=1'
hn_rent_url = 'https://batdongsan.com.vn/nha-dat-cho-thue-ha-noi/p{}?sortValue=1'
    





def wirte_Data_To_Cassandra(df,table_name,keyspace_name):
    df.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace_name).mode("append").save()


def write_Data_To_MySQL(df,engine,table_name):
    df.to_sql(name=table_name,con=engine,if_exists='append',index=False)


def read_Data_From_Cassandra(table_name,keyspace_name):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace_name).load().where(sf.col('date')== str(datetime.now().date()))
    return data

def connect_Cassandra(keyspace1,url):
    file_name=url.split('/')[3].replace('-','_')
    file_name=file_name.lower()
    data=spark.read.format("org.apache.spark.sql.cassandra").options(table=file_name, keyspace='bds').load()
    check_day=data.count()
    if check_day == 0:
        check_Database_Emty=True
    else:
        check_Database_Emty=False
    return  check_Database_Emty


def process_pages(thread_id, num_threads, total_pages, data_lock, property_data,driver,url,check_database):
    import time
    last_successful_page = thread_id - num_threads
    while last_successful_page <= total_pages:
        try:
            for page_num in range(last_successful_page + num_threads, total_pages + 1, num_threads):
                print(page_num)
                if page_num == 1:
                    if url == hcm_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-ban-tp-hcm")
                    elif url == hn_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-ban-ha_noi")   
                    elif url == hn_rent_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-cho-thue-ha-noi")
                    elif url == hcm_rent_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-cho-thue-tp-hcm")
                                           
                    dropdown_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, ".js__bds-select-button"))
                    )
                    dropdown_button.click()
                    newest_option = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'js__option')][@vl='1']"))
                    )
                    newest_option.click()
                else:
                    driver.get(url.format(page_num))
                
                
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                topics = soup.find_all(class_="re__card-info")
                exit_while = False
                
                page_data = []
                for topic in topics:
                    # print(topic.find('span',class_='re__card-published-info-published-at'))
                    title = topic.find('span', class_='pr-title js__card-title').get_text(strip=True) if topic.find('span', class_='pr-title js__card-title') else ''
                    address = topic.find('div', class_='re__card-location').find('span').get_text(strip=True) if topic.find('div', class_='re__card-location') else ''
                    district = address.split(',')[0].strip()
                    price  = topic.find(class_='re__card-config-price js__card-config-item').get_text(strip=True) if topic.find(class_='re__card-config-price js__card-config-item') else ''        
                    area  = topic.find(class_='re__card-config-area js__card-config-item').get_text(strip=True) if topic.find(class_='re__card-config-area js__card-config-item') else ''        
                    price_per_m2  = topic.find(class_='re__card-config-price_per_m2 js__card-config-item').get_text(strip=True) if topic.find(class_='re__card-config-price_per_m2 js__card-config-item') else ''
                    try:
                        bedrooms  = topic.find(class_='re__card-config-bedroom js__card-config-item')['aria-label'] 
                    except:
                        bedrooms = ''
                    try:
                        toilets  = topic.find(class_='re__card-config-toilet js__card-config-item')['aria-label']
                    except:
                        toilets =''
                    try:
                        day = topic.find('span',class_='re__card-published-info-published-at')['aria-label']
                    except:
                        continue
                    
                    #Lấy dữ liệu lần đầu thì command điều kiện if này
                    if check_database== False:
                        date_obj = datetime.strptime(day, "%d/%m/%Y").date()
                        Test_address=address.split(',')[-1].strip()
                        if date_obj != datetime.now().date() and (Test_address == 'Hà Nội' or Test_address == 'Hồ Chí Minh'):
                            exit_while = True
                            break
                    
                    page_data.append([title, district, price, area, price_per_m2, bedrooms, toilets, day])
                    
                    last_successful_page = page_num 
                


                # Lock to safely update the shared data structure
                with data_lock:
                    property_data.extend(page_data)

                if exit_while:
                    driver.quit()
                    break
                        
        except TimeoutException:
            print(f"Timeout occurred in thread {thread_id}")
            driver.quit()
            print("Restarting the driver in 5 seconds")
            time.sleep(5)
            driver=get_driver()
            
        else:
            driver.quit()
            break    

def Write_data_csv(df,file_name):
    df.repartition(1).write.csv(file_name,header=True)



def Crawl_data(url):

        columns_headers = ['title', 'address', 'price', 'area', 'price_per_m2', 'bedrooms', 'toilets', 'author', 'date']
        ## connect mysql
        # engine,check_database=connect_My_SQL('root','1234','localhost','3307','BDS',url.split('/')[3].replace('-','_'))
        check_cassandra=connect_Cassandra('bds',url)
        print(check_cassandra)

        #Get total_pages in website
        url_new=url.replace('/p{}','')
        # print(url_new)
        driver = get_driver()
        driver.get(url_new)
        # total_pages=[]
        # for i in driver.find_elements(By.CSS_SELECTOR, 'a.re__pagination-number'):
        #     total_pages.append(i.get_attribute('pid'))
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        total_pages = soup.find_all(class_="re__pagination-number")[-1].get_text()
        total_pages=total_pages.replace('.','')
        total_pages=int(total_pages)
        driver.quit()
        

        property_data = []
        data_lock = threading.Lock()

        # Setup Chrome options for undetected_chromedriver

        num_threads = 2 # Number of chrome open

        threads = []
        for thread_id in range(1,num_threads + 1):
            driver=get_driver()
          
            t = threading.Thread(target=process_pages, args=(thread_id, num_threads, total_pages, data_lock, property_data,driver,url,check_cassandra))
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()

        ## create pyspark dataframe
        df=spark.createDataFrame(property_data,schema='title string, address string, price string, area string, price_per_m2 string, bedrooms string, toilets string, date string')
        df = df.withColumn("date", sf.to_date(df["date"], "dd/MM/yyyy").cast(DateType()))
        df=df.withColumn('id',sf.expr('uuid()'))
        df = df.filter(df.id. isNotNull())
        file_name='C:/Users/029at/Desktop/BDS_CODE/Crawl_data/'+ url.split("/")[3] + ' ' + str(datetime.now().date())

        ## write data to csv
        # Write_data_csv(df,file_name)

        # # # Write data to Cassandra
        string_name=url.split('/')[3].replace('-','_')
        wirte_Data_To_Cassandra(df,string_name.lower(),'bds')

        print('Sucessfull crawl_data {}'.format(url.split('/')[3].replace('-','_')))


   
def ETL_rent(filename,address):
            import numpy as np
            ## Extract data from cassandra
            # df=spark.read.format("org.apache.spark.sql.cassandra").options(table=filename, keyspace="raw_bds").load()
            df=read_Data_From_Cassandra(filename,'bds')


            ## Processing data
            df=df.toPandas()
            df=df.loc[df['address'].isin(address)]
            df.dropna(subset=['title'],inplace=True)
            df.dropna(subset=['price'],inplace=True)
            df.dropna(subset=['date'],inplace=True)
            df.drop('price_per_m2',axis=1,inplace=True)
            df['price'] = df['price'].astype(str)
            df['area'] = df['area'].astype(str)
            df['title'] = df['title'].astype(str)
            df=df.drop_duplicates(subset=['title','date'], keep='last')
            df=df[(df['bedrooms'].str.contains('Phòng ngủ')==True) | (df['bedrooms'].isna()==True)]
            df=df[(df['toilets'].str.contains('WC')==True) | (df['toilets'].isna()==True)]
            df['bedrooms'] = df['bedrooms'].astype(str) 
            df['toilets'] = df['toilets'].astype(str) 

            def drop_title(value):
                value = value.replace('\n', '').replace('t', '')
                return value

            def convert_price(value):
                value = value.strip().replace(' ', '').replace('.', '').replace(',', '.')
                if 'triệu/tháng' in value:
                    return float(value.replace('triệu/tháng', '')) 
                elif 'nghìn/tháng' in value and '/m²' not in value: 
                    return float(value.replace('nghìn/tháng', '')) / 1000
                elif value.lower() == 'giá thoả thuận':
                    return np.nan
                else:
                    return np.nan
            
            def convert_area(value):
                value = value.strip()
                value = value.replace('.', '').replace(',', '.').replace(' ', '') 
                if 'm²' in value:
                    return float(value.replace('m²', ''))
                else:
                    return np.nan

            def convert_to_number(value):
                try:
                    number = value.split()[0]  
                    return int(number)
                except ValueError:
                    return np.nan


            def extract_keyword(title):
                title_lower = title.lower()
                if "nhà" in title_lower or "biệt thự" in title_lower or "tầng" in title_lower:
                    return "nhà ở"
                elif "chung cư" in title_lower or "căn" in title_lower or "penthouse" in title_lower or "chcc" in title_lower:
                    return "chung cư"
                elif "đất" in title_lower or "lô" in title_lower or "mặt đường" in title_lower:
                    return "đất"
                elif "phòng trọ" in title_lower:
                        return "phòng trọ"
                else:
                    return "Khác"  
                
            df['title']=df['title'].apply(drop_title)
            df['price'] = df['price'].apply(convert_price)
            df['area'] = df['area'].apply(convert_area)
            df['bedrooms'] = df['bedrooms'].apply(convert_to_number)
            df['toilets'] = df['toilets'].apply(convert_to_number)
            df['type'] = df['title'].apply(extract_keyword)

            ## load data to Mysql
            write_Data_To_MySQL(df,engine,filename)

            print('Sucessfull ETL_data {}'.format(filename))


    
    ## processing buy 
def ETL_Buy(filename,address):
        import numpy as np
        ## extract data from cassandra
        df=read_Data_From_Cassandra(filename,'bds')
        # df=spark.read.format("org.apache.spark.sql.cassandra").options(table=filename, keyspace="raw_bds").load()

        ## Processing data
        df=df.toPandas()
        df=df.loc[df['address'].isin(address)]
        df.dropna(subset=['title'],inplace=True)
        df.dropna(subset=['price'],inplace=True)
        df.dropna(subset=['date'],inplace=True)
        df['price'] = df['price'].astype(str)
        df['area'] = df['area'].astype(str)
        df['price_per_m2'] = df['price_per_m2'].astype(str) 
        df['title'] = df['title'].astype(str)
        df=df.drop_duplicates(subset=['title','date'], keep='last')
        df=df[(df['bedrooms'].str.contains('Phòng ngủ')==True) | (df['bedrooms'].isna()==True)]
        df=df[(df['toilets'].str.contains('WC')==True) | (df['toilets'].isna()==True)]
        df['bedrooms'] = df['bedrooms'].astype(str) 
        df['toilets'] = df['toilets'].astype(str) 

        def convert_price(value):
            value = value.strip().replace(' ', '').replace('.', '').replace(',', '.')
            if 'tỷ' in value:
                return float(value.replace('tỷ', ''))
            elif 'triệu' in value and '/m²' not in value: 
                return float(value.replace('triệu', '')) / 1000
            elif value.lower() == 'giá thoả thuận':
                return np.nan
            else:
                return np.nan
            

        def drop_title(value):
            value = value.replace('\n', '').replace('t', '')
            return value
        
        
        def convert_area(value):
            value = value.strip()
            value = value.replace('.', '').replace(',', '.').replace(' ', '') 
            if 'm²' in value:
                return float(value.replace('m²', ''))
            else:
                return np.nan
            
        def convert_price_per_m2(value):
            if 'tr/m²' in value:
                return float(value.strip().replace('.', '').replace(',', '.').replace(' ', '').replace('tr/m²', ''))
            elif 'nghìn/m²' in value:
                return float(value.strip().replace('.', '').replace(',', '.').replace(' ', '').replace('nghìn/m²', ''))
            else:
                return np.nan

        def convert_to_number(value):
            try:
                number = value.split()[0]  
                return float(number)
            except ValueError:
                return np.nan
            
        def update_price_per_m2(row):
            if pd.isna(row['price']) or pd.isna(row['area']) or row['area'] == 0:
                return row['price_per_m2']
            
            calculated_price_per_m2 = round((row['price'] * 1000)/ row['area'], 2)
            
            if pd.isna(row['price_per_m2']) or row['price_per_m2'] != calculated_price_per_m2:
                return calculated_price_per_m2
            else:
                return row['price_per_m2']

        def extract_keyword(title):
            title_lower = title.lower()
            if "nhà" in title_lower or "biệt thự" in title_lower or "tầng" in title_lower:
                return "nhà ở"
            elif "chung cư" in title_lower or "căn" in title_lower or "penthouse" in title_lower or "chcc" in title_lower:
                return "chung cư"
            elif "đất" in title_lower or "lô" in title_lower or "mặt đường" in title_lower:
                return "đất"
            else:
                return "Khác"  
            
        df['title']=df['title'].apply(drop_title)    
        df['price'] = df['price'].apply(convert_price)
        df['area'] = df['area'].apply(convert_area)
        df['price_per_m2'] = df['price_per_m2'].apply(convert_price_per_m2)
        df['bedrooms'] = df['bedrooms'].apply(convert_to_number)
        df['toilets'] = df['toilets'].apply(convert_to_number)
        df['price_per_m2'] = df.apply(update_price_per_m2,axis=1)
        df['type'] = df['title'].apply(extract_keyword)

        ## load data to mysql
        write_Data_To_MySQL(df,engine,filename)

        print('Sucessfull ETL_data {}'.format(filename))
    

default_args = {
    'owner': 'TeamUDPTDLTM',
    'start_date': pendulum.datetime(2024, 3, 17,tz='Asia/Ho_Chi_Minh'),
    'email': ['tqbao05.it@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5)
}
with DAG(
       dag_id="BDS",
       default_args=default_args,
       description="Final project in HCMUS",
       schedule_interval='30 19 * * *'
) as dag:
    task1=PythonOperator(
        task_id='Crawl_data_Buy_in_hcm',
        python_callable=Crawl_data,
        op_kwargs={'url':hcm_url},
        dag=dag
    )
    task2=PythonOperator(
        task_id='Craw_data_Buy_in_hn',
        python_callable=Crawl_data,
        op_kwargs={'url':hn_url},
        dag=dag
    )
    task3=PythonOperator(
        task_id='Craw_data_rent_in_hcm',
        python_callable=Crawl_data,
        op_kwargs={'url':hcm_rent_url},
        dag=dag
    )
    task4=PythonOperator(
        task_id='Craw_data_rent_in_hn',
        python_callable=Crawl_data,
        op_kwargs={'url':hn_rent_url},
        dag=dag
    )
    task5=PythonOperator(
        task_id='ETL_Buy_in_HCM',
        python_callable=ETL_Buy,
        op_kwargs={'filename':'nha_dat_ban_tp_hcm','address':hcm},
        dag=dag
    )
    task6=PythonOperator(
        task_id='ETL_Buy_in_HN',
        python_callable=ETL_Buy,
        op_kwargs={'filename':'nha_dat_ban_ha_noi','address':Hn},
        dag=dag
    )
    task7=PythonOperator(
        task_id='ETL_rent_in_HCM',
        python_callable=ETL_rent,
        op_kwargs={'filename':'nha_dat_cho_thue_tp_hcm','address':hcm},
        dag=dag
    )
    task8=PythonOperator(
        task_id='ETL_rent_in_HN',
        python_callable=ETL_rent,
        op_kwargs={'filename':'nha_dat_cho_thue_ha_noi','address':Hn},
        dag=dag
    )
    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8

