from crawiling_naver import NaverCrawler
from proxy_manager import ProxyManager
import pandas as pd
import os

if __name__ == "__main__":
    proxy_manager = ProxyManager(
        proxy_id="SuperVIPZHGIELE",
        proxy_pw="TMO3VCr1",
        proxy_txt_path="./config_data/proxy_http_ip.txt",
        block_cnt=5,
        item_id_txt_path="./config_data/item_ids.csv",
        max_retry_cnt=3,
        curl_format="https://new.land.naver.com/houses?articleNo={}",
        plugin_file_path="./config_data/proxy_auth_plugin.zip",
        min_time=2,
        max_time=8,
    )
    crawler = NaverCrawler(
        min_time=2,
        max_time=8,
        proxy_manager=proxy_manager
    )
    
    # 설정값 입력
    area_name = "seoul"
    csv_path = r"D:\Kernel360_final_project\crawling\crawled_data\naver_seoul_v1\coordinates.csv"
    data_dir = r"D:\Kernel360_final_project\crawling\crawled_data\naver_seoul_v1"
    
    view = 14
    coordinates = pd.read_csv(csv_path)

    for i in range(len(coordinates)):

        if i != 7:
            continue
        
        lat_idx = coordinates.loc[i, 'lat_idx']
        lon_idx = coordinates.loc[i, 'lon_idx']
        area_id = f"{area_name}_{lat_idx}_{lon_idx}"
        lat = coordinates.loc[i, 'latitude']
        lon = coordinates.loc[i, 'longitude']

        # 매물 id 크롤링
        crawler.crawl_item_ids(data_dir, area_id, lat, lon, view)

        # 중복 매물 id 제거
        crawler.check_duplicate_property(
            item_id_csv_path=os.path.join(data_dir, "id_list", area_id + ".csv"),
            lat_idx=lat_idx,
            lon_idx=lon_idx,
            property_list_dir=os.path.join(data_dir, "property_list"),
            area_name=area_name
        )

        # 매물 상세 정보 크롤링
        crawler.crawl_property_datail(data_dir, area_id)