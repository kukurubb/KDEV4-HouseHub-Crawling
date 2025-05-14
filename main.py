from crawiling_naver import NaverCrawler
from proxy_manager import ProxyManager
import pandas as pd
import os
import argparse
import time as t

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "crawling_mode",
        type=str,
        choices=["proxypool", "default"],
        help="크롤링 모드를 선택하세요: proxypool / default"
    )
    args = parser.parse_args()

    if args.crawling_mode == "proxypool":
        proxy_manager = ProxyManager(
            proxy_csv_path="./config_data/proxy.csv",
            block_cnt=5,
            item_id_txt_path="./config_data/item_ids.csv",
            max_threads=6,
            max_retry_cnt=3,
            curl_format="https://new.land.naver.com/houses?articleNo={}",
            plugin_file_path_format="./config_data/{}_proxy_auth_plugin.zip",
            min_time=2,
            max_time=8,
        )
    elif args.crawling_mode == "default":
        proxy_manager = None

    crawler = NaverCrawler(
        min_time=2,
        max_time=8,
        print_log_cnt=50,
        proxy_manager=proxy_manager
    )
    
    # 설정값 입력
    area_name = "seoul"
    csv_path = r"D:\Kernel360_final_project\crawling\crawled_data\naver_seoul_v1\coordinates.csv"
    data_dir = r"D:\Kernel360_final_project\crawling\crawled_data\test3"
    
    view = 15
    coordinates = pd.read_csv(csv_path)

    for i in range(len(coordinates)):

        if i != 2:
            continue
        
        lat_idx = coordinates.loc[i, 'lat_idx']
        lon_idx = coordinates.loc[i, 'lon_idx']
        area_id = f"{area_name}_{lat_idx}_{lon_idx}"
        lat = coordinates.loc[i, 'latitude']
        lon = coordinates.loc[i, 'longitude']

        # # 매물 id 크롤링
        # crawler.crawl_item_ids(
        #     data_dir,
        #     area_id,
        #     lat, lon, view,
        #     max_threads=5
        # )

        # # 중복 매물 id 제거
        # crawler.check_duplicate_property(
        #     item_id_csv_path=os.path.join(data_dir, "id_list", area_id + ".csv"),
        #     lat_idx=lat_idx,
        #     lon_idx=lon_idx,
        #     property_list_dir=os.path.join(data_dir, "property_list"),
        #     area_name=area_name
        # )

        start_t = t.time()

        # 매물 상세 정보 크롤링
        crawler.crawl_property_datails(
            data_dir,
            area_id,
            max_threads=10
        )

        end_t = t.time()
        test_t = end_t - start_t

        print(f"소요시간: {test_t}")