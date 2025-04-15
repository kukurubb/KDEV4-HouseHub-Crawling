from crawiling_naver import NaverCrawler
import pandas as pd

if __name__ == "__main__":
    crawler = NaverCrawler(
        headless=False,
        min_time=2,
        max_time=10
    )
    
    # 설정값 입력
    csv_path = r"D:\Kernel360_final_project\crawling\crawled_data\naver_daejeon_v2\coordinates.csv"
    data_dir = r"D:\Kernel360_final_project\crawling\crawled_data\naver_daejeon_v2"
    
    view = 14
    coordinates = pd.read_csv(csv_path)

    for i in range(len(coordinates)):
        area_id = f"daejeon{i}"
        lat = coordinates.loc[i, 'latitude']
        lon = coordinates.loc[i, 'longitude']

        # 크롤링 수행
        crawler.crawl_item_ids(data_dir, area_id, lat, lon, view)