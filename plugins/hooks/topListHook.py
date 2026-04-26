# Hook
from airflow.hooks.base_hook import BaseHook

# Configs
from configs.payloads import Payload

# Utils
from utils.preprocessor import parse_areas

# Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

# Generals
import logging
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict
from itertools import count
import heapq


# setup logging format
logger = logging.getLogger(__name__)



class TopListHook(BaseHook):
    
    
    def __init__(
        self,
        urls,
        session,
    ):
        super().__init__()
        self.session = session        
        self.urls = urls
        
        self.data_container = defaultdict(list)
        self.counter = count()
        self.seen_products = defaultdict(set)
        
        
    @staticmethod
    def _score(
        adt_amt : str,
        dep_day : str
    ):
        price = int(adt_amt)
        dep = int(dep_day)
        return (-price, -dep)
        
        
        
    def getTravelAreas(self, url:str) -> list:
        """
            Get entire places and its code from international (travel-areas)
        """
        
        try:
            resp = self.session.get(
                url,
                timeout=10
            )
            
            resp.raise_for_status()
            data = resp.json().get('data', [])
            
            logger.info(
                "[GET SUCCESS] API [Travel-areas] responded !",
                extra={
                    'event' : "Travel-areas",
                    "condition" : "Success"
                }
            )
            
            return data
        
        except Exception as e:
            logger.error(
                "[GET FAILED] API [Travel-areas] not responded / error=%s",
                e,
                extra={
                    "event": "Travel-areas",
                    "condition" : "Fail"
                },
            )
            raise
        
        
        
    def get_product_info(
        self,
        major_url:str,
        product_url:str,
        area_info:dict,
        N:int,
        start_dep_day : datetime,
        end_dep_day : datetime
        ):
        
        code = "OKA" if area_info['code'] == 'J47' else area_info['code']
        isNation = area_info['type'] == 'N'
        
        payload = Payload().get_major_payload(
            area_type=area_info['type'],
            code=code,
            name=area_info['name'],
            startDepDay=start_dep_day,
            endDepDay=end_dep_day
        )
        
        try:
            resp = self.session.post(
                major_url,
                json=payload,
                timeout=10
            )
            
            resp.raise_for_status()
            logger.info(
                "[SUCCESS] POST major : %s",
                area_info['name'],
                extra={
                    'event' : "Major_product",
                    'condition' : "Success"
                }
            )
            
            products = resp.json().get('data', {}).get('products', [])
            
            for product in products:
                if any(
                    sticker.get("value") in ("에어텔", "자유여행", "허니문")
                    for sticker in product.get("sticker", [])
                ):
                    continue
                
                rprs_prod_cd = product['rprsProdCd']
                
                for page_num in range(1, 31):
                    
                    product_payload = Payload().get_product_payload(
                        area_type=area_info['type'],
                        code=code,
                        name=area_info['name'],
                        rprsProdCds=rprs_prod_cd,
                        startDepDay=start_dep_day,
                        endDepDay=end_dep_day,
                        page=page_num
                    )
                    
                    try:
                        _product_data = self.session.post(
                            product_url,
                            json=product_payload,
                            timeout=10
                        )
                        
                        _product_data.raise_for_status()
                        
                        product_data = _product_data.json().get("data", {}).get("products", [])

                        
                        if not product_data:
                            break
                        
                        for product2 in product_data:
                            if product2.get('remaSeatCnt', 0) == 0 and product2.get('reserveStatus', '') != '예약가능':
                                continue
                            
                            sale_prod_cd = product2.get('saleProdCd', '')
                            
                            if sale_prod_cd[2] == 'B':
                                continue
                            
                            title = product2.get('saleProdNm', '')
                            rprs_cd = product2.get('rprsProdCd', '')
                            stickers = ' | '.join([x.get('value', '') for x in product2.get('sticker', [])])
                            airline = product2.get('depAirNm', '')
                            trvDay = f"{product2.get('trvlNgtCnt')}박{product2.get('trvlDayCnt')}일"
                            depDay = product2.get('depDay', "")
                            hashtag = " ".join(product2.get("hashtag", []))
                            label = ",".join(f"{z.get('code', '')}_{z.get('value', '')}" for z in product2.get('label', []))
                            price = product2.get('adtAmt', '0')
                            normal_price = product2.get('nrmlAmt', '') or ''
                            discount_text = product2.get('discountText', '') or ''
                            city_info = ",".join(
                                f"{z.get('countryName', '')}_{z.get('cityName', '')}"
                                for z in product2.get('logger', {}).get('click', {}).get('cityInfo', [])
                            )
                            
                            base_key = sale_prod_cd[:6] + sale_prod_cd[12:]
                            
                            dic = {
                                'saleProdCd': sale_prod_cd,
                                'base_key' : base_key,
                                'title' : title,
                                'rprs_cd' : rprs_cd,
                                'stickers' : stickers,
                                'airline' : airline,
                                'trvDay' : trvDay,
                                'depDay' : depDay,
                                'hashtag' : hashtag,
                                'label' : label,
                                'price' : price,
                                'normal_price' : normal_price,
                                'discount_text' : discount_text,
                                'city_info' : city_info
                            }
                            
                            score = self._score(price, depDay)
                            max_heapq = self.data_container[base_key]
                            seen_set = self.seen_products[base_key]
                            
                            if sale_prod_cd in seen_set:
                                continue
                            
                            entry = (score, next(self.counter), sale_prod_cd, dic)
                            
                            if len(max_heapq) < N:
                                heapq.heappush(max_heapq, entry)
                                seen_set.add(sale_prod_cd)
                                
                            else:
                                if score > max_heapq[0][0]:
                                    removed_entry = heapq.heapreplace(max_heapq, entry)
                                    removed_code = removed_entry[2]
                                    seen_set.discard(removed_code)
                                    seen_set.add(sale_prod_cd)
                                    
                    except Exception as e:
                        logger.warning(
                            "[ERROR] POST FAILED : %s\n\tError Message : %s",
                            rprs_prod_cd,
                            e,
                            extra={
                                'event' : "Products",
                                'condition' : "Fail"
                            }
                        )
                            
        
        except Exception as e:
            logger.warning(
                "[ERROR] POST FAILED (Major) : %s\n\tError Message : %s",
                area_info['name'],
                e,
                extra={
                    'event' : "Major",
                    'condition' : "Fail"
                }
            )
        
        
        
    def get_product_detail(
        self,
        url : str,
        record : dict,
        num : int
    ):
        try:
            sale_prod_cd = record.get('saleProdCd', '')
            payload = {
                'inpPathCd' : "CBP",
                'pkgCd' : sale_prod_cd,
                'ptnCd' : "A3595",
                'resAcceptPtn' : {},
                'smplYn' : "N"
            }
            
            resp = self.session.post(
                url,
                json=payload,
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json().get('data', {})
            
            if data.get('brndNm', '') == '현지투어플러스':
                return None
            
            promotion = data.get('promNms', '')
            theme = data.get('thmNm', '')
            prodSbttNm = data.get('prodSbttNm', '')
            exprWrdngCont2 = data.get('exprWrdngCont2', '')
            brndNm = data.get('brndNm', '')
            attrNm = data.get('attrNm', '')
            depCityNm = data.get('depCityNm', '')
            fuelExchgAmt = data.get('fuelExchgAmt', 0)
            cityBasInfoList = " ".join(x.get('cityNm', '') for x in data.get('cityBasInfoList', []))
            images = " ".join(i.get('rprsProdCntntUrlAdrs', '') for i in data.get('rppdCntntInfoList', []))
            
            extra = {
                'promotion' : promotion,
                'theme' : theme,
                'prodSbttNm' : prodSbttNm,
                'exprWrdngCont2' : exprWrdngCont2,
                'brndNm' : brndNm,
                'attrNm' : attrNm,
                'depCityNm' : depCityNm,
                'fuelExchgAmt' : fuelExchgAmt,
                'cityBasInfoList' : cityBasInfoList,
                'images' : images
            }
            
            return {**record, **extra}
        
        except Exception as e:
            logger.warning(
                "[ERROR] Describe FAILED : %s\n\tError Message : %s",
                f"{record.get('saleProdCd','')}[{num}]",
                e,
                extra={
                    'event' : "Details",
                    'condition' : "Fail"
                }
            )
        
    
    
    def run_pipeline(self, date:datetime, number_thread:int, N:int = 15):
        """
            Run Pipeline for get top 15 each package product with all meaningful infos
        """
        
        # Get travel list
        travel_area_url = self.urls[0]
        trave_area = self.getTravelAreas(travel_area_url)
        travel_area_parsed = parse_areas(trave_area)
        
        
        date = pd.to_datetime(date).to_pydatetime()
        start_dep_date = date + timedelta(days=21)
        end_dep_date = start_dep_date + timedelta(days=180)
        
        major_url = self.urls[1]
        product_url = self.urls[3]
        
        with ThreadPoolExecutor(max_workers=number_thread) as executor:
            for area in travel_area_parsed:
                executor.submit(
                    self.get_product_info,
                    major_url,
                    product_url,
                    area,
                    N,
                    start_dep_date,
                    end_dep_date
                )
        logger.info("[CHECK] data_container size = %s", len(self.data_container))
        logger.info("[CHECK] total heap records = %s", sum(len(v) for v in self.data_container.values()))
                
        self.seen_products.clear()
        
        list_set = defaultdict(list)
        detail_url = self.urls[4]
        
        with ThreadPoolExecutor(max_workers=N) as executor:
            future_to_num = {}
            for code, heap_list in self.data_container.items():
                for num, (_,_,_,record) in enumerate(heap_list):
                    f = executor.submit(
                        self.get_product_detail,
                        detail_url,
                        record,
                        num
                    )
                    future_to_num[f] = num
                    
            for future in as_completed(future_to_num):
                n = future_to_num[future]
                res = future.result()
                if res:
                    list_set[n].append(res)
                    
        all_records = []
        for _, records in list_set.items():
            all_records.extend(records)

        return pd.DataFrame(all_records)
        