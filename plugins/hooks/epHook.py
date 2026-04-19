# Hook
from airflow.hooks.base_hook import BaseHook

# Config
from configs.ep_config import *

# import generals
import logging
import pandas as pd

from utils.preprocessor import extend_review

# setup logging format
logger = logging.getLogger(__name__)

class EpHook(BaseHook):
    """
        This Hook assembles the EP schema based dataframe
    """
    
    def __init__(self, df:pd.DataFrame, review:pd.DataFrame, target:str):
        self.df = df
        self.review = review
        self.target = target
        
        
    def create_ep(self):
        
        # Review related serieses : review count | review rating
        review_count, review_rating = extend_review(self.df, self.review)
        
        try:
            ep = pd.DataFrame({
                'id' : self.df[f'{self.target}_id'],
                'title' : self.df[f'{self.target}_title'],
                'price_pc' : self.df['adtTotlAmt'],
                'benefit_price' : self.df['adtTotlAmt'],
                'normal_price' : self.df['adtTotlAmt'],
                'link' : (
                    "https://" + self.target +
                    ".hanatour.com/trp/pkg/CHPC0PKG0200M200?pkgCd=" +
                    self.df['product_code'].astype(str) +
                    "&prePage=major-products"
                ),
                'mobile_link' : (
                    "https://" + self.target +
                    ".hanatour.com/trp/pkg/CHPC0PKG0200M200?pkgCd=" +
                    self.df['product_code'].astype(str) +
                    "&prePage=major-products"
                ),
                'image_link' : self.df[f'{self.target}_image'],
                'add_image_link' : self.df['extra_image'],
                'video_url' : video_url,
                'category_name1' : category_name1,
                'category_name2' : category_name2,
                'category_name3' : category_name3,
                'category_name4' : category_name4,
                'naver_category' : naver_category,
                'naver_product_id' : naver_product_id,
                'condition' : condition,
                'import_flag' : import_flag,
                'parallel_import' : parallel_import,
                'order_made' : order_made,
                'product_flag' : product_flag,
                'adult' : adult,
                'goods_type' : goods_type,
                'barcode' : barcode,
                'manufacture_define_number': (
                    self.df['product_code'].astype(str).str[:6] +
                    self.df['product_code'].astype(str).str[12:]
                ),
                'model_number': self.df['product_code'].astype(str).str[6:12],
                'brand' : self.df['brand'],
                'brand_certification' : brand_certification,
                'maker' : maker,
                'origin' : origin,
                'card_event' : card_event,
                'event_words' : self.df['event_words'],
                'coupon' :coupon,
                'partner_coupon_download' : partner_coupon_download,
                'interest_free_event' : self.df['interest_free_event'],
                'point' : point,
                'installation_costs' : installation_costs,
                'search_tag' : self.df['search_tag'],
                'coordi_id' : coordi_id,
                'minimum_pruchase_quantity' : minimum_pruchase_quantity,
                'review_count' : review_count,
                'satisfylevel' : review_rating,
                'shipping' : shipping,
                'delivery_grade' : delivery_grade,
                'delivery_detail' : delivery_detail,
                'seller_id' : seller_id[self.target],
                'age_group' : age_group,
                'gender' : gender,
                'attribute' : self.df['attribute'],
                'option_detail' : self.df['option_detail'],
                'product_code' : self.df['product_code'], # Will remove after check duplicates
            })
            
            ep = ep.loc[ep['price_pc'] != 0]
            
            
            # DF dropped duplicate
            ep_unique = ep.drop_duplicates(subset=['product_code'], keep='first')
            ep_unique = ep_unique.drop(columns=['product_code'])

            # DF duplicated data
            ep_removed = ep[ep.duplicated(subset=['product_code'], keep='first')]
            code_removed = ep_removed['product_code'].tolist()
            
            logger.info(
                "[SUCCUESS] EP Dataset Created [%d | %d]",
                len(ep),
                len(self.df),
                extra={
                    'event' : "Create_EP",
                    'status' : "Success"
                }
            )
        except Exception as e:
            logger.error(
                "[ERROR] Unable to create EP Dataset : %s",
                e,
                extra={
                    'event' : "Create_EP",
                    'status' : "Fail"
                }
            )
            raise
        
        return ep_unique, code_removed
    
