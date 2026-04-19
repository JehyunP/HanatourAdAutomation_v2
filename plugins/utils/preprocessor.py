
from datetime import datetime


def split_kst(execution_date:datetime):
    kst_date = execution_date.in_timezone("Asia/Seoul")
    
    yymmdd = kst_date.strftime("%Y%m%d")
    hhmm = kst_date.strftime("%H%M")
    
    return yymmdd, hhmm



def parse_areas(data):
    list_areas = []
    valid_types = {"C", "S", 'N'}

    for item in data:
        for area in item.get("areas", []):
            if area.get("areaType") in valid_types:
                list_areas.append(
                    {
                        "name": area["name"],
                        "code": area["code"],
                        "type": area["areaType"],
                    }
                )

            if area["areas"]:
                for specific in area.get("areas", []):
                    if specific.get("areaType") in valid_types:
                        list_areas.append(
                            {
                                "name": specific["name"],
                                "code": specific["code"],
                                "type": specific["areaType"],
                            }
                        )

    return list_areas


def extract_reserved_review(df1, df2):
    """
        Extract reviews from reservation code
        df1 : Reviews
        df2 : Reserved
    """
    
    return df1[df1['reservationCode'].isin(df2['code'])]



def extend_review(df1, df2):
    """
        Extend reviews to orignal df as series
    """
    
    # Review Count
    review_count_map = (
        df2.groupby('rprsProdCd')
            .size()
    )

    # Aggregate 
    rating_map = (
        df2.groupby('rprsProdCd')['rating'].first()
    )

    review_count_series = (
        df1['rprsProdCd']
        .map(review_count_map)
        .fillna(0)
        .astype(int)
    )

    rating_series = (
        df1['rprsProdCd']
        .map(rating_map)
        .fillna(0.0)
    )

    return review_count_series, rating_series